package runtime

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrRuntimeHostUnauthorized = errors.New("RUNTIME_HOST_UNAUTHORIZED")
	ErrRuntimeHeartbeatStale   = errors.New("RUNTIME_HEARTBEAT_STALE")
)

// RuntimeHostHeartbeatVerifier is the Backend boundary for an already
// certificate-authenticated Host principal and its signed registration or
// heartbeat request.
// Production callers must provide a verifier backed by a verified mTLS
// listener and durable replay/revocation sources; a nil verifier is rejected.
type RuntimeHostHeartbeatVerifier interface {
	VerifyPrincipal(ctx context.Context, principal RuntimeHostPrincipal) error
	VerifyRegistration(ctx context.Context, principal RuntimeHostPrincipal, method, path string, registration RuntimeHostRegistration, proof RuntimeHostRegistrationProof) error
	VerifyHeartbeat(ctx context.Context, principal RuntimeHostPrincipal, method, path string, heartbeat RuntimeHostHeartbeat) error
}

type RuntimeHostHeartbeatSigner interface {
	SignRegistration(method, path string, principal RuntimeHostPrincipal, registration RuntimeHostRegistration, proof RuntimeHostRegistrationProof) (RuntimeHostRegistrationProof, error)
	SignHeartbeat(method, path string, principal RuntimeHostPrincipal, heartbeat RuntimeHostHeartbeat) (RuntimeHostHeartbeat, error)
}

type RuntimeHostNonceStore interface {
	Claim(ctx context.Context, principal RuntimeHostPrincipal, nonce string, expiresAt time.Time) error
}

type RuntimeHostVerificationKey struct {
	KeyID     string
	PublicKey ed25519.PublicKey
	Revoked   bool
}

type Ed25519RuntimeHostHeartbeatSigner struct {
	keyID      string
	privateKey ed25519.PrivateKey
}

// RuntimeHostRegistrationProof authenticates a semantic registration body.
// The mTLS listener supplies the Host principal; this proof binds that
// principal to the request method, path and canonical registration fields.
type RuntimeHostRegistrationProof struct {
	ObservedAt     time.Time
	SignatureKeyID string
	Nonce          string
	BodySHA256     string
	Signature      string
}

func NewEd25519RuntimeHostHeartbeatSigner(keyID string, privateKey ed25519.PrivateKey) (*Ed25519RuntimeHostHeartbeatSigner, error) {
	if keyID == "" || len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: signing key invalid", ErrRuntimeHostUnauthorized)
	}
	return &Ed25519RuntimeHostHeartbeatSigner{keyID: keyID, privateKey: append(ed25519.PrivateKey(nil), privateKey...)}, nil
}

func (s *Ed25519RuntimeHostHeartbeatSigner) SignHeartbeat(method, path string, principal RuntimeHostPrincipal, heartbeat RuntimeHostHeartbeat) (RuntimeHostHeartbeat, error) {
	if s == nil || s.keyID == "" || len(s.privateKey) != ed25519.PrivateKeySize || !runtimeHostPrincipalValid(principal) {
		return RuntimeHostHeartbeat{}, fmt.Errorf("%w: signing identity invalid", ErrRuntimeHostUnauthorized)
	}
	if heartbeat.Nonce == "" {
		nonce, err := newRuntimeHostHeartbeatNonce()
		if err != nil {
			return RuntimeHostHeartbeat{}, err
		}
		heartbeat.Nonce = nonce
	}
	heartbeat.SignatureKeyID = s.keyID
	bodyHash, err := RuntimeHostHeartbeatBodyHash(heartbeat)
	if err != nil {
		return RuntimeHostHeartbeat{}, err
	}
	heartbeat.BodySHA256 = bodyHash
	input, err := runtimeHostHeartbeatSignatureInput(method, path, principal, heartbeat)
	if err != nil {
		return RuntimeHostHeartbeat{}, err
	}
	heartbeat.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(s.privateKey, input))
	return heartbeat, nil
}

func (s *Ed25519RuntimeHostHeartbeatSigner) SignRegistration(method, path string, principal RuntimeHostPrincipal, registration RuntimeHostRegistration, proof RuntimeHostRegistrationProof) (RuntimeHostRegistrationProof, error) {
	if s == nil || s.keyID == "" || len(s.privateKey) != ed25519.PrivateKeySize || !runtimeHostPrincipalValid(principal) {
		return RuntimeHostRegistrationProof{}, fmt.Errorf("%w: signing identity invalid", ErrRuntimeHostUnauthorized)
	}
	if proof.ObservedAt.IsZero() {
		return RuntimeHostRegistrationProof{}, fmt.Errorf("%w: registration observed time invalid", ErrRuntimeHeartbeatStale)
	}
	if proof.Nonce == "" {
		nonce, err := newRuntimeHostHeartbeatNonce()
		if err != nil {
			return RuntimeHostRegistrationProof{}, err
		}
		proof.Nonce = nonce
	}
	proof.ObservedAt = proof.ObservedAt.UTC()
	proof.SignatureKeyID = s.keyID
	bodyHash, err := RuntimeHostRegistrationBodyHash(registration, proof)
	if err != nil {
		return RuntimeHostRegistrationProof{}, err
	}
	proof.BodySHA256 = bodyHash
	input, err := runtimeHostRegistrationSignatureInput(method, path, principal, proof)
	if err != nil {
		return RuntimeHostRegistrationProof{}, err
	}
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(s.privateKey, input))
	return proof, nil
}

type Ed25519RuntimeHostHeartbeatVerifier struct {
	keys                  map[string]RuntimeHostVerificationKey
	revokedCertificateIDs map[string]bool
	clockWindow           time.Duration
	nonceTTL              time.Duration
	nonces                RuntimeHostNonceStore
	now                   func() time.Time
}

func NewEd25519RuntimeHostHeartbeatVerifier(keys []RuntimeHostVerificationKey, revokedCertificateIDs []string, clockWindow, nonceTTL time.Duration, nonces RuntimeHostNonceStore) (*Ed25519RuntimeHostHeartbeatVerifier, error) {
	if clockWindow <= 0 || nonceTTL < clockWindow*2 || nonces == nil {
		return nil, fmt.Errorf("%w: heartbeat verifier configuration invalid", ErrRuntimeHostUnauthorized)
	}
	keyMap := make(map[string]RuntimeHostVerificationKey, len(keys))
	for _, key := range keys {
		if key.KeyID == "" || len(key.PublicKey) != ed25519.PublicKeySize || keyMap[key.KeyID].KeyID != "" {
			return nil, fmt.Errorf("%w: verification key invalid", ErrRuntimeHostUnauthorized)
		}
		key.PublicKey = append(ed25519.PublicKey(nil), key.PublicKey...)
		keyMap[key.KeyID] = key
	}
	if len(keyMap) == 0 {
		return nil, fmt.Errorf("%w: verification key missing", ErrRuntimeHostUnauthorized)
	}
	revoked := make(map[string]bool, len(revokedCertificateIDs))
	for _, certificateID := range revokedCertificateIDs {
		if certificateID != "" {
			revoked[certificateID] = true
		}
	}
	return &Ed25519RuntimeHostHeartbeatVerifier{
		keys: keyMap, revokedCertificateIDs: revoked, clockWindow: clockWindow,
		nonceTTL: nonceTTL, nonces: nonces, now: func() time.Time { return time.Now().UTC() },
	}, nil
}

func (v *Ed25519RuntimeHostHeartbeatVerifier) VerifyPrincipal(_ context.Context, principal RuntimeHostPrincipal) error {
	if v == nil || !runtimeHostPrincipalValid(principal) || principal.CertificateID == "" || v.revokedCertificateIDs[principal.CertificateID] {
		return ErrRuntimeHostUnauthorized
	}
	return nil
}

func (v *Ed25519RuntimeHostHeartbeatVerifier) VerifyHeartbeat(ctx context.Context, principal RuntimeHostPrincipal, method, path string, heartbeat RuntimeHostHeartbeat) error {
	if err := v.VerifyPrincipal(ctx, principal); err != nil {
		return err
	}
	now := time.Now().UTC()
	if v.now != nil {
		now = v.now().UTC()
	}
	if heartbeat.Sequence < 1 || heartbeat.ObservedAt.IsZero() || heartbeat.Nonce == "" || len(heartbeat.Nonce) > 256 ||
		!heartbeat.ObservedAt.After(now.Add(-v.clockWindow)) || !heartbeat.ObservedAt.Before(now.Add(v.clockWindow)) {
		return ErrRuntimeHeartbeatStale
	}
	key, ok := v.keys[heartbeat.SignatureKeyID]
	if !ok || key.Revoked || len(key.PublicKey) != ed25519.PublicKeySize {
		return ErrRuntimeHostUnauthorized
	}
	bodyHash, err := RuntimeHostHeartbeatBodyHash(heartbeat)
	if err != nil || bodyHash != heartbeat.BodySHA256 {
		return ErrRuntimeHeartbeatStale
	}
	input, err := runtimeHostHeartbeatSignatureInput(method, path, principal, heartbeat)
	if err != nil {
		return ErrRuntimeHeartbeatStale
	}
	signature, err := base64.RawURLEncoding.DecodeString(heartbeat.Signature)
	if err != nil || !ed25519.Verify(key.PublicKey, input, signature) {
		return ErrRuntimeHeartbeatStale
	}
	if err := v.nonces.Claim(ctx, principal, heartbeat.Nonce, now.Add(v.nonceTTL)); err != nil {
		if errors.Is(err, ErrRuntimeHostUnauthorized) {
			return ErrRuntimeHostUnauthorized
		}
		return ErrRuntimeHeartbeatStale
	}
	return nil
}

func (v *Ed25519RuntimeHostHeartbeatVerifier) VerifyRegistration(ctx context.Context, principal RuntimeHostPrincipal, method, path string, registration RuntimeHostRegistration, proof RuntimeHostRegistrationProof) error {
	if err := v.VerifyPrincipal(ctx, principal); err != nil {
		return err
	}
	now := time.Now().UTC()
	if v.now != nil {
		now = v.now().UTC()
	}
	if proof.ObservedAt.IsZero() || proof.Nonce == "" || len(proof.Nonce) > 256 ||
		!proof.ObservedAt.After(now.Add(-v.clockWindow)) || !proof.ObservedAt.Before(now.Add(v.clockWindow)) {
		return ErrRuntimeHeartbeatStale
	}
	key, ok := v.keys[proof.SignatureKeyID]
	if !ok || key.Revoked || len(key.PublicKey) != ed25519.PublicKeySize {
		return ErrRuntimeHostUnauthorized
	}
	bodyHash, err := RuntimeHostRegistrationBodyHash(registration, proof)
	if err != nil || bodyHash != proof.BodySHA256 {
		return ErrRuntimeHeartbeatStale
	}
	input, err := runtimeHostRegistrationSignatureInput(method, path, principal, proof)
	if err != nil {
		return ErrRuntimeHeartbeatStale
	}
	signature, err := base64.RawURLEncoding.DecodeString(proof.Signature)
	if err != nil || !ed25519.Verify(key.PublicKey, input, signature) {
		return ErrRuntimeHeartbeatStale
	}
	if err := v.nonces.Claim(ctx, principal, proof.Nonce, now.Add(v.nonceTTL)); err != nil {
		if errors.Is(err, ErrRuntimeHostUnauthorized) {
			return ErrRuntimeHostUnauthorized
		}
		return ErrRuntimeHeartbeatStale
	}
	return nil
}

type MemoryRuntimeHostNonceStore struct {
	mu     sync.Mutex
	nonces map[string]time.Time
	now    func() time.Time
}

// NewMemoryRuntimeHostNonceStore is deliberately test/local-only. Production
// must inject a durable replay store when constructing its verifier.
func NewMemoryRuntimeHostNonceStore() *MemoryRuntimeHostNonceStore {
	return &MemoryRuntimeHostNonceStore{nonces: map[string]time.Time{}, now: func() time.Time { return time.Now().UTC() }}
}

func (s *MemoryRuntimeHostNonceStore) Claim(ctx context.Context, principal RuntimeHostPrincipal, nonce string, expiresAt time.Time) error {
	if s == nil || !runtimeHostPrincipalValid(principal) || nonce == "" || expiresAt.IsZero() {
		return ErrRuntimeHostUnauthorized
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now().UTC()
	if s.now != nil {
		now = s.now().UTC()
	}
	key := principal.Environment + "\x00" + principal.RuntimeHostID + "\x00" + nonce
	s.mu.Lock()
	defer s.mu.Unlock()
	for currentKey, currentExpiry := range s.nonces {
		if !currentExpiry.After(now) {
			delete(s.nonces, currentKey)
		}
	}
	if currentExpiry, exists := s.nonces[key]; exists && currentExpiry.After(now) {
		return ErrRuntimeHeartbeatStale
	}
	s.nonces[key] = expiresAt.UTC()
	return nil
}

func RuntimeHostHeartbeatBodyHash(heartbeat RuntimeHostHeartbeat) (string, error) {
	body, err := json.Marshal(struct {
		Sequence       int64          `json:"sequence"`
		ObservedAt     string         `json:"observedAt"`
		ActiveRuns     int            `json:"activeRuns"`
		ReservedRuns   int            `json:"reservedRuns"`
		CapabilityHash string         `json:"capabilityHash"`
		SafeHealth     map[string]any `json:"safeHealth,omitempty"`
		SignatureKeyID string         `json:"signatureKeyId"`
	}{
		Sequence: heartbeat.Sequence, ObservedAt: heartbeat.ObservedAt.UTC().Format(time.RFC3339Nano),
		ActiveRuns: heartbeat.ActiveRuns, ReservedRuns: heartbeat.ReservedRuns,
		CapabilityHash: heartbeat.CapabilityHash, SafeHealth: heartbeat.SafeHealth,
		SignatureKeyID: heartbeat.SignatureKeyID,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func RuntimeHostRegistrationBodyHash(registration RuntimeHostRegistration, proof RuntimeHostRegistrationProof) (string, error) {
	body, err := json.Marshal(struct {
		Endpoint             string                    `json:"endpoint"`
		Zone                 string                    `json:"zone"`
		RuntimeVersion       string                    `json:"runtimeVersion"`
		AdapterVersion       string                    `json:"adapterVersion"`
		Capabilities         RuntimeCapabilitySnapshot `json:"capabilities"`
		SessionStoreID       string                    `json:"sessionStoreId"`
		MaxActiveRuns        int                       `json:"maxActiveRuns"`
		MaxProductThreadRuns int                       `json:"maxProductThreadRuns"`
		MaxDetachedTaskRuns  int                       `json:"maxDetachedTaskRuns"`
		ObservedAt           string                    `json:"observedAt"`
		SignatureKeyID       string                    `json:"signatureKeyId"`
	}{
		Endpoint: registration.Endpoint, Zone: registration.Zone,
		RuntimeVersion: registration.RuntimeVersion, AdapterVersion: registration.AdapterVersion,
		Capabilities: registration.Capabilities, SessionStoreID: registration.SessionStoreID,
		MaxActiveRuns: registration.MaxActiveRuns, MaxProductThreadRuns: registration.MaxProductThreadRuns,
		MaxDetachedTaskRuns: registration.MaxDetachedTaskRuns,
		ObservedAt:          proof.ObservedAt.UTC().Format(time.RFC3339Nano), SignatureKeyID: proof.SignatureKeyID,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func runtimeHostHeartbeatSignatureInput(method, path string, principal RuntimeHostPrincipal, heartbeat RuntimeHostHeartbeat) ([]byte, error) {
	if method == "" || path == "" || !runtimeHostPrincipalValid(principal) || heartbeat.BodySHA256 == "" || heartbeat.Nonce == "" || heartbeat.CapabilityHash == "" {
		return nil, fmt.Errorf("%w: heartbeat signature input invalid", ErrRuntimeHeartbeatStale)
	}
	return json.Marshal(struct {
		Method         string `json:"method"`
		Path           string `json:"path"`
		RuntimeHostID  string `json:"runtimeHostId"`
		InstanceID     string `json:"instanceId"`
		Environment    string `json:"environment"`
		Sequence       int64  `json:"sequence"`
		ObservedAt     string `json:"observedAt"`
		Nonce          string `json:"nonce"`
		CapabilityHash string `json:"capabilityHash"`
		BodySHA256     string `json:"bodySha256"`
	}{
		Method: method, Path: path, RuntimeHostID: principal.RuntimeHostID,
		InstanceID: principal.InstanceID, Environment: principal.Environment,
		Sequence: heartbeat.Sequence, ObservedAt: heartbeat.ObservedAt.UTC().Format(time.RFC3339Nano),
		Nonce: heartbeat.Nonce, CapabilityHash: heartbeat.CapabilityHash, BodySHA256: heartbeat.BodySHA256,
	})
}

func runtimeHostRegistrationSignatureInput(method, path string, principal RuntimeHostPrincipal, proof RuntimeHostRegistrationProof) ([]byte, error) {
	if method == "" || path == "" || !runtimeHostPrincipalValid(principal) || proof.BodySHA256 == "" || proof.Nonce == "" || proof.ObservedAt.IsZero() {
		return nil, fmt.Errorf("%w: registration signature input invalid", ErrRuntimeHeartbeatStale)
	}
	return json.Marshal(struct {
		Method        string `json:"method"`
		Path          string `json:"path"`
		RuntimeHostID string `json:"runtimeHostId"`
		InstanceID    string `json:"instanceId"`
		Environment   string `json:"environment"`
		ObservedAt    string `json:"observedAt"`
		Nonce         string `json:"nonce"`
		BodySHA256    string `json:"bodySha256"`
	}{
		Method: method, Path: path, RuntimeHostID: principal.RuntimeHostID,
		InstanceID: principal.InstanceID, Environment: principal.Environment,
		ObservedAt: proof.ObservedAt.UTC().Format(time.RFC3339Nano), Nonce: proof.Nonce,
		BodySHA256: proof.BodySHA256,
	})
}

func runtimeHostPrincipalValid(principal RuntimeHostPrincipal) bool {
	return principal.RuntimeHostID != "" && principal.InstanceID != "" && principal.Environment != ""
}

func newRuntimeHostHeartbeatNonce() (string, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
