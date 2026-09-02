package runtime

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const maxRunTicketTTLSeconds int64 = 15 * 60

// RuntimeSubmitBindingV2 marks a ticket that is authorized to start a model
// Run. Generic ticketed reads (status, events and Workspace search) do not
// carry a model input and intentionally remain outside this binding.
const RuntimeSubmitBindingV2 = "runtime_submit_binding.v2"

type RunTicketClaims struct {
	RunID             string                  `json:"runId"`
	TenantID          string                  `json:"tenantId"`
	ReservationID     string                  `json:"reservationId"`
	RuntimeHostID     string                  `json:"runtimeHostId"`
	CapabilityHash    string                  `json:"capabilityHash"`
	WorkspaceID       string                  `json:"workspaceId"`
	WorkspaceVersion  int64                   `json:"workspaceVersion"`
	ContextGeneration int64                   `json:"contextGeneration"`
	InputManifestHash string                  `json:"inputManifestHash"`
	PlanHash          string                  `json:"planHash"`
	SubmitBinding     *RunTicketSubmitBinding `json:"submitBinding,omitempty"`
	FencingToken      int64                   `json:"fencingToken"`
	JTI               string                  `json:"jti"`
	ExpiresAt         int64                   `json:"exp"`
	IssuedAt          int64                   `json:"iat"`
}

// RunTicketSubmitBinding is mandatory only for a model-start submit. It binds
// every caller-controlled execution field that is not represented in the
// immutable manifest or signed Plan. Read/abort/search tickets omit it.
type RunTicketSubmitBinding struct {
	Version              string `json:"version"`
	InputMessageHash     string `json:"inputMessageHash"`
	RuntimeConfigID      string `json:"runtimeConfigId"`
	RuntimeConfigVersion string `json:"runtimeConfigVersion"`
	ProductSessionHash   string `json:"productSessionHash"`
}

func SignRunTicket(claims RunTicketClaims, secret string) (string, error) {
	if err := validateRunTicketClaims(claims, secret, time.Now().UTC(), false); err != nil {
		return "", err
	}
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "HUAHUO-RUN-TICKET"})
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func VerifyRunTicket(token, secret string, now time.Time) (RunTicketClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || secret == "" {
		return RunTicketClaims{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	input := parts[0] + "." + parts[1]
	expected := hmac.New(sha256.New, []byte(secret))
	_, _ = expected.Write([]byte(input))
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(signature, expected.Sum(nil)) {
		return RunTicketClaims{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return RunTicketClaims{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	var claims RunTicketClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return RunTicketClaims{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	if err := validateRunTicketClaims(claims, secret, now, true); err != nil {
		return RunTicketClaims{}, err
	}
	return claims, nil
}

func validateRunTicketClaims(claims RunTicketClaims, secret string, now time.Time, verifyTime bool) error {
	if secret == "" || claims.RunID == "" || claims.TenantID == "" || claims.TenantID != strings.TrimSpace(claims.TenantID) || claims.ReservationID == "" || claims.RuntimeHostID == "" || claims.CapabilityHash == "" || claims.WorkspaceID == "" || claims.WorkspaceVersion < 1 || claims.ContextGeneration < 1 || claims.FencingToken < 1 || claims.JTI == "" || claims.IssuedAt <= 0 || claims.ExpiresAt <= claims.IssuedAt || claims.ExpiresAt-claims.IssuedAt > maxRunTicketTTLSeconds {
		return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	if !validRunTicketSHA256(claims.InputManifestHash) || !validRunTicketSHA256(claims.PlanHash) {
		return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	if claims.SubmitBinding != nil {
		binding := claims.SubmitBinding
		if binding.Version != RuntimeSubmitBindingV2 || !validRunTicketSHA256(binding.InputMessageHash) ||
			!validRunTicketBindingIdentifier(binding.RuntimeConfigID) || !validRunTicketBindingVersion(binding.RuntimeConfigVersion) ||
			!validRunTicketSHA256(binding.ProductSessionHash) {
			return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
		}
	}
	if verifyTime && (claims.ExpiresAt <= now.Unix() || claims.IssuedAt > now.Add(time.Minute).Unix()) {
		return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	return nil
}

func validRunTicketSHA256(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func validRunTicketBindingIdentifier(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func validRunTicketBindingVersion(value string) bool {
	return validRunTicketBindingIdentifier(value)
}

// ValidRuntimeSubmitConfigVersion is shared by Worker bootstrap, Dispatcher,
// Host transport, and Ticket validation so a configured version cannot be
// accepted at one boundary and rejected after a capacity reservation.
func ValidRuntimeSubmitConfigVersion(value string) bool {
	return validRunTicketBindingVersion(value)
}

// RunTicketJTIHash is the only JTI representation that may enter durable
// dispatch/registry records. Callers must not persist or log the raw JTI.
func RunTicketJTIHash(jti string) string {
	sum := sha256.Sum256([]byte(jti))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// RunTicketInputMessageHash binds the exact Backend-to-Host model turn without
// placing its text in the ticket, logs, or durable dispatch records. A fixed
// domain separator prevents it from being confused with manifest, plan or JTI
// hashes that share the same SHA-256 representation.
func RunTicketInputMessageHash(value string) string {
	sum := sha256.Sum256([]byte("huahuo.runtime.submit.input.v1\x00" + value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// RunTicketProductSessionHash binds the stable native-session pair without
// placing its thread or OpenClaw session key in the ticket or durable logs.
func RunTicketProductSessionHash(threadID, openclawSessionKey string) string {
	sum := sha256.Sum256([]byte("huahuo.runtime.submit.product_session.v1\x00" + threadID + "\x00" + openclawSessionKey))
	return "sha256:" + hex.EncodeToString(sum[:])
}
