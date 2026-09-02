package runtime

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
)

const runtimeHostSPIFFETrustDomain = "huahuo"

// RuntimeHostMTLSConfig contains protected material references, not plaintext
// key material. A process resolves the references only while building its TLS
// transport; callers must not log this structure or its resolution failures.
type RuntimeHostMTLSConfig struct {
	TrustRef       string
	CertificateRef string
	PrivateKeyRef  string
	ServerName     string
}

// RuntimeHostMTLSHTTPClient is a marked HTTP transport used for Backend to
// RuntimeHost calls. The marker prevents a production Runtime transport from
// silently falling back to http.DefaultClient.
type RuntimeHostMTLSHTTPClient interface {
	HTTPClient
	RuntimeHostMTLSConfigured() bool
}

type runtimeHostMTLSHTTPClient struct {
	client    *http.Client
	transport *http.Transport
}

func (c *runtimeHostMTLSHTTPClient) Do(request *http.Request) (*http.Response, error) {
	return c.client.Do(request)
}

func (*runtimeHostMTLSHTTPClient) RuntimeHostMTLSConfigured() bool { return true }

// RuntimeHostClient binds one request transport to the scheduled Host's
// certificate URI SAN before bytes are sent. The normal TLS DNS/CA check stays
// enabled; this is an additional Host/instance/environment assertion.
func (c *runtimeHostMTLSHTTPClient) RuntimeHostClient(host RuntimeHost) (HTTPClient, error) {
	if c == nil || c.transport == nil || host.RuntimeHostID == "" || host.InstanceID == "" || host.Environment == "" {
		return nil, ErrRuntimeHostUnauthorized
	}
	transport := c.transport.Clone()
	tlsConfig := transport.TLSClientConfig.Clone()
	tlsConfig.VerifyConnection = func(connection tls.ConnectionState) error {
		if len(connection.PeerCertificates) == 0 {
			return ErrRuntimeHostUnauthorized
		}
		principal, err := RuntimeHostPrincipalFromCertificate(connection.PeerCertificates[0])
		if err != nil || principal.RuntimeHostID != host.RuntimeHostID || principal.InstanceID != host.InstanceID || principal.Environment != host.Environment {
			return ErrRuntimeHostUnauthorized
		}
		return nil
	}
	transport.TLSClientConfig = tlsConfig
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}, nil
}

func RuntimeHostMTLSClientConfigured(client HTTPClient) bool {
	// RuntimeHostMTLSHTTPClient is exported so callers can retain the factory
	// return value, but its marker alone is not an authority boundary: another
	// package could implement that public method on a plain HTTP client. Runtime
	// Host calls therefore accept only the sealed transport constructed in this
	// package, which owns the client certificate and per-Host SAN binding.
	configured, ok := client.(*runtimeHostMTLSHTTPClient)
	return ok && configured != nil && configured.transport != nil && configured.RuntimeHostMTLSConfigured()
}

// LoadRuntimeHostMTLSServerConfig creates a server-side mutual TLS config.
// ClientAuth intentionally requires certificate verification before a request
// can reach the RuntimeHost route handler.
func LoadRuntimeHostMTLSServerConfig(config RuntimeHostMTLSConfig) (*tls.Config, error) {
	trust, certificate, err := loadRuntimeHostTLSMaterial(config)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(trust) {
		return nil, runtimeHostMTLSConfigError()
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    roots,
	}, nil
}

// NewRuntimeHostMTLSHTTPClient creates the dedicated Host-facing client. It
// validates the server chain and presents the process certificate; redirects
// are rejected so a registered Host cannot redirect a run ticket to another
// origin.
func NewRuntimeHostMTLSHTTPClient(config RuntimeHostMTLSConfig) (RuntimeHostMTLSHTTPClient, error) {
	tlsConfig, err := NewRuntimeHostMTLSClientTLSConfig(config)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &runtimeHostMTLSHTTPClient{client: client, transport: transport}, nil
}

// NewRuntimeHostMTLSClientTLSConfig builds the TLS 1.3 client configuration
// used by non-HTTP Host control-plane transports. The caller owns the returned
// config and must not weaken its certificate verification or reuse it for a
// non-Host principal. This is intentionally shared by the Backend HTTP and
// Gateway WebSocket recovery clients so one cannot silently fall back to a
// token-only Gateway connection during restart recovery.
func NewRuntimeHostMTLSClientTLSConfig(config RuntimeHostMTLSConfig) (*tls.Config, error) {
	trust, certificate, err := loadRuntimeHostTLSMaterial(config)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(trust) {
		return nil, runtimeHostMTLSConfigError()
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      roots,
		Certificates: []tls.Certificate{certificate},
		ServerName:   strings.TrimSpace(config.ServerName),
	}, nil
}

// RuntimeHostPrincipalFromCertificate accepts only the canonical URI SAN:
// spiffe://huahuo/runtime-host/<environment>/<runtimeHostId>/<instanceId>.
// The certificate fingerprint is an internal, non-secret reference used by
// revocation and heartbeat verification; it is never persisted or returned.
func RuntimeHostPrincipalFromCertificate(certificate *x509.Certificate) (RuntimeHostPrincipal, error) {
	if certificate == nil || len(certificate.Raw) == 0 {
		return RuntimeHostPrincipal{}, ErrRuntimeHostUnauthorized
	}
	var principal RuntimeHostPrincipal
	found := false
	for _, identity := range certificate.URIs {
		candidate, ok := runtimeHostPrincipalFromURI(identity)
		if !ok {
			continue
		}
		if found && (principal.RuntimeHostID != candidate.RuntimeHostID || principal.InstanceID != candidate.InstanceID || principal.Environment != candidate.Environment) {
			return RuntimeHostPrincipal{}, ErrRuntimeHostUnauthorized
		}
		principal = candidate
		found = true
	}
	if !found || !runtimeHostPrincipalValid(principal) {
		return RuntimeHostPrincipal{}, ErrRuntimeHostUnauthorized
	}
	fingerprint := sha256.Sum256(certificate.Raw)
	principal.CertificateID = "sha256:" + hex.EncodeToString(fingerprint[:])
	return principal, nil
}

func runtimeHostPrincipalFromURI(identity *url.URL) (RuntimeHostPrincipal, bool) {
	if identity == nil || !strings.EqualFold(identity.Scheme, "spiffe") || !strings.EqualFold(identity.Host, runtimeHostSPIFFETrustDomain) ||
		identity.User != nil || identity.RawQuery != "" || identity.Fragment != "" {
		return RuntimeHostPrincipal{}, false
	}
	segments := strings.Split(strings.Trim(identity.EscapedPath(), "/"), "/")
	if len(segments) != 4 || segments[0] != "runtime-host" {
		return RuntimeHostPrincipal{}, false
	}
	values := make([]string, 0, 3)
	for _, segment := range segments[1:] {
		value, err := url.PathUnescape(segment)
		if err != nil || !runtimeHostIdentitySegmentValid(value) {
			return RuntimeHostPrincipal{}, false
		}
		values = append(values, value)
	}
	return RuntimeHostPrincipal{Environment: values[0], RuntimeHostID: values[1], InstanceID: values[2]}, true
}

func runtimeHostIdentitySegmentValid(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 || value != strings.TrimSpace(value) {
		return false
	}
	for _, runeValue := range value {
		if unicode.IsLetter(runeValue) || unicode.IsDigit(runeValue) || runeValue == '-' || runeValue == '_' || runeValue == '.' {
			continue
		}
		return false
	}
	return true
}

// LoadEd25519RuntimeHostHeartbeatSigner resolves one private signing key from
// a protected reference. Rotation remains a deployment concern: a process is
// restarted with the next key ID and Backend overlap configuration.
func LoadEd25519RuntimeHostHeartbeatSigner(keyID, privateKeyRef string) (RuntimeHostHeartbeatSigner, error) {
	if strings.TrimSpace(keyID) == "" {
		return nil, runtimeHostMTLSConfigError()
	}
	raw, err := readRuntimeHostIdentityMaterial(privateKeyRef)
	if err != nil {
		return nil, err
	}
	privateKey, err := runtimeHostEd25519PrivateKey(raw)
	if err != nil {
		return nil, runtimeHostMTLSConfigError()
	}
	return NewEd25519RuntimeHostHeartbeatSigner(strings.TrimSpace(keyID), privateKey)
}

// LoadEd25519RuntimeHostHeartbeatVerifier resolves the configured public key
// and certificate revocation list. The nonce store is intentionally injected:
// production must pass a durable store and never substitutes the memory store.
func LoadEd25519RuntimeHostHeartbeatVerifier(keyID, verificationKeyRef, revocationRef string, clockWindow, nonceTTL time.Duration, nonces RuntimeHostNonceStore) (RuntimeHostHeartbeatVerifier, error) {
	if strings.TrimSpace(keyID) == "" || nonces == nil {
		return nil, runtimeHostMTLSConfigError()
	}
	raw, err := readRuntimeHostIdentityMaterial(verificationKeyRef)
	if err != nil {
		return nil, err
	}
	publicKey, err := runtimeHostEd25519PublicKey(raw)
	if err != nil {
		return nil, runtimeHostMTLSConfigError()
	}
	revoked, err := LoadRuntimeHostRevokedCertificateIDs(revocationRef)
	if err != nil {
		return nil, err
	}
	return NewEd25519RuntimeHostHeartbeatVerifier([]RuntimeHostVerificationKey{{KeyID: strings.TrimSpace(keyID), PublicKey: publicKey}}, revoked, clockWindow, nonceTTL, nonces)
}

// LoadRuntimeHostRevokedCertificateIDs reads a JSON object with
// revokedCertificateIds, a JSON string array, or newline-separated fingerprints.
// It never returns source material to callers.
func LoadRuntimeHostRevokedCertificateIDs(ref string) ([]string, error) {
	raw, err := readRuntimeHostIdentityMaterial(ref)
	if err != nil {
		return nil, err
	}
	var document struct {
		RevokedCertificateIDs []string `json:"revokedCertificateIds"`
	}
	if json.Unmarshal(raw, &document) == nil && document.RevokedCertificateIDs != nil {
		return normalizeRuntimeHostCertificateIDs(document.RevokedCertificateIDs), nil
	}
	var array []string
	if json.Unmarshal(raw, &array) == nil {
		return normalizeRuntimeHostCertificateIDs(array), nil
	}
	return normalizeRuntimeHostCertificateIDs(strings.FieldsFunc(string(raw), func(r rune) bool { return r == '\n' || r == '\r' || r == ',' })), nil
}

func normalizeRuntimeHostCertificateIDs(values []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}

func loadRuntimeHostTLSMaterial(config RuntimeHostMTLSConfig) ([]byte, tls.Certificate, error) {
	trust, err := readRuntimeHostIdentityMaterial(config.TrustRef)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	certificatePEM, err := readRuntimeHostIdentityMaterial(config.CertificateRef)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	privateKeyPEM, err := readRuntimeHostIdentityMaterial(config.PrivateKeyRef)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, tls.Certificate{}, runtimeHostMTLSConfigError()
	}
	return trust, certificate, nil
}

func runtimeHostEd25519PrivateKey(raw []byte) (ed25519.PrivateKey, error) {
	if block, _ := pem.Decode(raw); block != nil {
		value, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		privateKey, ok := value.(ed25519.PrivateKey)
		if !ok || len(privateKey) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("ed25519 private key required")
		}
		return append(ed25519.PrivateKey(nil), privateKey...), nil
	}
	decoded, err := decodeRuntimeHostKey(raw)
	if err != nil || len(decoded) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("ed25519 private key required")
	}
	return ed25519.PrivateKey(decoded), nil
}

func runtimeHostEd25519PublicKey(raw []byte) (ed25519.PublicKey, error) {
	if block, _ := pem.Decode(raw); block != nil {
		if certificate, err := x509.ParseCertificate(block.Bytes); err == nil {
			publicKey, ok := certificate.PublicKey.(ed25519.PublicKey)
			if !ok {
				return nil, fmt.Errorf("ed25519 public key required")
			}
			return append(ed25519.PublicKey(nil), publicKey...), nil
		}
		value, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err == nil {
			publicKey, ok := value.(ed25519.PublicKey)
			if ok && len(publicKey) == ed25519.PublicKeySize {
				return append(ed25519.PublicKey(nil), publicKey...), nil
			}
		}
		value, err = x509.ParsePKCS8PrivateKey(block.Bytes)
		if err == nil {
			if privateKey, ok := value.(ed25519.PrivateKey); ok && len(privateKey) == ed25519.PrivateKeySize {
				return append(ed25519.PublicKey(nil), privateKey.Public().(ed25519.PublicKey)...), nil
			}
		}
		return nil, fmt.Errorf("ed25519 public key required")
	}
	decoded, err := decodeRuntimeHostKey(raw)
	if err != nil {
		return nil, err
	}
	if len(decoded) == ed25519.PrivateKeySize {
		return append(ed25519.PublicKey(nil), ed25519.PrivateKey(decoded).Public().(ed25519.PublicKey)...), nil
	}
	if len(decoded) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("ed25519 public key required")
	}
	return ed25519.PublicKey(decoded), nil
}

func decodeRuntimeHostKey(raw []byte) ([]byte, error) {
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return nil, io.ErrUnexpectedEOF
	}
	for _, encoding := range []*base64.Encoding{base64.RawURLEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.StdEncoding} {
		decoded, err := encoding.DecodeString(value)
		if err == nil {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("encoded key invalid")
}

func readRuntimeHostIdentityMaterial(ref string) ([]byte, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, runtimeHostMTLSConfigError()
	}
	if envName, ok := runtimeHostIdentityEnvName(ref); ok {
		value := strings.TrimSpace(os.Getenv(envName))
		if value == "" {
			return nil, runtimeHostMTLSConfigError()
		}
		if path, fileRef := runtimeHostIdentityFilePath(value); fileRef {
			return readRuntimeHostIdentityFile(path)
		}
		return []byte(value), nil
	}
	path, ok := runtimeHostIdentityFilePath(ref)
	if !ok {
		return nil, runtimeHostMTLSConfigError()
	}
	return readRuntimeHostIdentityFile(path)
}

func readRuntimeHostIdentityFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, runtimeHostMTLSConfigError()
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 4<<20))
	if err != nil || len(raw) == 0 {
		return nil, runtimeHostMTLSConfigError()
	}
	return raw, nil
}

func runtimeHostIdentityEnvName(ref string) (string, bool) {
	for _, prefix := range []string{"secret://server-env/", "secret://env/", "env://"} {
		if strings.HasPrefix(ref, prefix) {
			name := strings.TrimPrefix(ref, prefix)
			return name, runtimeHostIdentityEnvNameValid(name)
		}
	}
	if runtimeHostIdentityEnvNameValid(ref) && os.Getenv(ref) != "" {
		return ref, true
	}
	return "", false
}

func runtimeHostIdentityEnvNameValid(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for index, runeValue := range value {
		if runeValue == '_' || unicode.IsLetter(runeValue) || (index > 0 && unicode.IsDigit(runeValue)) {
			continue
		}
		return false
	}
	return true
}

func runtimeHostIdentityFilePath(ref string) (string, bool) {
	if strings.HasPrefix(ref, "file://") {
		parsed, err := url.Parse(ref)
		if err != nil || parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") || parsed.Path == "" {
			return "", false
		}
		path := filepath.FromSlash(parsed.Path)
		if len(path) >= 3 && path[0] == filepath.Separator && path[2] == ':' {
			path = path[1:]
		}
		return path, true
	}
	if filepath.IsAbs(ref) || strings.HasPrefix(ref, ".") {
		return ref, true
	}
	return "", false
}

func runtimeHostMTLSConfigError() error {
	return fmt.Errorf("RUNTIME_HOST_IDENTITY_UNAVAILABLE")
}
