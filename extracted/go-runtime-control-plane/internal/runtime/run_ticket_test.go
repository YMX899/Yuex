package runtime

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRunTicketSignsAndVerifiesPlanHash(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	claims := testRunTicketClaims(now)
	claims.PlanHash = "sha256:" + strings.Repeat("a", 64)
	ticket, err := SignRunTicket(claims, "run-ticket-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyRunTicket(ticket, "run-ticket-test-secret", now)
	if err != nil {
		t.Fatal(err)
	}
	if verified.PlanHash != claims.PlanHash || verified.SubmitBinding == nil || claims.SubmitBinding == nil ||
		verified.SubmitBinding.InputMessageHash != claims.SubmitBinding.InputMessageHash ||
		verified.SubmitBinding.RuntimeConfigID != claims.SubmitBinding.RuntimeConfigID ||
		verified.SubmitBinding.RuntimeConfigVersion != claims.SubmitBinding.RuntimeConfigVersion ||
		verified.SubmitBinding.ProductSessionHash != claims.SubmitBinding.ProductSessionHash {
		t.Fatalf("signed submit binding=%#v plan hash=%q want=%#v/%q", verified.SubmitBinding, verified.PlanHash, claims.SubmitBinding, claims.PlanHash)
	}
}

func TestRunTicketInputMessageHashIsDomainSeparatedAndExact(t *testing.T) {
	const input = "  line one\r\nline two  "
	const want = "sha256:45fdd18f6594fbd395c24a9fa5af276709f5d3789ccf96fb38e04ccaea829a4d"
	if got := RunTicketInputMessageHash(input); got != want {
		t.Fatalf("input hash=%q want %q", got, want)
	}
	if RunTicketInputMessageHash(input) == RunTicketInputMessageHash(strings.TrimSpace(input)) {
		t.Fatal("input hash must not trim signed message bytes")
	}
	if RunTicketInputMessageHash(input) == RunTicketInputMessageHash(strings.Replace(input, "\r\n", "\n", 1)) {
		t.Fatal("input hash must not normalize signed message newlines")
	}
}

func TestRunTicketProductSessionHashIsDomainSeparatedAndExact(t *testing.T) {
	const threadID = "thread_ticket"
	const sessionKey = "oc:session:ticket"
	wantBytes := []byte("huahuo.runtime.submit.product_session.v1\x00" + threadID + "\x00" + sessionKey)
	want := sha256.Sum256(wantBytes)
	if got := RunTicketProductSessionHash(threadID, sessionKey); got != "sha256:"+hex.EncodeToString(want[:]) {
		t.Fatalf("session hash=%q want sha256:%x", got, want)
	}
	if RunTicketProductSessionHash(threadID, sessionKey) == RunTicketProductSessionHash(threadID+"_other", sessionKey) {
		t.Fatal("session hash must bind the exact thread ID")
	}
	if RunTicketProductSessionHash(threadID, sessionKey) == RunTicketProductSessionHash(threadID, sessionKey+":other") {
		t.Fatal("session hash must bind the exact OpenClaw session key")
	}
}

func TestRunTicketRejectsMalformedPlanHash(t *testing.T) {
	claims := testRunTicketClaims(time.Now().UTC())
	claims.PlanHash = "not-a-sha256"
	if _, err := SignRunTicket(claims, "run-ticket-test-secret"); err == nil {
		t.Fatal("malformed plan hash was signed")
	}
}

func TestRunTicketRejectsMissingRequiredPrivateContextBindings(t *testing.T) {
	now := time.Now().UTC()
	for name, mutate := range map[string]func(*RunTicketClaims){
		"tenant":                    func(claims *RunTicketClaims) { claims.TenantID = "" },
		"tenant whitespace":         func(claims *RunTicketClaims) { claims.TenantID = " tenant_ticket" },
		"input manifest hash":       func(claims *RunTicketClaims) { claims.InputManifestHash = "" },
		"plan hash":                 func(claims *RunTicketClaims) { claims.PlanHash = "" },
		"input hash prefix":         func(claims *RunTicketClaims) { claims.InputManifestHash = strings.Repeat("b", 64) },
		"plan hash prefix":          func(claims *RunTicketClaims) { claims.PlanHash = strings.Repeat("a", 64) },
		"missing issued at":         func(claims *RunTicketClaims) { claims.IssuedAt = 0 },
		"expired before issue":      func(claims *RunTicketClaims) { claims.ExpiresAt = claims.IssuedAt },
		"ttl exceeds maximum":       func(claims *RunTicketClaims) { claims.ExpiresAt = claims.IssuedAt + maxRunTicketTTLSeconds + 1 },
		"submit binding version":    func(claims *RunTicketClaims) { claims.SubmitBinding.Version = "unknown" },
		"submit binding input hash": func(claims *RunTicketClaims) { claims.SubmitBinding.InputMessageHash = "not-a-sha256" },
		"submit binding config":     func(claims *RunTicketClaims) { claims.SubmitBinding.RuntimeConfigID = "runtime config" },
		"submit binding session":    func(claims *RunTicketClaims) { claims.SubmitBinding.ProductSessionHash = "not-a-sha256" },
		"submit binding version empty": func(claims *RunTicketClaims) {
			claims.SubmitBinding.RuntimeConfigVersion = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			claims := testRunTicketClaims(now)
			mutate(&claims)
			if _, err := SignRunTicket(claims, "run-ticket-test-secret"); err == nil || err.Error() != "RUNTIME_PERMISSION_DENIED" {
				t.Fatalf("SignRunTicket error=%v, want RUNTIME_PERMISSION_DENIED", err)
			}
		})
	}
}

func TestRunTicketAllowsUnboundGenericTickets(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	claims := testRunTicketClaims(now)
	claims.SubmitBinding = nil
	ticket, err := SignRunTicket(claims, "run-ticket-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyRunTicket(ticket, "run-ticket-test-secret", now)
	if err != nil || verified.SubmitBinding != nil {
		t.Fatalf("generic ticket verified=%#v err=%v", verified, err)
	}
}

func TestVerifyRunTicketRejectsLegacySignedTicketWithoutPrivateBindings(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	claims := testRunTicketClaims(now)
	rawClaims := map[string]any{}
	raw, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &rawClaims); err != nil {
		t.Fatal(err)
	}
	delete(rawClaims, "tenantId")
	delete(rawClaims, "inputManifestHash")
	delete(rawClaims, "planHash")
	legacyTicket := signUncheckedRunTicket(t, rawClaims, "run-ticket-test-secret")
	if _, err := VerifyRunTicket(legacyTicket, "run-ticket-test-secret", now); err == nil || err.Error() != "RUNTIME_PERMISSION_DENIED" {
		t.Fatalf("legacy Ticket verify error=%v, want RUNTIME_PERMISSION_DENIED", err)
	}
}

func testRunTicketClaims(now time.Time) RunTicketClaims {
	return RunTicketClaims{
		RunID: "run_ticket", TenantID: "tenant_ticket", ReservationID: "reservation_ticket", RuntimeHostID: "host_ticket",
		CapabilityHash: "capability_ticket", WorkspaceID: "workspace_ticket", WorkspaceVersion: 1,
		ContextGeneration: 1, InputManifestHash: "sha256:" + strings.Repeat("b", 64), PlanHash: "sha256:" + strings.Repeat("a", 64),
		SubmitBinding: &RunTicketSubmitBinding{Version: RuntimeSubmitBindingV2, InputMessageHash: RunTicketInputMessageHash("line one\nline two"), RuntimeConfigID: "runtime-ticket-test", RuntimeConfigVersion: "v1", ProductSessionHash: RunTicketProductSessionHash("thread_ticket", "oc:session:ticket")},
		FencingToken:  1, JTI: "jti_ticket",
		IssuedAt: now.Add(-time.Second).Unix(), ExpiresAt: now.Add(time.Minute).Unix(),
	}
}

func signUncheckedRunTicket(t *testing.T, claims map[string]any, secret string) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "HUAHUO-RUN-TICKET"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signed := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
