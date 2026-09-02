package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
)

func TestRedisRuntimeHostNonceStoreRejectsDuplicateNonceAcrossClaims(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	store, err := NewRedisRuntimeHostNonceStore(server.Addr(), "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	principal := RuntimeHostPrincipal{Environment: "test", RuntimeHostID: "host_1", InstanceID: "instance_1", CertificateID: "cert_1"}
	if err := store.Claim(context.Background(), principal, "nonce_1", time.Now().UTC().Add(time.Minute)); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := store.Claim(context.Background(), principal, "nonce_1", time.Now().UTC().Add(time.Minute)); !errors.Is(err, ErrRuntimeHeartbeatStale) {
		t.Fatalf("duplicate claim error=%v, want %v", err, ErrRuntimeHeartbeatStale)
	}
}
