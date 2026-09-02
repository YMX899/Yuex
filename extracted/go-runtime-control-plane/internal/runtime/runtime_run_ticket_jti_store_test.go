package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFileRunTicketJTIStoreRejectsReplayAfterReopenWithoutPersistingPlaintext(t *testing.T) {
	root := filepath.Join(t.TempDir(), "run-ticket-jtis")
	now := time.Now().UTC()
	first, err := NewFileRunTicketJTIStore(root)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := first.Consume(context.Background(), "ticket-jti-secret-value", now.Add(time.Minute))
	if err != nil || !claimed {
		t.Fatalf("first claim claimed=%v err=%v", claimed, err)
	}

	second, err := NewFileRunTicketJTIStore(root)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err = second.Consume(context.Background(), "ticket-jti-secret-value", now.Add(time.Minute))
	if err != nil || claimed {
		t.Fatalf("reopen replay claimed=%v err=%v", claimed, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 {
		t.Fatalf("durable records=%d err=%v", len(entries), err)
	}
	if strings.Contains(entries[0].Name(), "ticket-jti-secret-value") {
		t.Fatalf("plaintext JTI leaked in filename %q", entries[0].Name())
	}
	raw, err := os.ReadFile(filepath.Join(root, entries[0].Name()))
	if err != nil || strings.Contains(string(raw), "ticket-jti-secret-value") {
		t.Fatalf("plaintext JTI leaked in record %q err=%v", raw, err)
	}
}

func TestFileRunTicketJTIStoreClaimsOnceAcrossStoreInstances(t *testing.T) {
	root := filepath.Join(t.TempDir(), "run-ticket-jtis")
	const contenders = 16
	var accepted atomic.Int32
	var failures atomic.Int32
	var failureMu sync.Mutex
	failureMessages := []string{}
	var wait sync.WaitGroup
	for index := 0; index < contenders; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			store, err := NewFileRunTicketJTIStore(root)
			if err != nil {
				failures.Add(1)
				failureMu.Lock()
				failureMessages = append(failureMessages, "open: "+err.Error())
				failureMu.Unlock()
				return
			}
			claimed, err := store.Consume(context.Background(), "ticket-jti-concurrent", time.Now().UTC().Add(time.Minute))
			if err != nil {
				failures.Add(1)
				failureMu.Lock()
				failureMessages = append(failureMessages, "consume: "+err.Error())
				failureMu.Unlock()
				return
			}
			if claimed {
				accepted.Add(1)
			}
		}()
	}
	wait.Wait()
	if failures.Load() != 0 || accepted.Load() != 1 {
		t.Fatalf("accepted=%d failures=%d messages=%v", accepted.Load(), failures.Load(), failureMessages)
	}
}

func TestFileRunTicketJTIStoreReclaimsExpiredLeaseByFileAge(t *testing.T) {
	root := filepath.Join(t.TempDir(), "run-ticket-jtis")
	store, err := NewFileRunTicketJTIStore(root)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(root, ".consume.lock")
	if err := os.WriteFile(lockPath, []byte("{\"expiresAt\":4102444800}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(lockPath, time.Now().Add(-runTicketJTIStoreLockTTL-time.Minute), time.Now().Add(-runTicketJTIStoreLockTTL-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if !store.consumeLockExpired(lockPath, time.Now().UTC()) {
		t.Fatal("expired lease was not recognized from its file age")
	}

	claimed, err := store.Consume(context.Background(), "ticket-jti-stale-lease", time.Now().UTC().Add(time.Minute))
	if err != nil || !claimed {
		t.Fatalf("claim after stale lease claimed=%v err=%v", claimed, err)
	}
}

func TestFileRunTicketJTIStoreRejectsSymlinkRoot(t *testing.T) {
	target := t.TempDir()
	root := filepath.Join(t.TempDir(), "run-ticket-jti-link")
	if err := os.Symlink(target, root); err != nil {
		t.Skipf("symlink unavailable on this platform: %v", err)
	}
	if _, err := NewFileRunTicketJTIStore(root); !errors.Is(err, ErrRunTicketJTIStoreUnavailable) {
		t.Fatalf("symlink store error=%v", err)
	}
}

func TestFileRunTicketJTIStoreProbeDoesNotLeaveState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "run-ticket-jtis")
	store, err := NewFileRunTicketJTIStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Probe(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("probe entries=%d err=%v", len(entries), err)
	}
}

func TestUnavailableRunTicketJTIStoreFailsClosed(t *testing.T) {
	store := NewUnavailableRunTicketJTIStore()
	claimed, err := store.Consume(context.Background(), "ticket-jti", time.Now().UTC().Add(time.Minute))
	if claimed || !errors.Is(err, ErrRunTicketJTIStoreUnavailable) {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
}
