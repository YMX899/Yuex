package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"time"
)

const (
	runTicketJTICleanupLimit   = 64
	runTicketJTIStoreLockTTL   = 2 * time.Minute
	runTicketJTIStoreLockWait  = 5 * time.Second
	runTicketJTIStoreLockRetry = 5 * time.Millisecond
)

// FileRunTicketJTIStore is the Host-local durable replay boundary for run
// tickets. It stores only sha256(jti) in a Host state directory and uses
// O_EXCL creation so separate Adapter processes sharing the same state disk
// cannot both consume one ticket.
//
// It intentionally owns only the Adapter-side replay fact. Gateway acceptance
// still needs its own durable transaction keyed by runTicketJtiHash and
// dispatchIdentity; this store must never be used to claim that an accepted
// Gateway Run was persisted.
type FileRunTicketJTIStore struct {
	root string
	mu   sync.Mutex
	now  func() time.Time
}

type runTicketJTIRecord struct {
	ExpiresAt int64 `json:"expiresAt"`
}

func NewFileRunTicketJTIStore(root string) (*FileRunTicketJTIStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, ErrRunTicketJTIStoreUnavailable
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, ErrRunTicketJTIStoreUnavailable
	}
	if err := ensureRunTicketJTIStoreDirectory(absRoot); err != nil {
		return nil, ErrRunTicketJTIStoreUnavailable
	}
	return &FileRunTicketJTIStore{root: absRoot, now: time.Now}, nil
}

func (s *FileRunTicketJTIStore) Health(ctx context.Context) error {
	if s == nil || strings.TrimSpace(s.root) == "" {
		return ErrRunTicketJTIStoreUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ensureRunTicketJTIStoreDirectory(s.root); err != nil {
		return ErrRunTicketJTIStoreUnavailable
	}
	return nil
}

func (*FileRunTicketJTIStore) Durable() bool {
	// The production Runtime Host image is Linux so the file and parent
	// directory can both be fsynced. Windows remains supported for local reopen
	// tests, but it cannot certify the same directory-entry durability here.
	return goruntime.GOOS != "windows"
}

func (s *FileRunTicketJTIStore) Probe(ctx context.Context) error {
	if err := s.Health(ctx); err != nil {
		return ErrRunTicketJTIStoreUnavailable
	}
	file, err := os.CreateTemp(s.root, ".run-ticket-jti-probe-")
	if err != nil {
		return ErrRunTicketJTIStoreUnavailable
	}
	path := file.Name()
	writeErr := func() error {
		if _, err := file.WriteString("1\n"); err != nil {
			return err
		}
		return file.Sync()
	}()
	closeErr := file.Close()
	removeErr := os.Remove(path)
	syncErr := syncRunTicketJTIStoreDirectory(s.root)
	if writeErr != nil || closeErr != nil || removeErr != nil || syncErr != nil {
		return ErrRunTicketJTIStoreUnavailable
	}
	return nil
}

func (s *FileRunTicketJTIStore) Consume(ctx context.Context, jti string, expiresAt time.Time) (bool, error) {
	if strings.TrimSpace(jti) == "" || !expiresAt.After(s.currentTime()) {
		return false, nil
	}
	if err := s.Health(ctx); err != nil {
		return false, ErrRunTicketJTIStoreUnavailable
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.acquireConsumeLock(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = release() }()
	if err := s.Health(ctx); err != nil {
		return false, ErrRunTicketJTIStoreUnavailable
	}
	now := s.currentTime()
	if err := s.purgeExpired(now); err != nil {
		return false, ErrRunTicketJTIStoreUnavailable
	}
	path := s.recordPath(jti)
	for attempt := 0; attempt < 2; attempt++ {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			writeErr := json.NewEncoder(file).Encode(runTicketJTIRecord{ExpiresAt: expiresAt.UTC().Unix()})
			if writeErr == nil {
				writeErr = file.Sync()
			}
			closeErr := file.Close()
			if writeErr != nil || closeErr != nil || syncRunTicketJTIStoreDirectory(s.root) != nil {
				_ = os.Remove(path)
				return false, ErrRunTicketJTIStoreUnavailable
			}
			return true, nil
		}
		if !os.IsExist(err) {
			return false, ErrRunTicketJTIStoreUnavailable
		}
		record, readErr := readRunTicketJTIRecord(path)
		if readErr != nil {
			return false, ErrRunTicketJTIStoreUnavailable
		}
		if record.ExpiresAt > now.Unix() {
			return false, nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return false, ErrRunTicketJTIStoreUnavailable
		}
	}
	return false, ErrRunTicketJTIStoreUnavailable
}

func (s *FileRunTicketJTIStore) currentTime() time.Time {
	if s != nil && s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}

func (s *FileRunTicketJTIStore) recordPath(jti string) string {
	sum := sha256.Sum256([]byte(jti))
	return filepath.Join(s.root, hex.EncodeToString(sum[:])+".json")
}

func (s *FileRunTicketJTIStore) acquireConsumeLock(ctx context.Context) (func() error, error) {
	lockPath := filepath.Join(s.root, ".consume.lock")
	waitDeadline := time.Now().UTC().Add(runTicketJTIStoreLockWait)
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("%w: jti store lock context", ErrRunTicketJTIStoreUnavailable)
		}
		file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			// The lease lifetime is represented by the lock file's mtime. Do not
			// encode and reread a JSON lease here: on Windows os.Open does not
			// share delete access, so contenders repeatedly reading this file can
			// starve the owner attempting to remove it.
			syncErr := file.Sync()
			closeErr := file.Close()
			if syncErr != nil || closeErr != nil || syncRunTicketJTIStoreDirectory(s.root) != nil {
				_ = os.Remove(lockPath)
				return nil, fmt.Errorf("%w: jti store lock write", ErrRunTicketJTIStoreUnavailable)
			}
			return func() error {
				deadline := time.Now().UTC().Add(time.Second)
				for {
					err := os.Remove(lockPath)
					if err == nil || os.IsNotExist(err) {
						return syncRunTicketJTIStoreDirectory(s.root)
					}
					if time.Now().UTC().After(deadline) {
						return err
					}
					time.Sleep(runTicketJTIStoreLockRetry)
				}
			}, nil
		}
		if !os.IsExist(err) {
			// A concurrent reader can surface a transient sharing violation on
			// Windows while another Host process releases the lock. Treat it as
			// busy and keep the same bounded, fail-closed wait rather than
			// misclassifying the duplicate claim as a storage success.
			if time.Now().UTC().After(waitDeadline) {
				return nil, fmt.Errorf("%w: jti store lock create", ErrRunTicketJTIStoreUnavailable)
			}
			time.Sleep(runTicketJTIStoreLockRetry)
			continue
		}
		if s.consumeLockExpired(lockPath, time.Now().UTC()) {
			if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("%w: jti store stale lock removal", ErrRunTicketJTIStoreUnavailable)
			}
			continue
		}
		if time.Now().UTC().After(waitDeadline) {
			return nil, fmt.Errorf("%w: jti store lock wait timeout", ErrRunTicketJTIStoreUnavailable)
		}
		timer := time.NewTimer(runTicketJTIStoreLockRetry)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, fmt.Errorf("%w: jti store lock context", ErrRunTicketJTIStoreUnavailable)
		case <-timer.C:
		}
	}
}

func (s *FileRunTicketJTIStore) consumeLockExpired(path string, now time.Time) bool {
	info, err := os.Lstat(path)
	return err == nil && !info.ModTime().UTC().Add(runTicketJTIStoreLockTTL).After(now)
}

func (s *FileRunTicketJTIStore) purgeExpired(now time.Time) error {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	checked := 0
	for _, entry := range entries {
		if checked >= runTicketJTICleanupLimit {
			break
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || len(entry.Name()) != len(strings.Repeat("0", 64))+len(".json") {
			continue
		}
		checked++
		path := filepath.Join(s.root, entry.Name())
		record, readErr := readRunTicketJTIRecord(path)
		if readErr != nil {
			return readErr
		}
		if record.ExpiresAt <= now.Unix() {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func readRunTicketJTIRecord(path string) (runTicketJTIRecord, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return runTicketJTIRecord{}, err
	}
	var record runTicketJTIRecord
	if err := json.Unmarshal(raw, &record); err != nil || record.ExpiresAt <= 0 {
		return runTicketJTIRecord{}, fmt.Errorf("invalid run ticket jti record")
	}
	return record, nil
}

func ensureRunTicketJTIStoreDirectory(root string) error {
	info, err := os.Lstat(root)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(root, 0o750); err != nil {
			return err
		}
		info, err = os.Lstat(root)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("unsafe run ticket jti store root")
	}
	return nil
}

func syncRunTicketJTIStoreDirectory(root string) error {
	// Windows does not expose a portable directory fsync through os.File. The
	// production Runtime Host deployment is Linux; local Windows tests still
	// exercise reopen and O_EXCL replay behavior.
	if goruntime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// UnavailableRunTicketJTIStore is installed only when production durable-store
// setup failed. It prevents the materializer from silently falling back to a
// memory map before startup and request validation surface the safe 503 code.
type UnavailableRunTicketJTIStore struct{}

func NewUnavailableRunTicketJTIStore() *UnavailableRunTicketJTIStore {
	return &UnavailableRunTicketJTIStore{}
}

func (*UnavailableRunTicketJTIStore) Health(context.Context) error {
	return ErrRunTicketJTIStoreUnavailable
}

func (*UnavailableRunTicketJTIStore) Durable() bool { return false }

func (*UnavailableRunTicketJTIStore) Probe(context.Context) error {
	return ErrRunTicketJTIStoreUnavailable
}

func (*UnavailableRunTicketJTIStore) Consume(context.Context, string, time.Time) (bool, error) {
	return false, ErrRunTicketJTIStoreUnavailable
}
