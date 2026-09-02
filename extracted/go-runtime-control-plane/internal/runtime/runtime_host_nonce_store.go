package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisRuntimeHostNonceStore is the production replay boundary for signed
// RuntimeHost heartbeats. It uses an atomic SET NX with a TTL longer than the
// accepted heartbeat window and never falls back to process memory.
type RedisRuntimeHostNonceStore struct {
	client redis.UniversalClient
	prefix string
}

func NewRedisRuntimeHostNonceStore(addr, password string, database int) (*RedisRuntimeHostNonceStore, error) {
	if strings.TrimSpace(addr) == "" || database < 0 {
		return nil, runtimeHostMTLSConfigError()
	}
	return &RedisRuntimeHostNonceStore{
		client: redis.NewClient(&redis.Options{Addr: strings.TrimSpace(addr), Password: password, DB: database}),
		prefix: "huahuo:runtime-host:heartbeat-nonce:",
	}, nil
}

func (s *RedisRuntimeHostNonceStore) Ping(ctx context.Context) error {
	if s == nil || s.client == nil {
		return runtimeHostMTLSConfigError()
	}
	if err := s.client.Ping(ctx).Err(); err != nil {
		return runtimeHostMTLSConfigError()
	}
	return nil
}

func (s *RedisRuntimeHostNonceStore) Claim(ctx context.Context, principal RuntimeHostPrincipal, nonce string, expiresAt time.Time) error {
	if s == nil || s.client == nil || !runtimeHostPrincipalValid(principal) || strings.TrimSpace(nonce) == "" || expiresAt.IsZero() {
		return ErrRuntimeHostUnauthorized
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ttl := time.Until(expiresAt.UTC())
	if ttl <= 0 {
		return ErrRuntimeHeartbeatStale
	}
	claimed, err := s.client.SetNX(ctx, s.nonceKey(principal, nonce), "1", ttl).Result()
	if err != nil {
		return fmt.Errorf("%w: nonce store unavailable", ErrRuntimeHostUnauthorized)
	}
	if !claimed {
		return ErrRuntimeHeartbeatStale
	}
	return nil
}

func (s *RedisRuntimeHostNonceStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *RedisRuntimeHostNonceStore) nonceKey(principal RuntimeHostPrincipal, nonce string) string {
	sum := sha256.Sum256([]byte(principal.Environment + "\x00" + principal.RuntimeHostID + "\x00" + principal.InstanceID + "\x00" + nonce))
	return s.prefix + hex.EncodeToString(sum[:])
}
