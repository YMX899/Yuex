package runtime

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"huahuoai/backend/source/internal/persistence"
)

const runtimeSessionCipherPrefix = "enc:v1:"

type ProductSessionBindingCommand struct {
	ThreadID                   string
	TenantID                   string
	UserID                     string
	WorkspaceID                string
	AgentProfile               string
	ContextGeneration          int64
	ManifestVersion            string
	AgentHash                  string
	SessionKeyEncryptionSecret string
}

type ProductSessionBinding struct {
	BindingID               string
	ThreadID                string
	TenantID                string
	UserID                  string
	WorkspaceID             string
	AgentProfile            string
	ContextGeneration       int64
	SessionGeneration       int
	OpenClawSessionKey      string
	OpenClawSessionKeyHash  string
	SessionStoreID          string
	RuntimeHostID           string
	ManifestVersion         string
	AgentHash               string
	Status                  string
	RecoveryMode            string
	RecoveredFromGeneration int
	ContextLossRisk         string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

func (r *RuntimeHostRepository) EnsureProductSessionBinding(ctx context.Context, command ProductSessionBindingCommand) (ProductSessionBinding, error) {
	if err := validateProductSessionBindingCommand(command); err != nil {
		return ProductSessionBinding{}, err
	}
	if r.postgresReady() {
		return r.ensureProductSessionBindingPostgres(ctx, command)
	}
	return r.ensureProductSessionBindingMemory(command)
}

func (r *RuntimeHostRepository) GetProductSessionBinding(ctx context.Context, binding ProductSessionHostBinding, encryptionSecret string) (ProductSessionBinding, error) {
	if binding.TenantID == "" || binding.ThreadID == "" || binding.AgentProfile == "" || binding.ContextGeneration < 1 || binding.SessionGeneration < 1 || len(strings.TrimSpace(encryptionSecret)) < 16 {
		return ProductSessionBinding{}, fmt.Errorf("RUNTIME_SESSION_BINDING_UNAVAILABLE")
	}
	if r.postgresReady() {
		row := r.db.Pool.QueryRow(ctx, productSessionBindingSelect+` where tenant_id=$1 and thread_id=$2 and agent_profile=$3 and context_generation=$4 and session_generation=$5 and status='active'`, binding.TenantID, binding.ThreadID, binding.AgentProfile, binding.ContextGeneration, binding.SessionGeneration)
		stored, ciphertext, err := scanProductSessionBinding(row.Scan)
		if err != nil {
			return ProductSessionBinding{}, err
		}
		stored.OpenClawSessionKey, err = decryptRuntimeSessionKey(ciphertext, encryptionSecret, productSessionBindingAAD(stored))
		if err != nil {
			return ProductSessionBinding{}, fmt.Errorf("RUNTIME_SESSION_BINDING_UNAVAILABLE")
		}
		return stored, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stored, ok := r.sessionBindings[productSessionHostKey(binding)]
	if !ok || stored.Status != "active" {
		return ProductSessionBinding{}, fmt.Errorf("NOT_FOUND")
	}
	return stored, nil
}

func (r *RuntimeHostRepository) RotateProductSessionGeneration(ctx context.Context, current ProductSessionHostBinding, command ProductSessionBindingCommand, recoveryMode, contextLossRisk string) (ProductSessionBinding, error) {
	if err := validateProductSessionBindingCommand(command); err != nil {
		return ProductSessionBinding{}, err
	}
	if current.TenantID != command.TenantID || current.ThreadID != command.ThreadID || current.AgentProfile != command.AgentProfile || current.ContextGeneration != command.ContextGeneration || current.SessionGeneration < 1 {
		return ProductSessionBinding{}, fmt.Errorf("RUNTIME_SESSION_BINDING_CONFLICT")
	}
	if recoveryMode == "" {
		recoveryMode = "new_generation"
	}
	if contextLossRisk == "" {
		contextLossRisk = "unknown"
	}
	if r.postgresReady() {
		return r.rotateProductSessionGenerationPostgres(ctx, current, command, recoveryMode, contextLossRisk)
	}
	return r.rotateProductSessionGenerationMemory(current, command, recoveryMode, contextLossRisk)
}

func (r *RuntimeHostRepository) RotateProductSessionsForHost(ctx context.Context, hostID, encryptionSecret, recoveryMode, contextLossRisk string) ([]ProductSessionBinding, error) {
	if hostID == "" || len(strings.TrimSpace(encryptionSecret)) < 16 {
		return nil, fmt.Errorf("RUNTIME_SESSION_BINDING_UNAVAILABLE")
	}
	bindings := []ProductSessionBinding{}
	if r.postgresReady() {
		rows, err := r.db.Pool.Query(ctx, productSessionBindingSelect+` where runtime_host_id=$1 and status='active' order by thread_id,agent_profile,context_generation,session_generation`, hostID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			binding, _, err := scanProductSessionBinding(rows.Scan)
			if err != nil {
				return nil, err
			}
			bindings = append(bindings, binding)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	} else {
		r.mu.Lock()
		for _, binding := range r.sessionBindings {
			if binding.RuntimeHostID == hostID && binding.Status == "active" {
				bindings = append(bindings, binding)
			}
		}
		r.mu.Unlock()
	}
	rotated := []ProductSessionBinding{}
	for _, binding := range bindings {
		next, err := r.RotateProductSessionGeneration(ctx, ProductSessionHostBinding{
			TenantID: binding.TenantID, ThreadID: binding.ThreadID, AgentProfile: binding.AgentProfile, ContextGeneration: binding.ContextGeneration,
			SessionGeneration: binding.SessionGeneration, RuntimeHostID: binding.RuntimeHostID, SessionStoreID: binding.SessionStoreID,
		}, ProductSessionBindingCommand{
			ThreadID: binding.ThreadID, TenantID: binding.TenantID, UserID: binding.UserID, WorkspaceID: binding.WorkspaceID,
			AgentProfile: binding.AgentProfile, ContextGeneration: binding.ContextGeneration,
			ManifestVersion: binding.ManifestVersion, AgentHash: binding.AgentHash,
			SessionKeyEncryptionSecret: encryptionSecret,
		}, recoveryMode, contextLossRisk)
		if err != nil {
			return rotated, err
		}
		rotated = append(rotated, next)
	}
	return rotated, nil
}

func (r *RuntimeHostRepository) ensureProductSessionBindingMemory(command ProductSessionBindingCommand) (ProductSessionBinding, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, binding := range r.sessionBindings {
		if binding.TenantID == command.TenantID && binding.ThreadID == command.ThreadID && binding.AgentProfile == command.AgentProfile && binding.ContextGeneration == command.ContextGeneration && binding.Status == "active" {
			if binding.TenantID != command.TenantID || binding.UserID != command.UserID || binding.WorkspaceID != command.WorkspaceID {
				return ProductSessionBinding{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
			}
			return binding, nil
		}
	}
	binding, _, err := newProductSessionBinding(command, 1, "", 0, "")
	if err != nil {
		return ProductSessionBinding{}, err
	}
	r.sessionBindings[productSessionHostKey(ProductSessionHostBinding{TenantID: binding.TenantID, ThreadID: binding.ThreadID, AgentProfile: binding.AgentProfile, ContextGeneration: binding.ContextGeneration, SessionGeneration: binding.SessionGeneration})] = binding
	return binding, nil
}

func (r *RuntimeHostRepository) rotateProductSessionGenerationMemory(current ProductSessionHostBinding, command ProductSessionBindingCommand, recoveryMode, contextLossRisk string) (ProductSessionBinding, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := productSessionHostKey(current)
	stored, ok := r.sessionBindings[key]
	if !ok || stored.Status != "active" {
		return ProductSessionBinding{}, fmt.Errorf("RUNTIME_SESSION_BINDING_CONFLICT")
	}
	stored.Status = "orphaned"
	stored.UpdatedAt = time.Now().UTC()
	r.sessionBindings[key] = stored
	delete(r.sessionHosts, key)
	next, _, err := newProductSessionBinding(command, current.SessionGeneration+1, recoveryMode, current.SessionGeneration, contextLossRisk)
	if err != nil {
		return ProductSessionBinding{}, err
	}
	r.sessionBindings[productSessionHostKey(ProductSessionHostBinding{TenantID: next.TenantID, ThreadID: next.ThreadID, AgentProfile: next.AgentProfile, ContextGeneration: next.ContextGeneration, SessionGeneration: next.SessionGeneration})] = next
	return next, nil
}

func (r *RuntimeHostRepository) ensureProductSessionBindingPostgres(ctx context.Context, command ProductSessionBindingCommand) (ProductSessionBinding, error) {
	var result ProductSessionBinding
	err := r.db.WithTx(ctx, func(tx *persistence.Tx) error {
		if err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended(@key,0))`, map[string]any{"key": productSessionBindingLockKey(command.TenantID, command.ThreadID, command.AgentProfile, command.ContextGeneration)}); err != nil {
			return err
		}
		ownerRows, err := tx.Query(ctx, `select t.user_id,coalesce(t.active_workspace_id,t.workspace_id) workspace_id,coalesce(t.context_generation,1) context_generation,w.tenant_id from chat_threads t join workspaces w on w.workspace_id=coalesce(t.active_workspace_id,t.workspace_id) where t.thread_id=@thread for update`, map[string]any{"thread": command.ThreadID})
		if err != nil || len(ownerRows) != 1 {
			return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
		}
		if fmt.Sprint(ownerRows[0]["user_id"]) != command.UserID || fmt.Sprint(ownerRows[0]["workspace_id"]) != command.WorkspaceID {
			return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
		}
		if fmt.Sprint(ownerRows[0]["tenant_id"]) != command.TenantID || runtimeBindingInt64(ownerRows[0]["context_generation"]) != command.ContextGeneration {
			return fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
		}
		rows, err := tx.Query(ctx, productSessionBindingSelect+` where tenant_id=@tenant and thread_id=@thread and agent_profile=@agent and context_generation=@context and status='active' order by session_generation desc limit 1 for update`, map[string]any{"tenant": command.TenantID, "thread": command.ThreadID, "agent": command.AgentProfile, "context": command.ContextGeneration})
		if err != nil {
			return err
		}
		if len(rows) == 1 {
			stored, ciphertext, scanErr := productSessionBindingFromMap(rows[0])
			if scanErr != nil {
				return scanErr
			}
			if stored.TenantID != command.TenantID || stored.UserID != command.UserID || stored.WorkspaceID != command.WorkspaceID {
				return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
			}
			key, decryptErr := decryptRuntimeSessionKey(ciphertext, command.SessionKeyEncryptionSecret, productSessionBindingAAD(stored))
			if decryptErr == nil {
				stored.OpenClawSessionKey = key
				result = stored
				return nil
			}
			if err := tx.Exec(ctx, `update thread_agent_runtime_bindings set status='rotated',recovery_mode='legacy_ciphertext_rotation',context_loss_risk='medium',rotated_at=now(),updated_at=now() where binding_id=@id and status='active'`, map[string]any{"id": stored.BindingID}); err != nil {
				return err
			}
			return insertProductSessionBindingTx(ctx, tx, command, stored.SessionGeneration+1, "legacy_ciphertext_rotation", stored.SessionGeneration, "medium", &result)
		}
		return insertProductSessionBindingTx(ctx, tx, command, 1, "", 0, "", &result)
	})
	return result, err
}

func (r *RuntimeHostRepository) rotateProductSessionGenerationPostgres(ctx context.Context, current ProductSessionHostBinding, command ProductSessionBindingCommand, recoveryMode, contextLossRisk string) (ProductSessionBinding, error) {
	var result ProductSessionBinding
	err := r.db.WithTx(ctx, func(tx *persistence.Tx) error {
		if err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended(@key,0))`, map[string]any{"key": productSessionBindingLockKey(command.TenantID, command.ThreadID, command.AgentProfile, command.ContextGeneration)}); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, productSessionBindingSelect+` where tenant_id=@tenant and thread_id=@thread and agent_profile=@agent and context_generation=@context and session_generation=@generation and status='active' for update`, map[string]any{"tenant": current.TenantID, "thread": current.ThreadID, "agent": current.AgentProfile, "context": current.ContextGeneration, "generation": current.SessionGeneration})
		if err != nil || len(rows) != 1 {
			return fmt.Errorf("RUNTIME_SESSION_BINDING_CONFLICT")
		}
		if err := tx.Exec(ctx, `update thread_agent_runtime_bindings set status='orphaned',runtime_host_id=null,session_store_id=null,recovery_mode=@mode,context_loss_risk=@risk,rotated_at=now(),updated_at=now() where binding_id=@id and status='active'`, map[string]any{"id": fmt.Sprint(rows[0]["binding_id"]), "mode": recoveryMode, "risk": contextLossRisk}); err != nil {
			return err
		}
		return insertProductSessionBindingTx(ctx, tx, command, current.SessionGeneration+1, recoveryMode, current.SessionGeneration, contextLossRisk, &result)
	})
	return result, err
}

func insertProductSessionBindingTx(ctx context.Context, tx *persistence.Tx, command ProductSessionBindingCommand, generation int, recoveryMode string, recoveredFrom int, contextLossRisk string, result *ProductSessionBinding) error {
	binding, ciphertext, err := newProductSessionBinding(command, generation, recoveryMode, recoveredFrom, contextLossRisk)
	if err != nil {
		return err
	}
	if err := tx.Exec(ctx, `insert into thread_agent_runtime_bindings(binding_id,thread_id,tenant_id,user_id,agent_profile,session_generation,context_generation,openclaw_session_key_ciphertext,openclaw_session_key_hash,workspace_id,manifest_version,agent_hash,status,recovery_mode,recovered_from_generation,context_loss_risk) values(@id,@thread,@tenant,@user,@agent,@generation,@context,@ciphertext,@hash,@workspace,@manifest,@agentHash,'active',nullif(@mode,''),nullif(@recovered,0),nullif(@risk,''))`, map[string]any{
		"id": binding.BindingID, "thread": binding.ThreadID, "tenant": binding.TenantID, "user": binding.UserID,
		"agent": binding.AgentProfile, "generation": binding.SessionGeneration, "context": binding.ContextGeneration,
		"ciphertext": ciphertext, "hash": binding.OpenClawSessionKeyHash, "workspace": binding.WorkspaceID,
		"manifest": binding.ManifestVersion, "agentHash": binding.AgentHash, "mode": recoveryMode, "recovered": recoveredFrom, "risk": contextLossRisk,
	}); err != nil {
		return err
	}
	*result = binding
	return nil
}

func newProductSessionBinding(command ProductSessionBindingCommand, generation int, recoveryMode string, recoveredFrom int, contextLossRisk string) (ProductSessionBinding, string, error) {
	sessionKey, err := newRuntimeSessionKey()
	if err != nil {
		return ProductSessionBinding{}, "", err
	}
	now := time.Now().UTC()
	binding := ProductSessionBinding{
		BindingID: stableProductSessionBindingID(command.TenantID, command.ThreadID, command.AgentProfile, command.ContextGeneration, generation),
		ThreadID:  command.ThreadID, TenantID: command.TenantID, UserID: command.UserID, WorkspaceID: command.WorkspaceID,
		AgentProfile: command.AgentProfile, ContextGeneration: command.ContextGeneration, SessionGeneration: generation,
		OpenClawSessionKey: sessionKey, OpenClawSessionKeyHash: runtimeSessionBindingHash(sessionKey),
		ManifestVersion: command.ManifestVersion, AgentHash: command.AgentHash, Status: "active",
		RecoveryMode: recoveryMode, RecoveredFromGeneration: recoveredFrom, ContextLossRisk: contextLossRisk,
		CreatedAt: now, UpdatedAt: now,
	}
	ciphertext, err := encryptRuntimeSessionKey(sessionKey, command.SessionKeyEncryptionSecret, productSessionBindingAAD(binding))
	return binding, ciphertext, err
}

func validateProductSessionBindingCommand(command ProductSessionBindingCommand) error {
	if command.ThreadID == "" || command.TenantID == "" || command.UserID == "" || command.WorkspaceID == "" || command.AgentProfile == "" || command.ContextGeneration < 1 || command.ManifestVersion == "" || command.AgentHash == "" || len(strings.TrimSpace(command.SessionKeyEncryptionSecret)) < 16 {
		return fmt.Errorf("RUNTIME_SESSION_BINDING_UNAVAILABLE")
	}
	return nil
}

func encryptRuntimeSessionKey(value, secret, aad string) (string, error) {
	block, err := aes.NewCipher(runtimeSessionEncryptionKey(secret))
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nil, nonce, []byte(value), []byte(aad))
	return runtimeSessionCipherPrefix + base64.RawURLEncoding.EncodeToString(append(nonce, sealed...)), nil
}

func decryptRuntimeSessionKey(value, secret, aad string) (string, error) {
	if !strings.HasPrefix(value, runtimeSessionCipherPrefix) {
		return "", fmt.Errorf("legacy session ciphertext")
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, runtimeSessionCipherPrefix))
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(runtimeSessionEncryptionKey(secret))
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil || len(raw) < gcm.NonceSize() {
		return "", fmt.Errorf("invalid session ciphertext")
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], []byte(aad))
	return string(plain), err
}

func runtimeSessionEncryptionKey(secret string) []byte {
	sum := sha256.Sum256([]byte(strings.TrimSpace(secret)))
	return sum[:]
}

func newRuntimeSessionKey() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "oc:ps:" + base64.RawURLEncoding.EncodeToString(raw), nil
}

func runtimeSessionBindingHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func stableProductSessionBindingID(tenantID, threadID, agentProfile string, contextGeneration int64, sessionGeneration int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d", tenantID, threadID, agentProfile, contextGeneration, sessionGeneration)))
	return "binding_" + hex.EncodeToString(sum[:])[:32]
}

func productSessionBindingAAD(binding ProductSessionBinding) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d", binding.TenantID, binding.ThreadID, binding.AgentProfile, binding.ContextGeneration, binding.SessionGeneration)
}

func productSessionBindingLockKey(tenantID, threadID, agentProfile string, contextGeneration int64) string {
	return fmt.Sprintf("runtime-session-binding:%s:%s:%s:%d", tenantID, threadID, agentProfile, contextGeneration)
}

const productSessionBindingSelect = `select binding_id,thread_id,tenant_id,user_id,workspace_id,agent_profile,context_generation,session_generation,openclaw_session_key_ciphertext,openclaw_session_key_hash,coalesce(session_store_id,'') session_store_id,coalesce(runtime_host_id,'') runtime_host_id,manifest_version,agent_hash,status,coalesce(recovery_mode,'') recovery_mode,coalesce(recovered_from_generation,0) recovered_from_generation,coalesce(context_loss_risk,'') context_loss_risk,created_at,updated_at from thread_agent_runtime_bindings`

func productSessionBindingFromMap(row map[string]any) (ProductSessionBinding, string, error) {
	binding := ProductSessionBinding{
		BindingID: fmt.Sprint(row["binding_id"]), ThreadID: fmt.Sprint(row["thread_id"]), TenantID: fmt.Sprint(row["tenant_id"]),
		UserID: fmt.Sprint(row["user_id"]), WorkspaceID: fmt.Sprint(row["workspace_id"]), AgentProfile: fmt.Sprint(row["agent_profile"]),
		ContextGeneration: runtimeBindingInt64(row["context_generation"]), SessionGeneration: int(runtimeBindingInt64(row["session_generation"])),
		OpenClawSessionKeyHash: fmt.Sprint(row["openclaw_session_key_hash"]), SessionStoreID: fmt.Sprint(row["session_store_id"]),
		RuntimeHostID: fmt.Sprint(row["runtime_host_id"]), ManifestVersion: fmt.Sprint(row["manifest_version"]), AgentHash: fmt.Sprint(row["agent_hash"]),
		Status: fmt.Sprint(row["status"]), RecoveryMode: fmt.Sprint(row["recovery_mode"]), RecoveredFromGeneration: int(runtimeBindingInt64(row["recovered_from_generation"])), ContextLossRisk: fmt.Sprint(row["context_loss_risk"]),
	}
	return binding, fmt.Sprint(row["openclaw_session_key_ciphertext"]), nil
}

func scanProductSessionBinding(scan func(...any) error) (ProductSessionBinding, string, error) {
	var binding ProductSessionBinding
	var ciphertext string
	err := scan(&binding.BindingID, &binding.ThreadID, &binding.TenantID, &binding.UserID, &binding.WorkspaceID, &binding.AgentProfile,
		&binding.ContextGeneration, &binding.SessionGeneration, &ciphertext, &binding.OpenClawSessionKeyHash, &binding.SessionStoreID,
		&binding.RuntimeHostID, &binding.ManifestVersion, &binding.AgentHash, &binding.Status, &binding.RecoveryMode,
		&binding.RecoveredFromGeneration, &binding.ContextLossRisk, &binding.CreatedAt, &binding.UpdatedAt)
	return binding, ciphertext, err
}

func runtimeBindingInt64(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int32:
		return int64(typed)
	case int:
		return int64(typed)
	default:
		var out int64
		_, _ = fmt.Sscan(fmt.Sprint(value), &out)
		return out
	}
}
