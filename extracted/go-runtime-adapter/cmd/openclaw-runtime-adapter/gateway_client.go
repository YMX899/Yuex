package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	gatewayProtocolVersion        = 4
	defaultGatewayMaxPendingCalls = 128
)

var errGatewayTransportUnavailable = errors.New("gateway transport unavailable")

type gatewayCallResult struct {
	payload json.RawMessage
	err     error
	conn    *websocket.Conn
}

type gatewayPendingCall struct {
	expectFinal bool
	result      chan gatewayCallResult
	conn        *websocket.Conn
	written     bool
}

type persistentGatewayClient struct {
	url    string
	token  string
	dialer *websocket.Dialer

	connectMu sync.Mutex
	connMu    sync.RWMutex
	conn      *websocket.Conn
	writeMu   sync.Mutex
	pendingMu sync.Mutex
	pending   map[string]*gatewayPendingCall

	maxPendingCalls int
}

func newPersistentGatewayClient(url, token string) *persistentGatewayClient {
	return newPersistentGatewayClientWithDialer(url, token, nil)
}

// newPersistentGatewayClientWithDialer creates an isolated Gateway connection
// pool. Restart recovery supplies a distinct mutual-TLS dialer here rather
// than reusing the token-authenticated execution connection.
func newPersistentGatewayClientWithDialer(url, token string, dialer *websocket.Dialer) *persistentGatewayClient {
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	return &persistentGatewayClient{
		url: strings.TrimSpace(url), token: strings.TrimSpace(token), dialer: dialer,
		pending:         map[string]*gatewayPendingCall{},
		maxPendingCalls: defaultGatewayMaxPendingCalls,
	}
}

func (c *persistentGatewayClient) Invoke(ctx context.Context, method string, params map[string]any, timeout time.Duration) ([]byte, []byte, error) {
	if c == nil || c.url == "" || c.token == "" || strings.TrimSpace(method) == "" {
		return nil, nil, fmt.Errorf("gateway transport is not configured")
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	id := gatewayRequestID()
	pending := &gatewayPendingCall{expectFinal: method == "enterprise.runtime.run", result: make(chan gatewayCallResult, 1)}
	if err := c.addPending(id, pending); err != nil {
		return nil, []byte(err.Error()), err
	}
	defer c.removePending(id, pending)

	frame := map[string]any{"type": "req", "id": id, "method": method, "params": params}
	replayed := false
	for {
		conn, err := c.connectionForPending(ctx, id, pending)
		if err != nil {
			return nil, []byte(err.Error()), err
		}
		if err := c.writeRequest(ctx, conn, id, pending, frame); err != nil {
			if !replayed && gatewaySubmitReplayable(method, params) && c.pendingWasWritten(id, pending) && errors.Is(err, errGatewayTransportUnavailable) {
				replayed = true
				continue
			}
			return nil, []byte(err.Error()), err
		}
		result, err := waitGatewayResult(ctx, pending, conn)
		if err == nil {
			return append([]byte(nil), result.payload...), nil, nil
		}
		if !replayed && gatewaySubmitReplayable(method, params) && c.pendingWasWritten(id, pending) && errors.Is(err, errGatewayTransportUnavailable) {
			replayed = true
			continue
		}
		return nil, []byte(err.Error()), err
	}
}

func (c *persistentGatewayClient) addPending(id string, pending *gatewayPendingCall) error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if c.pending == nil {
		c.pending = map[string]*gatewayPendingCall{}
	}
	limit := c.maxPendingCalls
	if limit <= 0 {
		limit = defaultGatewayMaxPendingCalls
	}
	if len(c.pending) >= limit {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	c.pending[id] = pending
	return nil
}

func (c *persistentGatewayClient) connectionForPending(ctx context.Context, id string, pending *gatewayPendingCall) (*websocket.Conn, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := c.ensureConnected(ctx)
		if err != nil {
			return nil, errGatewayTransportUnavailable
		}
		if c.attachPendingToConnection(id, pending, conn) {
			return conn, nil
		}
	}
}

func (c *persistentGatewayClient) attachPendingToConnection(id string, pending *gatewayPendingCall, conn *websocket.Conn) bool {
	c.connMu.RLock()
	connected := c.conn == conn
	c.connMu.RUnlock()
	if !connected {
		return false
	}
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if c.pending[id] != pending {
		return false
	}
	for {
		select {
		case <-pending.result:
		default:
			pending.conn = conn
			return true
		}
	}
}

func (c *persistentGatewayClient) writeRequest(ctx context.Context, conn *websocket.Conn, id string, pending *gatewayPendingCall, frame map[string]any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.currentConnection() != conn {
		c.invalidate(conn, errGatewayTransportUnavailable)
		return errGatewayTransportUnavailable
	}
	c.pendingMu.Lock()
	if c.pending[id] != pending || pending.conn != conn {
		c.pendingMu.Unlock()
		return errGatewayTransportUnavailable
	}
	// A failed WebSocket write may have reached the Gateway. Mark it before the
	// write so only idempotent async submit can recover the unknown outcome.
	pending.written = true
	c.pendingMu.Unlock()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
		defer conn.SetWriteDeadline(time.Time{})
	}
	if err := conn.WriteJSON(frame); err != nil {
		c.invalidate(conn, errGatewayTransportUnavailable)
		return errGatewayTransportUnavailable
	}
	return nil
}

func (c *persistentGatewayClient) pendingWasWritten(id string, pending *gatewayPendingCall) bool {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	return c.pending[id] == pending && pending.written
}

func waitGatewayResult(ctx context.Context, pending *gatewayPendingCall, conn *websocket.Conn) (gatewayCallResult, error) {
	for {
		select {
		case <-ctx.Done():
			return gatewayCallResult{}, ctx.Err()
		case result := <-pending.result:
			if result.conn != conn {
				continue
			}
			if result.err != nil {
				return gatewayCallResult{}, result.err
			}
			return result, nil
		}
	}
}

func gatewaySubmitReplayable(method string, params map[string]any) bool {
	if method != "enterprise.runtime.submit" {
		return false
	}
	idempotencyKey := gatewayStringValue(params["idempotencyKey"])
	runID := gatewayStringValue(gatewayMapValue(params["spec"])["runId"])
	return idempotencyKey != "" && runID != "" && idempotencyKey == runID
}

func (c *persistentGatewayClient) ensureConnected(ctx context.Context) (*websocket.Conn, error) {
	if conn := c.currentConnection(); conn != nil {
		return conn, nil
	}
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	if conn := c.currentConnection(); conn != nil {
		return conn, nil
	}
	dialer := c.dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	conn, _, err := dialer.DialContext(ctx, c.url, nil)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(8 << 20)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	var challenge struct {
		Type    string `json:"type"`
		Event   string `json:"event"`
		Payload struct {
			Nonce string `json:"nonce"`
		} `json:"payload"`
	}
	if err := conn.ReadJSON(&challenge); err != nil || challenge.Type != "event" || challenge.Event != "connect.challenge" || strings.TrimSpace(challenge.Payload.Nonce) == "" {
		_ = conn.Close()
		return nil, fmt.Errorf("gateway connect challenge failed")
	}
	connectID := gatewayRequestID()
	connectFrame := map[string]any{
		"type": "req", "id": connectID, "method": "connect",
		"params": map[string]any{
			"minProtocol": gatewayProtocolVersion,
			"maxProtocol": gatewayProtocolVersion,
			"client": map[string]any{
				"id": "gateway-client", "displayName": "huahuo-runtime-adapter",
				"version": "v0.5", "platform": runtime.GOOS, "mode": "backend",
				"instanceId": gatewayRequestID(),
			},
			"caps": []string{}, "role": "operator",
			"scopes": []string{"operator.admin", "operator.read", "operator.write"},
			"auth":   map[string]any{"token": c.token},
		},
	}
	if err := conn.WriteJSON(connectFrame); err != nil {
		_ = conn.Close()
		return nil, err
	}
	var response struct {
		Type    string          `json:"type"`
		ID      string          `json:"id"`
		OK      bool            `json:"ok"`
		Payload json.RawMessage `json:"payload"`
		Error   struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := conn.ReadJSON(&response); err != nil || response.Type != "res" || response.ID != connectID || !response.OK {
		_ = conn.Close()
		message := strings.TrimSpace(response.Error.Code + " " + response.Error.Message)
		if message == "" {
			message = "gateway connect rejected"
		}
		return nil, fmt.Errorf("%s", message)
	}
	_ = conn.SetReadDeadline(time.Time{})
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	go c.readLoop(conn)
	return conn, nil
}

func (c *persistentGatewayClient) readLoop(conn *websocket.Conn) {
	for {
		var frame struct {
			Type    string          `json:"type"`
			ID      string          `json:"id"`
			OK      bool            `json:"ok"`
			Payload json.RawMessage `json:"payload"`
			Error   struct {
				Code       string `json:"code"`
				Message    string `json:"message"`
				Retryable  bool   `json:"retryable"`
				RetryAfter int64  `json:"retryAfterMs"`
			} `json:"error"`
		}
		if err := conn.ReadJSON(&frame); err != nil {
			c.invalidate(conn, errGatewayTransportUnavailable)
			return
		}
		if frame.Type != "res" || frame.ID == "" {
			continue
		}
		c.pendingMu.Lock()
		pending, ok := c.pending[frame.ID]
		if !ok || pending.conn != conn {
			c.pendingMu.Unlock()
			continue
		}
		if pending.expectFinal && frame.OK && gatewayAccepted(frame.Payload) {
			c.pendingMu.Unlock()
			continue
		}
		delete(c.pending, frame.ID)
		c.pendingMu.Unlock()
		if frame.OK {
			pending.result <- gatewayCallResult{payload: frame.Payload, conn: conn}
			continue
		}
		message := strings.TrimSpace(frame.Error.Code + " " + frame.Error.Message)
		if message == "" {
			message = "gateway request failed"
		}
		pending.result <- gatewayCallResult{err: fmt.Errorf("%s", c.redactGatewayToken(message)), conn: conn}
	}
}

func (c *persistentGatewayClient) invalidate(conn *websocket.Conn, cause error) {
	c.connMu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	if cause == nil {
		cause = errGatewayTransportUnavailable
	}
	c.pendingMu.Lock()
	for _, call := range c.pending {
		if call.conn != conn {
			continue
		}
		call.conn = nil
		select {
		case call.result <- gatewayCallResult{err: cause, conn: conn}:
		default:
		}
	}
	c.pendingMu.Unlock()
	c.connMu.Unlock()
	_ = conn.Close()
}

func (c *persistentGatewayClient) currentConnection() *websocket.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

func (c *persistentGatewayClient) removePending(id string, pending *gatewayPendingCall) {
	c.pendingMu.Lock()
	if c.pending[id] == pending {
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()
}

func (c *persistentGatewayClient) redactGatewayToken(message string) string {
	message = strings.TrimSpace(message)
	if c == nil || c.token == "" {
		return message
	}
	return strings.ReplaceAll(message, c.token, "[redacted]")
}

func gatewayStringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func gatewayMapValue(value any) map[string]any {
	mapValue, _ := value.(map[string]any)
	return mapValue
}

func gatewayAccepted(payload json.RawMessage) bool {
	var value struct {
		Status string `json:"status"`
	}
	return json.Unmarshal(payload, &value) == nil && value.Status == "accepted"
}

func gatewayRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}
