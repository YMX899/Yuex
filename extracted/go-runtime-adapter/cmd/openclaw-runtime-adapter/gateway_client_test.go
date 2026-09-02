package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestPersistentGatewayClientReusesConnectionAndWaitsForRunFinal(t *testing.T) {
	var connections atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connections.Add(1)
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"type": "event", "event": "connect.challenge", "payload": map[string]any{"nonce": "nonce-1"}})
		var connect map[string]any
		if conn.ReadJSON(&connect) != nil {
			return
		}
		connectParams := gatewayMapValue(connect["params"])
		if gatewayMapValue(connectParams["auth"])["token"] != "test-token" {
			return
		}
		_ = conn.WriteJSON(map[string]any{
			"type": "res", "id": connect["id"], "ok": true,
			"payload": map[string]any{"type": "hello-ok", "protocol": gatewayProtocolVersion},
		})
		for {
			var frame map[string]any
			if conn.ReadJSON(&frame) != nil {
				return
			}
			id := frame["id"]
			method := gatewayStringValue(frame["method"])
			if method == "enterprise.runtime.run" {
				_ = conn.WriteJSON(map[string]any{"type": "res", "id": id, "ok": true, "payload": map[string]any{"status": "accepted"}})
				_ = conn.WriteJSON(map[string]any{"type": "res", "id": id, "ok": true, "payload": map[string]any{"status": "succeeded", "finalAnswer": "done"}})
				continue
			}
			_ = conn.WriteJSON(map[string]any{"type": "res", "id": id, "ok": true, "payload": map[string]any{"status": "running", "method": method}})
		}
	}))
	defer server.Close()

	client := newPersistentGatewayClient("ws"+strings.TrimPrefix(server.URL, "http"), "test-token")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	results := make(chan map[string]any, 2)
	for _, method := range []string{"enterprise.runtime.status", "enterprise.runtime.run"} {
		method := method
		wg.Add(1)
		go func() {
			defer wg.Done()
			raw, _, err := client.Invoke(ctx, method, map[string]any{"runId": "run-1"}, time.Second)
			if err != nil {
				t.Errorf("Invoke(%s): %v", method, err)
				return
			}
			var value map[string]any
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Errorf("decode(%s): %v", method, err)
				return
			}
			results <- value
		}()
	}
	wg.Wait()
	close(results)
	var sawFinal bool
	for result := range results {
		if result["finalAnswer"] == "done" {
			sawFinal = true
		}
	}
	if !sawFinal {
		t.Fatal("enterprise.runtime.run returned accepted instead of final result")
	}
	if connections.Load() != 1 {
		t.Fatalf("gateway connections=%d want 1", connections.Load())
	}
}

func TestPersistentGatewayClientDoesNotExposeTokenInTransportErrors(t *testing.T) {
	client := newPersistentGatewayClient("ws://127.0.0.1:1", "sensitive-token")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, stderr, err := client.Invoke(ctx, "enterprise.runtime.status", map[string]any{"runId": "run-1"}, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected transport error")
	}
	if strings.Contains(string(stderr), "sensitive-token") || strings.Contains(err.Error(), "sensitive-token") {
		t.Fatal("gateway token leaked in transport error")
	}
}

func TestPersistentGatewayClientCorrelatesConcurrentResponses(t *testing.T) {
	const calls = 16
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		writeGatewayChallenge(t, conn)
		if !readGatewayConnect(t, conn) {
			return
		}
		requests := make([]map[string]any, 0, calls)
		for len(requests) < calls {
			var frame map[string]any
			if err := conn.ReadJSON(&frame); err != nil {
				t.Errorf("read request: %v", err)
				return
			}
			requests = append(requests, frame)
		}
		for index := len(requests) - 1; index >= 0; index-- {
			frame := requests[index]
			if err := conn.WriteJSON(map[string]any{
				"type": "res", "id": frame["id"], "ok": true,
				"payload": map[string]any{"request": gatewayMapValue(frame["params"])["request"]},
			}); err != nil {
				t.Errorf("write response: %v", err)
				return
			}
		}
	}))
	defer server.Close()

	client := newPersistentGatewayClient("ws"+strings.TrimPrefix(server.URL, "http"), "test-token")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := make(chan error, calls)
	for index := 0; index < calls; index++ {
		index := index
		go func() {
			raw, _, err := client.Invoke(ctx, "enterprise.runtime.status", map[string]any{"request": index}, time.Second)
			if err != nil {
				results <- err
				return
			}
			var response struct {
				Request int `json:"request"`
			}
			if err := json.Unmarshal(raw, &response); err != nil {
				results <- err
				return
			}
			if response.Request != index {
				results <- fmt.Errorf("response request=%d want %d", response.Request, index)
				return
			}
			results <- nil
		}()
	}
	for index := 0; index < calls; index++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestPersistentGatewayClientRecoversUnknownAsyncSubmitAfterDisconnect(t *testing.T) {
	var connections atomic.Int32
	var firstID string
	var firstIDMu sync.Mutex
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connection := connections.Add(1)
		writeGatewayChallenge(t, conn)
		if !readGatewayConnect(t, conn) {
			return
		}
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Errorf("read submit: %v", err)
			return
		}
		if gatewayStringValue(frame["method"]) != "enterprise.runtime.submit" {
			t.Errorf("method=%q", frame["method"])
			return
		}
		if connection == 1 {
			firstIDMu.Lock()
			firstID = gatewayStringValue(frame["id"])
			firstIDMu.Unlock()
			return
		}
		firstIDMu.Lock()
		wantID := firstID
		firstIDMu.Unlock()
		if gatewayStringValue(frame["id"]) != wantID {
			t.Errorf("replayed request id=%q want %q", frame["id"], wantID)
			return
		}
		params := gatewayMapValue(frame["params"])
		if params["idempotencyKey"] != "run-replay" || gatewayStringValue(gatewayMapValue(params["spec"])["runId"]) != "run-replay" {
			t.Errorf("replayed submit lost idempotency identity: %#v", params)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"type": "res", "id": frame["id"], "ok": true,
			"payload": map[string]any{"runId": "run-replay", "status": "accepted", "runtimeRequestId": "gateway-request-1"},
		}); err != nil {
			t.Errorf("write accepted response: %v", err)
		}
	}))
	defer server.Close()

	client := newPersistentGatewayClient("ws"+strings.TrimPrefix(server.URL, "http"), "test-token")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	raw, _, err := client.Invoke(ctx, "enterprise.runtime.submit", map[string]any{
		"idempotencyKey": "run-replay",
		"spec":           map[string]any{"runId": "run-replay"},
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.Status != "accepted" {
		t.Fatalf("result=%s err=%v", raw, err)
	}
	if connections.Load() != 2 {
		t.Fatalf("connections=%d want 2", connections.Load())
	}
}

func TestPersistentGatewayClientBoundsPendingCalls(t *testing.T) {
	firstRequest := make(chan struct{})
	releaseServer := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		writeGatewayChallenge(t, conn)
		if !readGatewayConnect(t, conn) {
			return
		}
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Errorf("read first request: %v", err)
			return
		}
		close(firstRequest)
		<-releaseServer
	}))
	defer func() {
		close(releaseServer)
		server.Close()
	}()

	client := newPersistentGatewayClient("ws"+strings.TrimPrefix(server.URL, "http"), "test-token")
	client.maxPendingCalls = 1
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstResult := make(chan error, 1)
	go func() {
		_, _, err := client.Invoke(firstCtx, "enterprise.runtime.status", map[string]any{"runId": "run-one"}, time.Second)
		firstResult <- err
	}()
	select {
	case <-firstRequest:
	case <-time.After(time.Second):
		t.Fatal("first request did not reach Gateway")
	}

	_, _, err := client.Invoke(context.Background(), "enterprise.runtime.status", map[string]any{"runId": "run-two"}, time.Second)
	if err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("saturated Invoke error=%v", err)
	}
	cancelFirst()
	select {
	case err := <-firstResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first Invoke error=%v want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Invoke did not stop after cancellation")
	}
}

func writeGatewayChallenge(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	if err := conn.WriteJSON(map[string]any{"type": "event", "event": "connect.challenge", "payload": map[string]any{"nonce": "nonce-1"}}); err != nil {
		t.Errorf("write challenge: %v", err)
	}
}

func readGatewayConnect(t *testing.T, conn *websocket.Conn) bool {
	t.Helper()
	var connect map[string]any
	if err := conn.ReadJSON(&connect); err != nil {
		t.Errorf("read connect: %v", err)
		return false
	}
	if err := conn.WriteJSON(map[string]any{
		"type": "res", "id": connect["id"], "ok": true,
		"payload": map[string]any{"type": "hello-ok", "protocol": gatewayProtocolVersion},
	}); err != nil {
		t.Errorf("write connect response: %v", err)
		return false
	}
	return true
}
