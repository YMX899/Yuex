package routes

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"huahuoai/backend/source/internal/persistence"
)

func TestExclusiveAgentRunEventCursorRejectsConflict(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/agent/runs/run_1/events/stream?afterSequence=7", nil)
	request.Header.Set("Last-Event-ID", "8")
	if _, err := exclusiveAgentRunEventCursor(request); err == nil || err.Code != "INVALID_ARGUMENT" {
		t.Fatalf("cursor conflict error = %#v", err)
	}
}

func TestExclusiveAgentRunEventCursorRejectsMultipleLastEventIDs(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/agent/runs/run_1/events/stream", nil)
	request.Header.Add("Last-Event-ID", "7")
	request.Header.Add("Last-Event-ID", "8")
	if _, err := exclusiveAgentRunEventCursor(request); err == nil || err.Code != "INVALID_ARGUMENT" {
		t.Fatalf("multiple cursor error = %#v", err)
	}
}

func TestExclusiveAgentRunEventCursorRejectsEmptyLastEventID(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/agent/runs/run_1/events/stream", nil)
	request.Header.Set("Last-Event-ID", " ")
	if _, err := exclusiveAgentRunEventCursor(request); err == nil || err.Code != "INVALID_ARGUMENT" {
		t.Fatalf("empty cursor error = %#v", err)
	}
}

func TestPublicAgentRunReservedClientContextKeyRejectsRuntimeToolAllowVariants(t *testing.T) {
	for _, key := range []string{"spec.tools.allow", "SPEC_TOOLS_ALLOW", "spec-tools-allow"} {
		if !publicAgentRunReservedClientContextKey(key) {
			t.Fatalf("reserved Runtime tool key %q was accepted", key)
		}
	}
	if publicAgentRunReservedClientContextKey("locale") {
		t.Fatal("ordinary client context metadata must remain allowed")
	}
}

func TestAgentRunEventReplayQueriesAreGloballyBounded(t *testing.T) {
	if capacity := cap(agentRunEventReplaySlots); capacity != 32 {
		t.Fatalf("replay capacity = %d, want 32", capacity)
	}
}

func TestAgentRunSSEWriterHeartbeatUsesPerFlushDeadline(t *testing.T) {
	response := &deadlineResponseWriter{header: http.Header{}}
	writer := newAgentRunSSEWriter(response, 30*time.Second)
	before := time.Now()
	if err := writer.writeHeartbeat(); err != nil {
		t.Fatal(err)
	}
	if response.body.String() != ": heartbeat\n\n" {
		t.Fatalf("heartbeat body = %q", response.body.String())
	}
	if response.deadline.Before(before.Add(29*time.Second)) || response.deadline.After(time.Now().Add(31*time.Second)) {
		t.Fatalf("flush deadline = %s", response.deadline)
	}
	if response.flushes != 1 {
		t.Fatalf("flushes = %d, want 1", response.flushes)
	}
}

func TestAgentRunSSEWriterRejectsMissingWriteDeadlineSupport(t *testing.T) {
	writer := newAgentRunSSEWriter(httptest.NewRecorder(), 30*time.Second)
	started, err := writer.start()
	if started || err == nil {
		t.Fatalf("started=%v err=%v, want fail closed before stream headers", started, err)
	}
}

func TestAgentRunSSEWriterSlowClientIsBoundedByFlushDeadline(t *testing.T) {
	response := &slowDeadlineResponseWriter{header: http.Header{}}
	writer := newAgentRunSSEWriter(response, 20*time.Millisecond)
	started := time.Now()
	err := writer.writeHeartbeat()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("slow client error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 15*time.Millisecond || elapsed > 250*time.Millisecond {
		t.Fatalf("slow client elapsed = %s", elapsed)
	}
}

func TestAgentRunSSEWriterTerminalUsesDurableSequence(t *testing.T) {
	response := &deadlineResponseWriter{header: http.Header{}}
	writer := newAgentRunSSEWriter(response, 30*time.Second)
	terminal := int64(4)
	page := persistence.AgentRunEventPage{
		Items:             []persistence.AgentRunEvent{{Sequence: 4, EventType: "succeeded", Status: "succeeded", SafeData: map[string]any{"status": "succeeded"}}},
		NextAfterSequence: 4,
		LatestSequence:    4,
		TerminalSequence:  &terminal,
	}
	closed, cursor, err := writer.writePage(page, 3)
	if err != nil || !closed || cursor != 4 {
		t.Fatalf("closed=%v cursor=%d err=%v", closed, cursor, err)
	}
	want := "id: 4\nevent: terminal\n"
	if body := response.body.String(); !bytes.Contains([]byte(body), []byte(want)) {
		t.Fatalf("terminal stream missing %q: %s", want, body)
	}
}

func TestAgentRunSSEWriterGapAdvancesLastEventIDToSafeResumeCursor(t *testing.T) {
	response := &deadlineResponseWriter{header: http.Header{}}
	writer := newAgentRunSSEWriter(response, 30*time.Second)
	page := persistence.AgentRunEventPage{Gap: true, OldestAvailableSequence: 3, LatestSequence: 9}
	closed, cursor, err := writer.writePage(page, 1)
	if err != nil || !closed || cursor != 2 {
		t.Fatalf("closed=%v cursor=%d err=%v", closed, cursor, err)
	}
	body := response.body.String()
	for _, expected := range []string{"id: 2\n", "event: gap\n", `"resumeAfterSequence":2`} {
		if !bytes.Contains([]byte(body), []byte(expected)) {
			t.Fatalf("gap stream missing %q: %s", expected, body)
		}
	}
}

func TestAgentRunSSEWriterTerminalCursorClosesWithoutRepeatingTerminal(t *testing.T) {
	response := &deadlineResponseWriter{header: http.Header{}}
	writer := newAgentRunSSEWriter(response, 30*time.Second)
	terminal := int64(4)
	closed, cursor, err := writer.writePage(persistence.AgentRunEventPage{
		LatestSequence: 4, TerminalSequence: &terminal,
	}, terminal)
	if err != nil || !closed || cursor != terminal || response.body.Len() != 0 {
		t.Fatalf("closed=%v cursor=%d body=%q err=%v", closed, cursor, response.body.String(), err)
	}
}

func TestAgentRunSSEWriterDetectsTerminalWhenBoundsLagConcurrentAppend(t *testing.T) {
	response := &deadlineResponseWriter{header: http.Header{}}
	writer := newAgentRunSSEWriter(response, 30*time.Second)
	page := persistence.AgentRunEventPage{
		Items:             []persistence.AgentRunEvent{{Sequence: 9, EventType: "succeeded", Status: "succeeded", SafeData: map[string]any{"status": "succeeded"}}},
		NextAfterSequence: 9,
		LatestSequence:    8,
	}
	closed, cursor, err := writer.writePage(page, 8)
	if err != nil || !closed || cursor != 9 {
		t.Fatalf("closed=%v cursor=%d err=%v", closed, cursor, err)
	}
	if body := response.body.String(); !bytes.Contains([]byte(body), []byte("id: 9\nevent: terminal\n")) {
		t.Fatalf("lagging bounds lost terminal marker: %s", body)
	}
}

type deadlineResponseWriter struct {
	header   http.Header
	body     bytes.Buffer
	status   int
	deadline time.Time
	flushes  int
}

func (w *deadlineResponseWriter) Header() http.Header            { return w.header }
func (w *deadlineResponseWriter) WriteHeader(status int)         { w.status = status }
func (w *deadlineResponseWriter) Write(body []byte) (int, error) { return w.body.Write(body) }
func (w *deadlineResponseWriter) Flush()                         { w.flushes++ }
func (w *deadlineResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

type slowDeadlineResponseWriter struct {
	header   http.Header
	deadline time.Time
}

func (w *slowDeadlineResponseWriter) Header() http.Header { return w.header }
func (w *slowDeadlineResponseWriter) WriteHeader(int)     {}
func (w *slowDeadlineResponseWriter) Flush()              {}
func (w *slowDeadlineResponseWriter) SetWriteDeadline(value time.Time) error {
	w.deadline = value
	return nil
}
func (w *slowDeadlineResponseWriter) Write([]byte) (int, error) {
	if wait := time.Until(w.deadline); wait > 0 {
		time.Sleep(wait)
	}
	return 0, os.ErrDeadlineExceeded
}
