package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
)

// ErrRuntimeSubmitCaptured is returned only by the server-local B4 fixture
// capture sink after it has durably recorded the canonical submit envelope.
// It is never configured by an App, Worker, or Runtime Host service.
var ErrRuntimeSubmitCaptured = errors.New("RUNTIME_SUBMIT_CAPTURED")

// RuntimeSubmitCaptureRecord is the exact canonical request bytes that the
// normal Host transport would otherwise submit. RunTicket remains out of the
// JSON envelope and is carried separately so a root-only fixture sink can
// protect it with a stricter file policy.
type RuntimeSubmitCaptureRecord struct {
	RunID            string
	RunTicket        string
	CanonicalRequest []byte
}

// RuntimeSubmitCaptureSink is intentionally an in-process dependency of the
// server-local B4 fixture CLI. NewRuntimeTransportClient never configures it,
// so it cannot become an App/API or normal Worker capture channel.
type RuntimeSubmitCaptureSink interface {
	CaptureAsyncRuntimeSubmit(context.Context, RuntimeSubmitCaptureRecord) error
}

// NewFixtureSubmitCaptureTransport is the only supported construction point
// for a capture-capable transport. It is intentionally unavailable to normal
// API/Worker construction: the fixture process must be a Linux root process
// in the explicitly isolated prelaunch environment.
func NewFixtureSubmitCaptureTransport(base HTTPTransportOpenClawClient, sink RuntimeSubmitCaptureSink) (HTTPTransportOpenClawClient, error) {
	if sink == nil || runtime.GOOS != "linux" || os.Geteuid() != 0 || strings.TrimSpace(os.Getenv("HUAHUO_ENV")) != "prelaunch" {
		return HTTPTransportOpenClawClient{}, fmt.Errorf("RUNTIME_SUBMIT_CAPTURE_FORBIDDEN")
	}
	base.submitCapture = sink
	return base, nil
}
