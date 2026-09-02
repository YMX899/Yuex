package runtime

import (
	"context"
	"os"
	"runtime"
	"testing"
)

type testRuntimeSubmitCaptureSink struct{}

func (testRuntimeSubmitCaptureSink) CaptureAsyncRuntimeSubmit(context.Context, RuntimeSubmitCaptureRecord) error {
	return ErrRuntimeSubmitCaptured
}

func TestFixtureSubmitCaptureTransportRejectsUnprivilegedProcess(t *testing.T) {
	if runtime.GOOS == "linux" && os.Geteuid() == 0 && os.Getenv("HUAHUO_ENV") == "prelaunch" {
		t.Skip("current process is the privileged fixture boundary")
	}
	if _, err := NewFixtureSubmitCaptureTransport(HTTPTransportOpenClawClient{}, testRuntimeSubmitCaptureSink{}); err == nil || err.Error() != "RUNTIME_SUBMIT_CAPTURE_FORBIDDEN" {
		t.Fatalf("unprivileged capture constructor error=%v, want RUNTIME_SUBMIT_CAPTURE_FORBIDDEN", err)
	}
}

func TestFixtureSubmitCaptureTransportRejectsNilSink(t *testing.T) {
	if _, err := NewFixtureSubmitCaptureTransport(HTTPTransportOpenClawClient{}, nil); err == nil || err.Error() != "RUNTIME_SUBMIT_CAPTURE_FORBIDDEN" {
		t.Fatalf("nil sink error=%v, want RUNTIME_SUBMIT_CAPTURE_FORBIDDEN", err)
	}
}
