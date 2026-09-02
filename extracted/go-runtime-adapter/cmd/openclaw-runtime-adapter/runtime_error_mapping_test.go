package main

import (
	"errors"
	"net/http"
	"testing"
)

func TestAdapterRuntimeErrorCodePreservesRuntimeRunNotFound(t *testing.T) {
	code := adapterRuntimeErrorCode(errors.New("UNAVAILABLE Error: run not found: code=RUNTIME_RUN_NOT_FOUND"), nil)
	if code != "RUNTIME_RUN_NOT_FOUND" {
		t.Fatalf("error code = %q, want RUNTIME_RUN_NOT_FOUND", code)
	}
	if status := adapterRuntimeHTTPStatus(code); status != http.StatusNotFound {
		t.Fatalf("HTTP status = %d, want %d", status, http.StatusNotFound)
	}
}
