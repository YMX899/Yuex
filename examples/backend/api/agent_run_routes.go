package api

import "net/http"

// RegisterAgentRunRoutes exposes product-facing endpoints. A frontend talks
// to these handlers, never directly to a Runtime Host or Agent Harness.
func RegisterAgentRunRoutes(router Router, handlers AgentRunHandlers) {
	router.Handle(http.MethodPost, "/api/v1/agent/runs", handlers.Create)
	router.Handle(http.MethodGet, "/api/v1/agent/runs/{runId}", handlers.Status)
	router.Handle(http.MethodGet, "/api/v1/agent/runs/{runId}/events", handlers.Events)
	router.Handle(http.MethodPost, "/api/v1/agent/runs/{runId}/cancel", handlers.Cancel)
}

// Create performs these steps in order:
//  1. derive tenantId and userId from the authenticated request;
//  2. authorize workspaceId, threadId and every attachment;
//  3. persist the user's message;
//  4. call ProductAgentRunBridge.Create with an idempotency key;
//  5. return 202 with runId and status.
//
// Status and Events repeat the ownership check before reading. Events accept
// an opaque cursor so reconnecting clients do not replay the entire stream.
// Cancel records intent durably; a worker performs the fenced Runtime abort.
type AgentRunHandlers interface {
	Create(http.ResponseWriter, *http.Request)
	Status(http.ResponseWriter, *http.Request)
	Events(http.ResponseWriter, *http.Request)
	Cancel(http.ResponseWriter, *http.Request)
}

type Router interface {
	Handle(method, path string, handler http.HandlerFunc)
}
