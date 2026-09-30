// Package api is flowd's JSON REST API.
//
// Routes use the Go 1.22+ ServeMux patterns, such as "POST /v1/workflows".
// Every error is an RFC 7807 application/problem+json body, including the
// 404 and 405 answers for unknown routes. api/openapi.yaml describes the
// same routes, bodies and status codes.
package api

import (
	"log/slog"
	"net/http"

	"github.com/kaustubhagarwal21/flowd/internal/metrics"
	"github.com/kaustubhagarwal21/flowd/internal/store"
)

// Waker is the part of the engine the API needs: a nudge after a run is created.
type Waker interface{ Wake() }

// server holds what the handlers need. It is read-only after New, so the
// handlers can share it across goroutines without locking.
type server struct {
	store   store.Store
	waker   Waker
	metrics *metrics.Metrics
	log     *slog.Logger
}

// New returns the HTTP handler for the whole API, including /healthz and /metrics.
func New(st store.Store, w Waker, m *metrics.Metrics, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	s := &server{store: st, waker: w, metrics: m, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/workflows", s.createWorkflow)
	mux.HandleFunc("GET /v1/workflows/{id}", s.getWorkflow)
	mux.HandleFunc("POST /v1/workflows/{id}/runs", s.createRun)
	mux.HandleFunc("GET /v1/runs", s.listRuns)
	mux.HandleFunc("GET /v1/runs/{id}", s.getRun)
	mux.HandleFunc("POST /v1/runs/{id}/cancel", s.cancelRun)
	mux.HandleFunc("GET /healthz", s.healthz)
	if m != nil {
		// A nil *Metrics would answer with a plain-text 404. Leaving the
		// route out gives the usual problem+json 404 instead.
		mux.Handle("GET /metrics", m.Handler())
	}

	// The access log is outermost so that it records the final status,
	// including the 500 that recoverPanics writes.
	return logRequests(log, recoverPanics(log, problemMux{mux}))
}
