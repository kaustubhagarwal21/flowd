package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// maxPageSize is the largest limit GET /v1/runs accepts; it matches the
// 1..100 range of store.ListRunsFilter.
const maxPageSize = 100

// pingTimeout bounds /healthz, so a hung database fails the probe quickly
// instead of leaving the prober waiting.
const pingTimeout = 2 * time.Second

func (s *server) createWorkflow(w http.ResponseWriter, r *http.Request) {
	var def workflow.Definition
	if !readJSON(w, r, &def) {
		return
	}
	// Validate also fills in the defaults, so the stored copy is exactly
	// what will run.
	if _, err := workflow.Validate(&def); err != nil {
		p := newProblem(r, http.StatusBadRequest, err.Error())
		var ve *workflow.ValidationError
		if errors.As(err, &ve) {
			p.Detail, p.Field = ve.Message, ve.Field
		}
		p.write(w)
		return
	}
	wf, err := s.store.CreateWorkflow(r.Context(), def)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/workflows/"+url.PathEscape(wf.ID))
	s.writeJSON(w, r, http.StatusCreated, wf)
}

func (s *server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	wf, err := s.store.GetWorkflow(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeProblem(w, r, http.StatusNotFound, fmt.Sprintf("workflow %q not found", id))
	case err != nil:
		s.internalError(w, r, err)
	default:
		s.writeJSON(w, r, http.StatusOK, wf)
	}
}

// createRunRequest is the optional body of POST /v1/workflows/{id}/runs.
type createRunRequest struct {
	Input json.RawMessage `json:"input"`
}

// createRunResponse is the 202 body. 202, not 201, because the run has only
// been queued: the engine executes it in the background.
type createRunResponse struct {
	RunID string `json:"run_id"`
}

func (s *server) createRun(w http.ResponseWriter, r *http.Request) {
	var req createRunRequest
	// The body is optional because a run does not need input.
	if r.ContentLength != 0 && !readJSON(w, r, &req) {
		return
	}
	input := req.Input
	if bytes.Equal(input, []byte("null")) {
		input = nil // "input": null means the same as no input
	}
	if len(input) > 0 && input[0] != '{' {
		writeProblem(w, r, http.StatusBadRequest, `"input" must be a JSON object`)
		return
	}

	id := r.PathValue("id")
	run, err := s.store.CreateRun(r.Context(), id, input)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeProblem(w, r, http.StatusNotFound, fmt.Sprintf("workflow %q not found", id))
		return
	case err != nil:
		s.internalError(w, r, err)
		return
	}
	s.metrics.RunStarted()
	if s.waker != nil {
		// Start the first steps now rather than at the next poll.
		s.waker.Wake()
	}
	w.Header().Set("Location", "/v1/runs/"+url.PathEscape(run.ID))
	s.writeJSON(w, r, http.StatusAccepted, createRunResponse{RunID: run.ID})
}

func (s *server) getRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.store.GetRun(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeProblem(w, r, http.StatusNotFound, fmt.Sprintf("run %q not found", id))
	case err != nil:
		s.internalError(w, r, err)
	default:
		s.writeJSON(w, r, http.StatusOK, run)
	}
}

// runList is one page of GET /v1/runs. NextCursor is left out on the last page.
type runList struct {
	Runs       []store.Run `json:"runs"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

func (s *server) listRuns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.ListRunsFilter{WorkflowID: q.Get("workflow_id"), Cursor: q.Get("cursor")}
	if v := q.Get("status"); v != "" {
		switch st := store.RunStatus(v); st {
		case store.RunRunning, store.RunSucceeded, store.RunFailed, store.RunCancelled:
			f.Status = st
		default:
			writeProblem(w, r, http.StatusBadRequest, "status must be one of running, succeeded, failed, cancelled")
			return
		}
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxPageSize {
			writeProblem(w, r, http.StatusBadRequest, fmt.Sprintf("limit must be an integer from 1 to %d", maxPageSize))
			return
		}
		f.Limit = n
	}

	runs, next, err := s.store.ListRuns(r.Context(), f)
	switch {
	case errors.Is(err, store.ErrInvalidCursor):
		// The cursor is opaque to the API; only the store can tell it is bad.
		writeProblem(w, r, http.StatusBadRequest, "cursor is invalid; pass next_cursor from the previous page unchanged")
		return
	case err != nil:
		s.internalError(w, r, err)
		return
	}
	if runs == nil {
		runs = []store.Run{} // an empty page is [], not null
	}
	s.writeJSON(w, r, http.StatusOK, runList{Runs: runs, NextCursor: next})
}

func (s *server) cancelRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.store.CancelRun(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeProblem(w, r, http.StatusNotFound, fmt.Sprintf("run %q not found", id))
	case errors.Is(err, store.ErrConflict):
		writeProblem(w, r, http.StatusConflict, fmt.Sprintf("run %q has already finished", id))
	case err != nil:
		s.internalError(w, r, err)
	default:
		s.writeJSON(w, r, http.StatusOK, run)
	}
}

func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), pingTimeout)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		// The error may name hosts or users, so it goes to the log only.
		s.log.Warn("health check failed", "err", err)
		writeProblem(w, r, http.StatusServiceUnavailable, "database is unreachable")
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
}

// internalError logs err and sends a generic 500. The error text stays in
// the log because it may contain SQL or connection details.
func (s *server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeProblem(w, r, http.StatusInternalServerError, "the server could not complete the request")
}
