package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// fakeStore is a small in-memory store.Store for handler tests. It follows
// the store contract for the methods the API calls; the engine-side methods
// are never reached from the API and just return an error.
type fakeStore struct {
	mu        sync.Mutex
	seq       int
	workflows map[string]store.Workflow
	runs      []store.Run // in creation order; ListRuns pages newest first
	pingErr   error
	lastList  store.ListRunsFilter // the filter of the last ListRuns call
}

var _ store.Store = (*fakeStore)(nil)

func newFakeStore() *fakeStore {
	return &fakeStore{workflows: map[string]store.Workflow{}}
}

func (f *fakeStore) newID(prefix string) string {
	f.seq++
	return fmt.Sprintf("%s-%d", prefix, f.seq)
}

// addWorkflow stores def directly, bypassing the API and workflow.Validate.
func (f *fakeStore) addWorkflow(def workflow.Definition) store.Workflow {
	wf, _ := f.CreateWorkflow(context.Background(), def)
	return wf
}

// addRun stores a run with the given status and steps, bypassing the API.
func (f *fakeStore) addRun(workflowID string, status store.RunStatus, steps ...store.StepState) store.Run {
	f.mu.Lock()
	defer f.mu.Unlock()
	run := store.Run{
		ID:         f.newID("run"),
		WorkflowID: workflowID,
		Status:     status,
		CreatedAt:  time.Now().UTC(),
		Steps:      steps,
	}
	f.runs = append(f.runs, run)
	return run
}

func (f *fakeStore) workflowCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.workflows)
}

func (f *fakeStore) CreateWorkflow(_ context.Context, def workflow.Definition) (store.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	wf := store.Workflow{ID: f.newID("wf"), Name: def.Name, Definition: def, CreatedAt: time.Now().UTC()}
	f.workflows[wf.ID] = wf
	return wf, nil
}

func (f *fakeStore) GetWorkflow(_ context.Context, id string) (store.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	wf, ok := f.workflows[id]
	if !ok {
		return store.Workflow{}, store.ErrNotFound
	}
	return wf, nil
}

func (f *fakeStore) CreateRun(_ context.Context, workflowID string, input json.RawMessage) (store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.workflows[workflowID]; !ok {
		return store.Run{}, store.ErrNotFound
	}
	run := store.Run{
		ID:         f.newID("run"),
		WorkflowID: workflowID,
		Status:     store.RunRunning,
		Input:      input,
		CreatedAt:  time.Now().UTC(),
	}
	f.runs = append(f.runs, run)
	return run, nil
}

func (f *fakeStore) GetRun(_ context.Context, id string) (store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, run := range f.runs {
		if run.ID == id {
			return run, nil
		}
	}
	return store.Run{}, store.ErrNotFound
}

// ListRuns pages with a decimal offset as the cursor. The real store uses
// keyset pagination; the API treats the cursor as opaque either way.
func (f *fakeStore) ListRuns(_ context.Context, flt store.ListRunsFilter) ([]store.Run, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastList = flt
	offset := 0
	if flt.Cursor != "" {
		n, err := strconv.Atoi(flt.Cursor)
		if err != nil || n < 0 {
			return nil, "", store.ErrInvalidCursor
		}
		offset = n
	}
	limit := flt.Limit
	if limit == 0 {
		limit = 20
	}
	var matched []store.Run
	for i := len(f.runs) - 1; i >= 0; i-- { // newest first
		run := f.runs[i]
		if (flt.WorkflowID == "" || run.WorkflowID == flt.WorkflowID) &&
			(flt.Status == "" || run.Status == flt.Status) {
			run.Steps = nil // ListRuns returns runs without steps
			matched = append(matched, run)
		}
	}
	if offset > len(matched) {
		offset = len(matched)
	}
	page, next := matched[offset:], ""
	if len(page) > limit {
		page, next = page[:limit], strconv.Itoa(offset+limit)
	}
	return page, next, nil
}

func (f *fakeStore) CancelRun(_ context.Context, id string) (store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.runs {
		run := &f.runs[i]
		if run.ID != id {
			continue
		}
		switch run.Status {
		case store.RunRunning:
			now := time.Now().UTC()
			run.Status, run.FinishedAt = store.RunCancelled, &now
		case store.RunCancelled:
			// Cancelling twice returns the run unchanged.
		default:
			return store.Run{}, store.ErrConflict
		}
		return *run, nil
	}
	return store.Run{}, store.ErrNotFound
}

var errNotForAPI = errors.New("fakeStore: the API does not call this")

func (f *fakeStore) ClaimStep(context.Context, string, time.Duration) (*store.Claim, error) {
	return nil, errNotForAPI
}

func (f *fakeStore) Heartbeat(context.Context, *store.Claim, time.Duration) (time.Time, error) {
	return time.Time{}, errNotForAPI
}

func (f *fakeStore) CompleteStep(context.Context, *store.Claim, json.RawMessage) (store.RunStatus, error) {
	return "", errNotForAPI
}

func (f *fakeStore) FailStep(context.Context, *store.Claim, string, *time.Time) (store.RunStatus, error) {
	return "", errNotForAPI
}

func (f *fakeStore) Ping(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pingErr
}

func (f *fakeStore) Close() {}

// panicStore panics in GetRun, to exercise the recovery middleware.
type panicStore struct{ *fakeStore }

func (panicStore) GetRun(context.Context, string) (store.Run, error) { panic("boom") }

// brokenStore fails GetWorkflow with an error that must not reach the client.
type brokenStore struct{ *fakeStore }

func (brokenStore) GetWorkflow(context.Context, string) (store.Workflow, error) {
	return store.Workflow{}, errors.New("dial tcp 10.0.0.7:5432: password authentication failed for user secret_user")
}
