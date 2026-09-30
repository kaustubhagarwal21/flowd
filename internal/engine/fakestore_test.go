package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// fakeStore is an in-memory store.Store for engine unit tests: a mutex and
// maps, following the store.Store contract (claim order, fencing on
// owner+attempt, promotion, skip propagation, cancel). It also records every
// write the engine attempts and can inject claim errors.
type fakeStore struct {
	mu        sync.Mutex
	nextID    int
	workflows map[string]store.Workflow
	runs      map[string]*fakeRun
	runOrder  []string // creation order; claims take the oldest run first

	claimErrors int // how many upcoming ClaimStep calls fail
	claimCalls  int
	writes      []fakeWrite // CompleteStep and FailStep calls, fenced out or not
}

type fakeRun struct {
	run   store.Run // Steps unused; see steps
	steps []*fakeStep
}

type fakeStep struct {
	def        workflow.Step
	state      store.StepState
	owner      string
	leaseUntil time.Time
	notBefore  time.Time
}

// fakeWrite records one CompleteStep or FailStep call.
type fakeWrite struct {
	kind    string // "complete" or "fail"
	stepID  string
	attempt int
	errMsg  string
	retryAt *time.Time
	at      time.Time
}

var _ store.Store = (*fakeStore)(nil)

func newFakeStore() *fakeStore {
	return &fakeStore{workflows: map[string]store.Workflow{}, runs: map[string]*fakeRun{}}
}

func (f *fakeStore) id(prefix string) string {
	f.nextID++
	return fmt.Sprintf("%s-%d", prefix, f.nextID)
}

func (f *fakeStore) CreateWorkflow(ctx context.Context, def workflow.Definition) (store.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := store.Workflow{ID: f.id("wf"), Name: def.Name, Definition: def, CreatedAt: time.Now()}
	f.workflows[w.ID] = w
	return w, nil
}

func (f *fakeStore) GetWorkflow(ctx context.Context, id string) (store.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.workflows[id]
	if !ok {
		return store.Workflow{}, store.ErrNotFound
	}
	return w, nil
}

func (f *fakeStore) CreateRun(ctx context.Context, workflowID string, input json.RawMessage) (store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.workflows[workflowID]
	if !ok {
		return store.Run{}, store.ErrNotFound
	}
	r := &fakeRun{run: store.Run{ID: f.id("run"), WorkflowID: w.ID, Status: store.RunRunning, Input: input, CreatedAt: time.Now()}}
	for _, s := range w.Definition.Steps {
		status := store.StepPending
		if len(s.DependsOn) == 0 {
			status = store.StepReady
		}
		r.steps = append(r.steps, &fakeStep{def: s, state: store.StepState{
			StepID: s.ID, Status: status, MaxAttempts: s.Retry.MaxAttempts, DependsOn: s.DependsOn,
		}})
	}
	f.runs[r.run.ID] = r
	f.runOrder = append(f.runOrder, r.run.ID)
	return r.snapshot(), nil
}

func (f *fakeStore) GetRun(ctx context.Context, id string) (store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[id]
	if !ok {
		return store.Run{}, store.ErrNotFound
	}
	return r.snapshot(), nil
}

func (f *fakeStore) ListRuns(ctx context.Context, flt store.ListRunsFilter) ([]store.Run, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	limit := flt.Limit
	if limit == 0 {
		limit = 20
	}
	var out []store.Run
	for _, id := range slices.Backward(f.runOrder) { // newest first
		r := f.runs[id]
		if (flt.WorkflowID == "" || r.run.WorkflowID == flt.WorkflowID) && (flt.Status == "" || r.run.Status == flt.Status) {
			run := r.run
			run.Steps = nil
			out = append(out, run)
		}
		if len(out) == limit {
			break
		}
	}
	return out, "", nil // the fake does not paginate
}

func (f *fakeStore) CancelRun(ctx context.Context, id string) (store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[id]
	if !ok {
		return store.Run{}, store.ErrNotFound
	}
	switch r.run.Status {
	case store.RunCancelled:
		return r.snapshot(), nil
	case store.RunSucceeded, store.RunFailed:
		return store.Run{}, store.ErrConflict
	}
	r.finish(store.RunCancelled)
	for _, s := range r.steps {
		if s.state.Status == store.StepPending || s.state.Status == store.StepReady {
			s.state.Status = store.StepCancelled
		}
	}
	return r.snapshot(), nil
}

func (f *fakeStore) ClaimStep(ctx context.Context, owner string, lease time.Duration) (*store.Claim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimCalls++
	if f.claimErrors > 0 {
		f.claimErrors--
		return nil, errors.New("fake: database unavailable")
	}
	now := time.Now()
	for _, id := range f.runOrder {
		r := f.runs[id]
		for _, s := range r.steps {
			if r.run.Status != store.RunRunning {
				break // only running runs have work (failStep below can end one)
			}
			due := s.state.Status == store.StepReady && !now.Before(s.notBefore)
			expired := s.state.Status == store.StepRunning && now.After(s.leaseUntil)
			if !due && !expired {
				continue
			}
			if expired && s.state.Attempt >= s.state.MaxAttempts {
				r.failStep(s, "lease expired after the last attempt")
				continue
			}
			s.state.Status = store.StepRunning
			s.state.Attempt++
			s.state.StartedAt = &now
			s.owner = owner
			s.leaseUntil = now.Add(lease)
			return &store.Claim{
				RunID: r.run.ID, StepID: s.def.ID, Attempt: s.state.Attempt, Owner: owner,
				LeaseExpiresAt: s.leaseUntil, Step: s.def, Input: r.run.Input,
			}, nil
		}
	}
	return nil, nil
}

// fenced returns the claimed step if c still holds it.
func (f *fakeStore) fenced(c *store.Claim) (*fakeRun, *fakeStep, error) {
	r, ok := f.runs[c.RunID]
	if !ok {
		return nil, nil, store.ErrLeaseLost
	}
	s := r.step(c.StepID)
	if s == nil || s.state.Status != store.StepRunning || s.owner != c.Owner || s.state.Attempt != c.Attempt {
		return nil, nil, store.ErrLeaseLost
	}
	return r, s, nil
}

func (f *fakeStore) Heartbeat(ctx context.Context, c *store.Claim, lease time.Duration) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, s, err := f.fenced(c)
	if err != nil {
		return time.Time{}, err
	}
	if r.run.Status == store.RunCancelled {
		return time.Time{}, store.ErrRunCancelled
	}
	s.leaseUntil = time.Now().Add(lease)
	return s.leaseUntil, nil
}

func (f *fakeStore) CompleteStep(ctx context.Context, c *store.Claim, output json.RawMessage) (store.RunStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, fakeWrite{kind: "complete", stepID: c.StepID, attempt: c.Attempt, at: time.Now()})
	r, s, err := f.fenced(c)
	if err != nil {
		return "", err
	}
	now := time.Now()
	s.state.Status = store.StepSucceeded
	s.state.Output = output
	s.state.FinishedAt = &now
	s.owner = ""
	if r.run.Status != store.RunRunning {
		return r.run.Status, nil
	}
	for _, d := range r.steps {
		if d.state.Status == store.StepPending && r.allSucceeded(d.def.DependsOn) {
			d.state.Status = store.StepReady
		}
	}
	allSucceeded, allTerminal := true, true
	for _, d := range r.steps {
		allSucceeded = allSucceeded && d.state.Status == store.StepSucceeded
		allTerminal = allTerminal && d.state.Status.Terminal()
	}
	if allTerminal {
		if allSucceeded {
			r.finish(store.RunSucceeded)
		} else {
			r.finish(store.RunFailed)
		}
	}
	return r.run.Status, nil
}

func (f *fakeStore) FailStep(ctx context.Context, c *store.Claim, errMsg string, retryAt *time.Time) (store.RunStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, fakeWrite{kind: "fail", stepID: c.StepID, attempt: c.Attempt, errMsg: errMsg, retryAt: retryAt, at: time.Now()})
	r, s, err := f.fenced(c)
	if err != nil {
		return "", err
	}
	s.state.LastError = errMsg
	s.owner = ""
	if retryAt != nil && r.run.Status == store.RunRunning { // a finished run retries nothing
		s.state.Status = store.StepReady
		s.notBefore = *retryAt
		return r.run.Status, nil
	}
	r.failStep(s, errMsg)
	return r.run.Status, nil
}

func (f *fakeStore) Ping(ctx context.Context) error { return nil }
func (f *fakeStore) Close()                         {}

// Test hooks.

// failClaims makes the next n ClaimStep calls return an error.
func (f *fakeStore) failClaims(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimErrors = n
}

// claims returns how many times ClaimStep was called.
func (f *fakeStore) claims() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claimCalls
}

// writeLog returns a copy of the CompleteStep/FailStep calls so far.
func (f *fakeStore) writeLog() []fakeWrite {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.writes)
}

// stealLease re-claims a running step for another owner, as if this
// engine's lease had expired and a second engine had taken the step over.
func (f *fakeStore) stealLease(runID, stepID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.runs[runID].step(stepID)
	s.owner = "another-engine"
	s.state.Attempt++
	s.leaseUntil = time.Now().Add(time.Hour)
}

// Helpers; callers hold f.mu.

func (r *fakeRun) snapshot() store.Run {
	run := r.run
	run.Steps = nil
	for _, s := range r.steps {
		run.Steps = append(run.Steps, s.state)
	}
	return run
}

func (r *fakeRun) step(id string) *fakeStep {
	for _, s := range r.steps {
		if s.def.ID == id {
			return s
		}
	}
	return nil
}

func (r *fakeRun) allSucceeded(ids []string) bool {
	for _, id := range ids {
		if r.step(id).state.Status != store.StepSucceeded {
			return false
		}
	}
	return true
}

func (r *fakeRun) finish(status store.RunStatus) {
	now := time.Now()
	r.run.Status = status
	r.run.FinishedAt = &now
}

// failStep fails s for good, skips everything downstream of it and fails
// the run.
func (r *fakeRun) failStep(s *fakeStep, errMsg string) {
	now := time.Now()
	s.state.Status = store.StepFailed
	s.state.LastError = errMsg
	s.state.FinishedAt = &now
	s.owner = ""
	for changed := true; changed; { // repeat until no new step is skipped
		changed = false
		for _, d := range r.steps {
			if d.state.Status != store.StepPending && d.state.Status != store.StepReady {
				continue
			}
			for _, dep := range d.def.DependsOn {
				if st := r.step(dep).state.Status; st == store.StepFailed || st == store.StepSkipped {
					d.state.Status = store.StepSkipped
					changed = true
					break
				}
			}
		}
	}
	if r.run.Status == store.RunRunning {
		r.finish(store.RunFailed)
	}
}
