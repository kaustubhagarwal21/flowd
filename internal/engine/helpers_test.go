package engine_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/engine"
	"github.com/kaustubhagarwal21/flowd/internal/executor"
	"github.com/kaustubhagarwal21/flowd/internal/metrics"
	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// testStepType is a step type only these tests use, so a test can plug in an
// executor that blocks, fails or records exactly as it needs.
const testStepType workflow.StepType = "test"

// newStep returns a fully specified step, so it needs no defaults from
// workflow.Validate.
func newStep(id string, typ workflow.StepType, deps ...string) workflow.Step {
	s := workflow.Step{
		ID: id, Type: typ, DependsOn: deps,
		Retry:     workflow.RetryPolicy{MaxAttempts: 3, InitialBackoffMS: 10, MaxBackoffMS: 50},
		TimeoutMS: 10_000,
	}
	if typ == workflow.StepNoop {
		s.Noop = &workflow.NoopSpec{}
	}
	return s
}

func noopStep(id string, deps ...string) workflow.Step {
	return newStep(id, workflow.StepNoop, deps...)
}

func httpStep(id, url string, deps ...string) workflow.Step {
	s := newStep(id, workflow.StepHTTP, deps...)
	s.HTTP = &workflow.HTTPSpec{Method: http.MethodPost, URL: url, Body: json.RawMessage(`{"hello":"world"}`)}
	return s
}

// realExecutors returns the production executors.
func realExecutors() map[workflow.StepType]engine.Executor {
	return map[workflow.StepType]engine.Executor{
		workflow.StepHTTP: executor.NewHTTP(nil),
		workflow.StepNoop: executor.NewNoop(),
	}
}

// funcExecutor adapts a function to engine.Executor.
type funcExecutor func(ctx context.Context, c *store.Claim) (json.RawMessage, error)

func (f funcExecutor) Execute(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
	return f(ctx, c)
}

// createRun stores a workflow made of steps and starts one run of it.
func createRun(t *testing.T, st store.Store, name string, steps ...workflow.Step) store.Run {
	t.Helper()
	ctx := context.Background()
	wf, err := st.CreateWorkflow(ctx, workflow.Definition{Name: name, Steps: steps})
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	run, err := st.CreateRun(ctx, wf.ID, nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return run
}

// logWriter sends engine logs to the test log (shown for failing tests and
// with -v). A log line after the test has ended makes t.Log panic, which
// would expose a goroutine that outlived its engine.
type logWriter struct{ t testing.TB }

func (w logWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// startEngine runs an engine until stop is called or the test ends. stop
// checks that Run returns promptly, which means every worker and heartbeat
// goroutine has exited.
func startEngine(t *testing.T, st store.Store, cfg engine.Config, execs map[workflow.StepType]engine.Executor, m *metrics.Metrics) (e *engine.Engine, stop func()) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(logWriter{t}, nil))
	e = engine.New(cfg, st, execs, m, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run returned %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Errorf("engine did not stop within 30s")
			}
		})
	}
	t.Cleanup(stop)
	return e, stop
}

// waitFor polls cond until it is true, failing the test after timeout.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitRunFinished waits until the run reaches a final status and returns it.
func waitRunFinished(t *testing.T, st store.Store, runID string, timeout time.Duration) store.Run {
	t.Helper()
	var run store.Run
	waitFor(t, timeout, "run "+runID+" to finish", func() bool {
		var err error
		if run, err = st.GetRun(context.Background(), runID); err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		return run.Status.Finished()
	})
	return run
}

// stepOf returns the state of one step of a run fetched with GetRun.
func stepOf(t *testing.T, run store.Run, stepID string) store.StepState {
	t.Helper()
	for _, s := range run.Steps {
		if s.StepID == stepID {
			return s
		}
	}
	t.Fatalf("run %s has no step %q", run.ID, stepID)
	return store.StepState{}
}

// goroutineBaseline records the goroutine count; the returned check fails the
// test if more goroutines than that are still alive shortly afterwards.
func goroutineBaseline(t *testing.T) (check func()) {
	before := runtime.NumGoroutine()
	return func() {
		t.Helper()
		waitFor(t, 2*time.Second, "goroutines to exit (leak?)", func() bool {
			return runtime.NumGoroutine() <= before
		})
	}
}

// attempt is one recorded execution of a step.
type attempt struct {
	runID, stepID string
	n             int
	start, end    time.Time
	err           error
	cause         error // context.Cause(ctx) when the attempt ended; nil if ctx was live
}

// recorder wraps executors and records every attempt, so tests can check
// ordering, overlap, exactly-once execution and why an attempt ended.
type recorder struct {
	mu       sync.Mutex
	started  map[string]int // step ID -> attempts started
	attempts []attempt      // finished attempts
}

func newRecorder() *recorder { return &recorder{started: map[string]int{}} }

func (r *recorder) wrap(execs map[workflow.StepType]engine.Executor) map[workflow.StepType]engine.Executor {
	out := make(map[workflow.StepType]engine.Executor, len(execs))
	for typ, ex := range execs {
		out[typ] = funcExecutor(func(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
			start := time.Now()
			r.mu.Lock()
			r.started[c.StepID]++
			r.mu.Unlock()

			res, err := ex.Execute(ctx, c)

			r.mu.Lock()
			r.attempts = append(r.attempts, attempt{
				runID: c.RunID, stepID: c.StepID, n: c.Attempt,
				start: start, end: time.Now(), err: err, cause: context.Cause(ctx),
			})
			r.mu.Unlock()
			return res, err
		})
	}
	return out
}

// startedCount returns how many attempts of stepID have started.
func (r *recorder) startedCount(stepID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started[stepID]
}

// finished returns the finished attempts of stepID, in the order they ended.
func (r *recorder) finished(stepID string) []attempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []attempt
	for _, a := range r.attempts {
		if a.stepID == stepID {
			out = append(out, a)
		}
	}
	return out
}

// all returns every finished attempt.
func (r *recorder) all() []attempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]attempt(nil), r.attempts...)
}

// only returns the single finished attempt of stepID, failing otherwise.
func (r *recorder) only(t *testing.T, stepID string) attempt {
	t.Helper()
	got := r.finished(stepID)
	if len(got) != 1 {
		t.Fatalf("step %q: %d finished attempts, want 1", stepID, len(got))
	}
	return got[0]
}

// hasMetric reports whether m exposes a sample line that starts with prefix,
// e.g. `flowd_claim_errors_total 3`. Labels appear sorted by name.
func hasMetric(m *metrics.Metrics, prefix string) bool {
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for line := range strings.Lines(rec.Body.String()) {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

// wantMetric fails the test unless m exposes exactly this sample line.
func wantMetric(t *testing.T, m *metrics.Metrics, sample string) {
	t.Helper()
	if !hasMetric(m, sample+"\n") {
		t.Errorf("metric sample %q not found", sample)
	}
}
