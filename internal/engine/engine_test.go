package engine_test

// Unit tests of the engine's mechanics against the in-memory fakeStore.
// End-to-end behaviour against PostgreSQL is in engine_pg_test.go.

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/engine"
	"github.com/kaustubhagarwal21/flowd/internal/executor"
	"github.com/kaustubhagarwal21/flowd/internal/metrics"
	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// fast polls often, so tests do not wait on idle workers.
const fast = 5 * time.Millisecond

// blockingExecutor runs until its context ends. It reports each start on
// started and each context cause on causes; then it claims success, so a
// test can check that the engine discards a result that arrives too late.
func blockingExecutor(started chan<- struct{}, causes chan<- error) engine.Executor {
	return funcExecutor(func(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
		started <- struct{}{}
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return json.RawMessage(`{"late":true}`), nil
	})
}

// receive waits for a value on ch, failing the test after timeout.
func receive[T any](t *testing.T, ch <-chan T, timeout time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("timed out after %s waiting for %s", timeout, what)
		var zero T
		return zero
	}
}

func TestRetryWithBackoffThenSuccess(t *testing.T) {
	st := newFakeStore()
	flaky := noopStep("flaky")
	flaky.Noop.FailTimes = 2
	flaky.Retry = workflow.RetryPolicy{MaxAttempts: 3, InitialBackoffMS: 20, MaxBackoffMS: 1000}
	run := createRun(t, st, "retry", flaky)
	m := metrics.New()
	_, stop := startEngine(t, st, engine.Config{Workers: 2, Poll: fast}, realExecutors(), m)

	run = waitRunFinished(t, st, run.ID, 5*time.Second)
	// A worker updates the metrics just after its store write, so wait for
	// the workers to exit before reading them.
	stop()
	if run.Status != store.RunSucceeded {
		t.Fatalf("run status = %s, want succeeded", run.Status)
	}
	if got := stepOf(t, run, "flaky").Attempt; got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	writes := st.writeLog()
	if len(writes) != 3 || writes[2].kind != "complete" {
		t.Fatalf("writes = %+v, want fail, fail, complete", writes)
	}
	for i, w := range writes[:2] {
		if w.kind != "fail" || w.retryAt == nil {
			t.Fatalf("write %d = %+v, want a fail with a retry time", i, w)
		}
		// Full jitter: the delay is in [0, 20ms * 2^(attempt-1)]. The engine
		// read its clock just before the fake recorded w.at, hence the slack
		// below zero.
		ceiling := 20 * time.Millisecond << i
		if d := w.retryAt.Sub(w.at); d > ceiling || d < -100*time.Millisecond {
			t.Errorf("attempt %d: retry in %v, want within [0, %v]", w.attempt, d, ceiling)
		}
	}
	wantMetric(t, m, `flowd_steps_executed_total{result="retry",type="noop"} 2`)
	wantMetric(t, m, `flowd_steps_executed_total{result="success",type="noop"} 1`)
	wantMetric(t, m, `flowd_runs_finished_total{status="succeeded"} 1`)
}

func TestPermanentErrorIsNotRetried(t *testing.T) {
	st := newFakeStore()
	run := createRun(t, st, "permanent", newStep("a", testStepType), noopStep("b", "a"), noopStep("c", "b"))
	execs := realExecutors()
	execs[testStepType] = funcExecutor(func(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
		return nil, engine.Permanent(errors.New("target said 400"))
	})
	m := metrics.New()
	_, stop := startEngine(t, st, engine.Config{Workers: 1, Poll: fast}, execs, m)

	run = waitRunFinished(t, st, run.ID, 5*time.Second)
	stop() // so every metric has been recorded
	if run.Status != store.RunFailed {
		t.Fatalf("run status = %s, want failed", run.Status)
	}
	a := stepOf(t, run, "a")
	if a.Status != store.StepFailed || a.Attempt != 1 || a.LastError != "target said 400" {
		t.Errorf("a = %+v, want failed on attempt 1 (max 3) with the error", a)
	}
	for _, id := range []string{"b", "c"} {
		if s := stepOf(t, run, id); s.Status != store.StepSkipped {
			t.Errorf("%s = %s, want skipped", id, s.Status)
		}
	}
	for _, w := range st.writeLog() {
		if w.stepID == "a" && w.retryAt != nil {
			t.Errorf("a was scheduled for a retry: %+v", w)
		}
	}
	wantMetric(t, m, `flowd_steps_executed_total{result="failure",type="test"} 1`)
	wantMetric(t, m, `flowd_runs_finished_total{status="failed"} 1`)
}

// A step that fails after its run has already failed is not retried, and the
// run is counted as finished only once.
func TestStepFailingInFailedRunIsNotRetried(t *testing.T) {
	st := newFakeStore()
	slowStarted, release := make(chan struct{}, 1), make(chan struct{})
	execs := map[workflow.StepType]engine.Executor{
		testStepType: funcExecutor(func(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
			if c.StepID == "bad" {
				<-slowStarted // fail the run while "slow" is still running
				return nil, engine.Permanent(errors.New("broken"))
			}
			slowStarted <- struct{}{}
			<-release
			return nil, errors.New("temporary trouble") // retryable, attempts left
		}),
	}
	run := createRun(t, st, "late-failure", newStep("slow", testStepType), newStep("bad", testStepType))
	m := metrics.New()
	_, stop := startEngine(t, st, engine.Config{Workers: 2, Poll: fast}, execs, m)

	if run = waitRunFinished(t, st, run.ID, 5*time.Second); run.Status != store.RunFailed {
		t.Fatalf("run status = %s, want failed", run.Status)
	}
	close(release)
	waitFor(t, 5*time.Second, "slow to fail", func() bool {
		r, err := st.GetRun(context.Background(), run.ID)
		return err == nil && stepOf(t, r, "slow").Status == store.StepFailed
	})
	stop() // so every metric has been recorded
	wantMetric(t, m, `flowd_steps_executed_total{result="failure",type="test"} 2`)
	wantMetric(t, m, `flowd_runs_finished_total{status="failed"} 1`)
	if hasMetric(m, `flowd_steps_executed_total{result="retry"`) {
		t.Errorf("a step of a failed run was counted as a retry")
	}
}

func TestLastAttemptFailsForGood(t *testing.T) {
	st := newFakeStore()
	s := noopStep("a")
	s.Noop.FailTimes = 5
	s.Retry.MaxAttempts = 2
	run := createRun(t, st, "exhausted", s)
	startEngine(t, st, engine.Config{Workers: 1, Poll: fast}, realExecutors(), nil)

	run = waitRunFinished(t, st, run.ID, 5*time.Second)
	if a := stepOf(t, run, "a"); run.Status != store.RunFailed || a.Status != store.StepFailed || a.Attempt != 2 {
		t.Fatalf("run %s, step %+v; want run failed, step failed after 2 attempts", run.Status, a)
	}
	w := st.writeLog()
	if len(w) != 2 || w[0].retryAt == nil || w[1].retryAt != nil {
		t.Errorf("writes = %+v, want a retry then a final failure", w)
	}
}

func TestTimeoutErrorNamesTheDeadline(t *testing.T) {
	st := newFakeStore()
	s := newStep("slow", testStepType)
	s.TimeoutMS = 30
	s.Retry.MaxAttempts = 1
	run := createRun(t, st, "timeout", s)
	execs := map[workflow.StepType]engine.Executor{
		testStepType: funcExecutor(func(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
			<-ctx.Done() // a step that respects its deadline
			return nil, ctx.Err()
		}),
	}
	startEngine(t, st, engine.Config{Workers: 1, Poll: fast}, execs, nil)

	run = waitRunFinished(t, st, run.ID, 5*time.Second)
	got := stepOf(t, run, "slow")
	if run.Status != store.RunFailed || got.Status != store.StepFailed {
		t.Fatalf("run %s, step %s; want both failed", run.Status, got.Status)
	}
	if !strings.Contains(got.LastError, "timed out after 30ms") {
		t.Errorf("last error = %q, want it to mention the timeout", got.LastError)
	}
}

func TestUnknownStepTypeFailsPermanently(t *testing.T) {
	st := newFakeStore()
	run := createRun(t, st, "unknown", newStep("a", "mystery"))
	startEngine(t, st, engine.Config{Workers: 1, Poll: fast}, realExecutors(), nil)

	run = waitRunFinished(t, st, run.ID, 5*time.Second)
	a := stepOf(t, run, "a")
	if a.Status != store.StepFailed || a.Attempt != 1 || !strings.Contains(a.LastError, `no executor for step type "mystery"`) {
		t.Errorf("a = %+v, want failed at once for its unknown type", a)
	}
}

// When the heartbeat learns that the lease is gone or the run was
// cancelled, the step's context must be cancelled with that reason, and the
// engine must not write the step's result.
func TestHeartbeatStopsAbandonedStep(t *testing.T) {
	tests := []struct {
		name       string
		interrupt  func(t *testing.T, st *fakeStore, runID string)
		wantCause  error
		wantResult string
	}{
		{
			name:       "lease lost",
			interrupt:  func(t *testing.T, st *fakeStore, runID string) { st.stealLease(runID, "a") },
			wantCause:  store.ErrLeaseLost,
			wantResult: "lost",
		},
		{
			name: "run cancelled",
			interrupt: func(t *testing.T, st *fakeStore, runID string) {
				if _, err := st.CancelRun(context.Background(), runID); err != nil {
					t.Fatal(err)
				}
			},
			wantCause:  store.ErrRunCancelled,
			wantResult: "cancelled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newFakeStore()
			run := createRun(t, st, "abandon", newStep("a", testStepType), noopStep("b", "a"))
			started, causes := make(chan struct{}, 1), make(chan error, 1)
			execs := map[workflow.StepType]engine.Executor{testStepType: blockingExecutor(started, causes)}
			m := metrics.New()
			// Lease 90ms: a heartbeat every 30ms.
			_, stop := startEngine(t, st, engine.Config{Workers: 1, Lease: 90 * time.Millisecond, Poll: fast}, execs, m)

			receive(t, started, 5*time.Second, "the step to start")
			tt.interrupt(t, st, run.ID)
			if cause := receive(t, causes, 5*time.Second, "the step to be cancelled"); !errors.Is(cause, tt.wantCause) {
				t.Fatalf("step context cause = %v, want %v", cause, tt.wantCause)
			}
			stop()
			if w := st.writeLog(); len(w) != 0 {
				t.Errorf("engine wrote %+v for an abandoned step, want no writes", w)
			}
			wantMetric(t, m, `flowd_steps_executed_total{result="`+tt.wantResult+`",type="test"} 1`)
		})
	}
}

func TestShutdownLetsInFlightStepsFinish(t *testing.T) {
	st := newFakeStore()
	run := createRun(t, st, "drain", newStep("a", testStepType), newStep("b", testStepType, "a"))
	started := make(chan struct{}, 2)
	execs := map[workflow.StepType]engine.Executor{
		testStepType: funcExecutor(func(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
			started <- struct{}{}
			select {
			case <-time.After(200 * time.Millisecond):
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}),
	}
	leaks := goroutineBaseline(t)
	_, stop := startEngine(t, st, engine.Config{Workers: 2, Poll: fast, ShutdownGrace: 10 * time.Second}, execs, nil)

	receive(t, started, 5*time.Second, "step a to start")
	stop() // returns only once a has finished
	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if a := stepOf(t, got, "a"); a.Status != store.StepSucceeded {
		t.Errorf("a = %s, want succeeded: the in-flight step should finish during shutdown", a.Status)
	}
	// Completing a made b ready, but a stopping engine claims nothing new.
	if b := stepOf(t, got, "b"); b.Status != store.StepReady {
		t.Errorf("b = %s, want ready (never claimed)", b.Status)
	}
	leaks()
}

func TestShutdownCancelsStepsAfterGrace(t *testing.T) {
	st := newFakeStore()
	run := createRun(t, st, "grace", newStep("a", testStepType))
	started, causes := make(chan struct{}, 1), make(chan error, 1)
	execs := map[workflow.StepType]engine.Executor{testStepType: blockingExecutor(started, causes)}
	m := metrics.New()
	leaks := goroutineBaseline(t)
	_, stop := startEngine(t, st, engine.Config{Workers: 1, Poll: fast, ShutdownGrace: 100 * time.Millisecond}, execs, m)

	receive(t, started, 5*time.Second, "step a to start")
	begin := time.Now()
	stop()
	if took := time.Since(begin); took < 100*time.Millisecond || took > 5*time.Second {
		t.Errorf("shutdown took %v, want about the 100ms grace period", took)
	}
	if cause := receive(t, causes, time.Second, "the step to be cancelled"); cause == nil || errors.Is(cause, store.ErrLeaseLost) {
		t.Errorf("step context cause = %v, want the shutdown", cause)
	}
	// The result is discarded and the lease is left to expire.
	if w := st.writeLog(); len(w) != 0 {
		t.Errorf("engine wrote %+v after the grace period, want no writes", w)
	}
	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if a := stepOf(t, got, "a"); a.Status != store.StepRunning {
		t.Errorf("a = %s, want still running (its lease will expire)", a.Status)
	}
	wantMetric(t, m, `flowd_steps_executed_total{result="cancelled",type="test"} 1`)
	leaks()
}

func TestWakeShortensIdleWait(t *testing.T) {
	st := newFakeStore()
	// A 5s poll (4-6s with jitter) is far longer than this test waits.
	e, _ := startEngine(t, st, engine.Config{Workers: 1, Poll: 5 * time.Second}, realExecutors(), nil)
	waitFor(t, 5*time.Second, "the first (empty) claim", func() bool { return st.claims() >= 1 })

	run := createRun(t, st, "wake", noopStep("a"))
	time.Sleep(100 * time.Millisecond)
	if n := st.claims(); n != 1 {
		t.Fatalf("worker claimed %d times, want 1: it should be idle", n)
	}
	e.Wake()
	run = waitRunFinished(t, st, run.ID, 2*time.Second)
	if run.Status != store.RunSucceeded {
		t.Errorf("run status = %s, want succeeded", run.Status)
	}
}

func TestClaimErrorsAreCountedAndBackedOff(t *testing.T) {
	st := newFakeStore()
	st.failClaims(3)
	run := createRun(t, st, "claim-errors", noopStep("a"))
	m := metrics.New()
	startEngine(t, st, engine.Config{Workers: 1, Poll: 10 * time.Millisecond}, realExecutors(), m)

	if run = waitRunFinished(t, st, run.ID, 5*time.Second); run.Status != store.RunSucceeded {
		t.Fatalf("run status = %s, want succeeded once the store recovers", run.Status)
	}
	wantMetric(t, m, "flowd_claim_errors_total 3")
}

func TestDefaultOwnerIsUniquePerEngine(t *testing.T) {
	st := newFakeStore()
	owners := make(chan string, 2)
	execs := map[workflow.StepType]engine.Executor{
		testStepType: funcExecutor(func(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
			owners <- c.Owner
			return nil, nil
		}),
	}
	var got []string
	for range 2 {
		run := createRun(t, st, "owner", newStep("a", testStepType))
		_, stop := startEngine(t, st, engine.Config{Workers: 1, Poll: fast}, execs, nil)
		waitRunFinished(t, st, run.ID, 5*time.Second)
		stop()
		got = append(got, receive(t, owners, time.Second, "the owner"))
	}
	pattern := regexp.MustCompile(`^.+-\d+-[0-9a-f]{8}$`) // hostname-pid-random
	for _, o := range got {
		if !pattern.MatchString(o) {
			t.Errorf("owner %q does not look like hostname-pid-random", o)
		}
	}
	if got[0] == got[1] {
		t.Errorf("two engines in one process share the owner %q", got[0])
	}
}

// Compile-time check that the executors satisfy the engine's interface.
var _ = []engine.Executor{executor.NewHTTP(nil), executor.NewNoop()}
