package engine_test

// Behaviour tests of the whole engine: real executors, real store. Each test
// runs twice: against the in-memory fakeStore, and against PostgreSQL through
// pgstore (the "postgres" subtests skip when FLOWD_TEST_DATABASE_URL is unset).

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/engine"
	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore"
	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore/pgtest"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// validatingStore validates definitions before storing them, as the API
// does: pgstore expects a validated definition with its defaults filled in.
type validatingStore struct{ store.Store }

func (v validatingStore) CreateWorkflow(ctx context.Context, def workflow.Definition) (store.Workflow, error) {
	if _, err := workflow.Validate(&def); err != nil {
		return store.Workflow{}, err
	}
	return v.Store.CreateWorkflow(ctx, def)
}

// storeCase opens a fresh, empty store. reopen returns another handle on the
// same data, like a second flowd process sharing the database.
type storeCase struct {
	name string
	open func(t *testing.T) (st store.Store, reopen func() store.Store)
}

var storeCases = []storeCase{
	{"fake", func(t *testing.T) (store.Store, func() store.Store) {
		f := newFakeStore()
		return f, func() store.Store { return f }
	}},
	{"postgres", func(t *testing.T) (store.Store, func() store.Store) {
		url := pgtest.NewSchemaURL(t) // skips without FLOWD_TEST_DATABASE_URL
		reopen := func() store.Store {
			st, err := pgstore.Open(context.Background(), url)
			if err != nil {
				t.Fatalf("pgstore.Open: %v", err)
			}
			t.Cleanup(st.Close)
			return validatingStore{st}
		}
		return reopen(), reopen
	}},
}

// forEachStore runs test once per store case, as a subtest.
func forEachStore(t *testing.T, test func(t *testing.T, st store.Store, reopen func() store.Store)) {
	for _, sc := range storeCases {
		t.Run(sc.name, func(t *testing.T) {
			st, reopen := sc.open(t)
			test(t, st, reopen)
		})
	}
}

func TestLinearChainRunsInOrder(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store, _ func() store.Store) {
		rec := newRecorder()
		run := createRun(t, st, "linear-chain", noopStep("a"), noopStep("b", "a"), noopStep("c", "b"), noopStep("d", "c"))
		startEngine(t, st, engine.Config{Workers: 4, Poll: fast}, rec.wrap(realExecutors()), nil)

		if run = waitRunFinished(t, st, run.ID, 20*time.Second); run.Status != store.RunSucceeded {
			t.Fatalf("run status = %s, want succeeded", run.Status)
		}
		got := rec.all()
		if len(got) != 4 {
			t.Fatalf("%d attempts, want 4", len(got))
		}
		for i, id := range []string{"a", "b", "c", "d"} {
			if got[i].stepID != id {
				t.Fatalf("attempt %d ran %q, want %q", i+1, got[i].stepID, id)
			}
			if i > 0 && got[i].start.Before(got[i-1].end) {
				t.Errorf("%s started before %s finished", id, got[i-1].stepID)
			}
		}
	})
}

func TestDiamondRunsMiddleStepsConcurrently(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store, _ func() store.Store) {
		rec := newRecorder()
		b, c := noopStep("b", "a"), noopStep("c", "a")
		b.Noop.SleepMS, c.Noop.SleepMS = 300, 300
		run := createRun(t, st, "diamond", noopStep("a"), b, c, noopStep("d", "b", "c"))
		startEngine(t, st, engine.Config{Workers: 4, Poll: fast}, rec.wrap(realExecutors()), nil)

		if run = waitRunFinished(t, st, run.ID, 20*time.Second); run.Status != store.RunSucceeded {
			t.Fatalf("run status = %s, want succeeded", run.Status)
		}
		ra, rb, rc, rd := rec.only(t, "a"), rec.only(t, "b"), rec.only(t, "c"), rec.only(t, "d")
		since := func(x time.Time) time.Duration { return x.Sub(ra.start).Round(time.Millisecond) }
		// Two intervals overlap when each starts before the other ends.
		if !rb.start.Before(rc.end) || !rc.start.Before(rb.end) {
			t.Errorf("b ran [%v, %v] and c ran [%v, %v]: no overlap", since(rb.start), since(rb.end), since(rc.start), since(rc.end))
		}
		if rb.start.Before(ra.end) || rc.start.Before(ra.end) {
			t.Errorf("a middle step started before a finished")
		}
		if rd.start.Before(rb.end) || rd.start.Before(rc.end) {
			t.Errorf("d started before both middle steps finished")
		}
	})
}

func TestFlakyStepSucceedsOnThirdAttempt(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store, _ func() store.Store) {
		flaky := noopStep("flaky")
		flaky.Noop.FailTimes = 2
		flaky.Retry = workflow.RetryPolicy{MaxAttempts: 3, InitialBackoffMS: 10, MaxBackoffMS: 50}
		run := createRun(t, st, "flaky", flaky)
		startEngine(t, st, engine.Config{Workers: 2, Poll: fast}, realExecutors(), nil)

		run = waitRunFinished(t, st, run.ID, 20*time.Second)
		if s := stepOf(t, run, "flaky"); run.Status != store.RunSucceeded || s.Status != store.StepSucceeded || s.Attempt != 3 {
			t.Errorf("run %s, step %s on attempt %d; want both succeeded on attempt 3", run.Status, s.Status, s.Attempt)
		}
	})
}

func TestPermanentFailureSkipsDependentsAndFailsRun(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store, _ func() store.Store) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			http.Error(w, `{"error":"bad input"}`, http.StatusBadRequest)
		}))
		t.Cleanup(srv.Close)
		run := createRun(t, st, "permanent", httpStep("a", srv.URL), noopStep("b", "a"), noopStep("c", "b"))
		startEngine(t, st, engine.Config{Workers: 2, Poll: fast}, realExecutors(), nil)

		if run = waitRunFinished(t, st, run.ID, 20*time.Second); run.Status != store.RunFailed {
			t.Fatalf("run status = %s, want failed", run.Status)
		}
		a := stepOf(t, run, "a")
		if a.Status != store.StepFailed || a.Attempt != 1 || !strings.Contains(a.LastError, "400") {
			t.Errorf("a = %+v, want failed on its first attempt with the 400 in last_error", a)
		}
		for _, id := range []string{"b", "c"} {
			if s := stepOf(t, run, id); s.Status != store.StepSkipped {
				t.Errorf("%s = %s, want skipped", id, s.Status)
			}
		}
		if n := calls.Load(); n != 1 {
			t.Errorf("target called %d times, want 1 (a 400 is not retried)", n)
		}
	})
}

// A binary response (with NUL bytes, which jsonb cannot hold) must still be
// saved as the step output; otherwise the step would be re-run forever.
func TestBinaryResponseIsSaved(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store, _ func() store.Store) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("GIF\x00\x01 image"))
		}))
		t.Cleanup(srv.Close)
		run := createRun(t, st, "binary", httpStep("fetch", srv.URL))
		startEngine(t, st, engine.Config{Workers: 1, Poll: fast}, realExecutors(), nil)

		run = waitRunFinished(t, st, run.ID, 20*time.Second)
		s := stepOf(t, run, "fetch")
		if run.Status != store.RunSucceeded {
			t.Fatalf("run %s, step %s: %s; want succeeded", run.Status, s.Status, s.LastError)
		}
		var out struct {
			Status int    `json:"status"`
			Body   string `json:"body"`
		}
		if err := json.Unmarshal(s.Output, &out); err != nil || out.Status != 200 || out.Body != "GIF\x01 image" {
			t.Errorf("output = %s (%v), want status 200 and the body without NUL bytes", s.Output, err)
		}
	})
}

func TestStepTimeoutCountsAsFailure(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store, _ func() store.Store) {
		rec := newRecorder()
		slow := noopStep("slow")
		slow.Noop.SleepMS = 5_000
		slow.TimeoutMS = 100
		slow.Retry = workflow.RetryPolicy{MaxAttempts: 2, InitialBackoffMS: 10, MaxBackoffMS: 20}
		run := createRun(t, st, "timeout", slow)
		startEngine(t, st, engine.Config{Workers: 1, Poll: fast}, rec.wrap(realExecutors()), nil)

		run = waitRunFinished(t, st, run.ID, 20*time.Second)
		s := stepOf(t, run, "slow")
		if run.Status != store.RunFailed || s.Status != store.StepFailed || s.Attempt != 2 {
			t.Fatalf("run %s, step %s after %d attempts; want both failed after 2", run.Status, s.Status, s.Attempt)
		}
		if !strings.Contains(s.LastError, "timed out") {
			t.Errorf("last_error = %q, want a timeout", s.LastError)
		}
		for _, a := range rec.finished("slow") {
			if took := a.end.Sub(a.start); !errors.Is(a.err, context.DeadlineExceeded) || took > 2*time.Second {
				t.Errorf("attempt %d ended after %v with %v, want a deadline error after about 100ms", a.n, took, a.err)
			}
		}
	})
}

func TestCancelMidRunStopsRunningStep(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store, _ func() store.Store) {
		ctx := context.Background()
		rec := newRecorder()
		long := noopStep("a")
		long.Noop.SleepMS = 10_000
		run := createRun(t, st, "cancel", long, noopStep("b", "a"))
		// Lease 300ms: a heartbeat every 100ms notices the cancellation.
		startEngine(t, st, engine.Config{Workers: 2, Lease: 300 * time.Millisecond, Poll: fast}, rec.wrap(realExecutors()), nil)

		waitFor(t, 20*time.Second, "step a to start", func() bool { return rec.startedCount("a") > 0 })
		if _, err := st.CancelRun(ctx, run.ID); err != nil {
			t.Fatalf("CancelRun: %v", err)
		}
		waitFor(t, 5*time.Second, "step a to stop", func() bool { return len(rec.finished("a")) > 0 })
		if cause := rec.only(t, "a").cause; !errors.Is(cause, store.ErrRunCancelled) {
			t.Errorf("step a stopped with cause %v, want %v", cause, store.ErrRunCancelled)
		}
		// Two lease periods later, nobody has picked the abandoned step up.
		time.Sleep(600 * time.Millisecond)
		if n := rec.startedCount("a"); n != 1 {
			t.Errorf("step a started %d times, want 1", n)
		}
		got, err := st.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != store.RunCancelled {
			t.Errorf("run status = %s, want cancelled", got.Status)
		}
		if a := stepOf(t, got, "a"); a.Status == store.StepSucceeded {
			t.Errorf("a = succeeded, want its result discarded")
		}
		if b := stepOf(t, got, "b"); b.Status != store.StepCancelled {
			t.Errorf("b = %s, want cancelled", b.Status)
		}
	})
}

// Crash recovery: engine A dies in the middle of an HTTP call. Once its lease
// expires, engine B runs the step again, and the target sees the same
// Idempotency-Key twice. That is the at-least-once guarantee.
func TestCrashedEngineStepIsRetriedWithSameIdempotencyKey(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store, reopen func() store.Store) {
		var (
			mu        sync.Mutex
			keys      = map[string]int{}
			firstCall = make(chan struct{})
			release   = make(chan struct{})
		)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The server only notices a client hanging up once the request
			// body has been read, so read it first.
			io.Copy(io.Discard, r.Body)
			mu.Lock()
			keys[r.Header.Get("Idempotency-Key")]++
			n := keys[r.Header.Get("Idempotency-Key")]
			mu.Unlock()
			if n == 1 {
				close(firstCall)
				select { // hang until engine A dies and its client disconnects
				case <-r.Context().Done():
				case <-release:
				}
				return
			}
			w.Write([]byte(`{"ok":true}`))
		}))
		t.Cleanup(srv.Close)
		t.Cleanup(func() { close(release) }) // runs first, so Close never waits on a stuck handler
		run := createRun(t, st, "crash", httpStep("call", srv.URL))
		stB := reopen() // engine B's own store handle, like a separate process

		// Engine A has a short lease and (almost) no shutdown grace, so
		// stopping it acts like a crash: the call is cut off, nothing is
		// written, and only the lease expiring frees the step.
		cfg := engine.Config{Workers: 1, Lease: time.Second, Poll: 10 * time.Millisecond, ShutdownGrace: time.Millisecond}
		_, stopA := startEngine(t, st, cfg, realExecutors(), nil)
		receive(t, firstCall, 20*time.Second, "engine A's call")
		stopA()

		got, err := st.GetRun(context.Background(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if s := stepOf(t, got, "call"); s.Status != store.StepRunning || s.Attempt != 1 {
			t.Fatalf("after the crash the step is %s on attempt %d, want still running (leased to A) on attempt 1", s.Status, s.Attempt)
		}

		// Engine B takes over once A's lease has expired.
		cfg.ShutdownGrace = 0
		startEngine(t, stB, cfg, realExecutors(), nil)
		got = waitRunFinished(t, st, run.ID, 20*time.Second)
		if s := stepOf(t, got, "call"); got.Status != store.RunSucceeded || s.Attempt != 2 {
			t.Errorf("run %s, step on attempt %d; want succeeded on attempt 2", got.Status, s.Attempt)
		}
		mu.Lock()
		defer mu.Unlock()
		if want := map[string]int{run.ID + "/call": 2}; !maps.Equal(keys, want) {
			t.Errorf("target saw Idempotency-Keys %v, want %v", keys, want)
		}
	})
}

// Multi-instance: three engines, each with its own store handle (its own
// connection pool, like three processes), share 200 runs. With no crashes,
// every step must run exactly once.
func TestThreeEnginesRunEveryStepExactlyOnce(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store, reopen func() store.Store) {
		const runs, engines = 200, 3
		ctx := context.Background()
		var (
			mu   sync.Mutex
			keys = map[string]int{}
		)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			keys[r.Header.Get("Idempotency-Key")]++
			mu.Unlock()
		}))
		t.Cleanup(srv.Close)

		// Fan-out/fan-in: start -> call-1, call-2, call-3 -> end.
		calls := []string{"call-1", "call-2", "call-3"}
		steps := []workflow.Step{noopStep("start")}
		for _, id := range calls {
			steps = append(steps, httpStep(id, srv.URL, "start"))
		}
		steps = append(steps, noopStep("end", calls...))
		wf, err := st.CreateWorkflow(ctx, workflow.Definition{Name: "multi-instance", Steps: steps})
		if err != nil {
			t.Fatalf("CreateWorkflow: %v", err)
		}
		runIDs := make([]string, runs)
		for i := range runIDs {
			r, err := st.CreateRun(ctx, wf.ID, nil)
			if err != nil {
				t.Fatalf("CreateRun: %v", err)
			}
			runIDs[i] = r.ID
		}

		// Open every store before any engine starts working.
		stores := []store.Store{st, reopen(), reopen()}
		recs := make([]*recorder, engines)
		for i := range recs {
			recs[i] = newRecorder()
			startEngine(t, stores[i], engine.Config{Workers: 4, Poll: 20 * time.Millisecond}, recs[i].wrap(realExecutors()), nil)
		}

		pending := slices.Clone(runIDs)
		waitFor(t, 3*time.Minute, "all runs to finish", func() bool {
			pending = slices.DeleteFunc(pending, func(id string) bool {
				r, err := st.GetRun(ctx, id)
				if err != nil {
					t.Fatalf("GetRun: %v", err)
				}
				if r.Status.Finished() && r.Status != store.RunSucceeded {
					t.Fatalf("run %s finished as %s, want succeeded", id, r.Status)
				}
				return r.Status.Finished()
			})
			return len(pending) == 0
		})

		// Every step ran exactly once, counted by the executors...
		executed := map[string]int{}
		perEngine := make([]int, engines)
		for i, rec := range recs {
			for _, a := range rec.all() {
				executed[a.runID+"/"+a.stepID]++
				perEngine[i]++
			}
		}
		for _, id := range runIDs {
			for _, s := range steps {
				if n := executed[id+"/"+s.ID]; n != 1 {
					t.Errorf("run %s step %s executed %d times, want 1", id, s.ID, n)
				}
			}
		}
		// ...and by the target: each HTTP step's key was seen once.
		mu.Lock()
		defer mu.Unlock()
		if len(keys) != runs*len(calls) {
			t.Errorf("target saw %d distinct keys, want %d", len(keys), runs*len(calls))
		}
		for key, n := range keys {
			if n != 1 {
				t.Errorf("Idempotency-Key %s seen %d times, want 1", key, n)
			}
		}
		if len(executed) != runs*len(steps) {
			t.Errorf("%d distinct steps executed, want %d", len(executed), runs*len(steps))
		}
		t.Logf("steps executed per engine: %v", perEngine)
	})
}
