package pgstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore"
	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore/pgtest"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

const longLease = time.Minute

func noop(id string, deps ...string) workflow.Step {
	return workflow.Step{ID: id, Type: workflow.StepNoop, DependsOn: deps}
}

// withAttempts returns s with its retry budget set to n attempts.
func withAttempts(s workflow.Step, n int) workflow.Step {
	s.Retry.MaxAttempts = n
	return s
}

func createWorkflow(t *testing.T, st *pgstore.Store, steps ...workflow.Step) store.Workflow {
	t.Helper()
	wf, err := st.CreateWorkflow(context.Background(), workflow.Definition{Name: "test", Steps: steps})
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	return wf
}

func createRun(t *testing.T, st *pgstore.Store, workflowID string) store.Run {
	t.Helper()
	run, err := st.CreateRun(context.Background(), workflowID, nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return run
}

// newRun creates a workflow with steps and one run of it.
func newRun(t *testing.T, st *pgstore.Store, steps ...workflow.Step) store.Run {
	t.Helper()
	return createRun(t, st, createWorkflow(t, st, steps...).ID)
}

func claim(t *testing.T, st *pgstore.Store, owner string, lease time.Duration) *store.Claim {
	t.Helper()
	c, err := st.ClaimStep(context.Background(), owner, lease)
	if err != nil {
		t.Fatalf("ClaimStep: %v", err)
	}
	return c
}

// mustClaim claims a step and checks it is the expected one.
func mustClaim(t *testing.T, st *pgstore.Store, owner, runID, stepID string) *store.Claim {
	t.Helper()
	c := claim(t, st, owner, longLease)
	if c == nil {
		t.Fatalf("ClaimStep = nil, want %s", stepID)
	}
	if c.RunID != runID || c.StepID != stepID {
		t.Fatalf("claimed %s/%s, want %s/%s", c.RunID, c.StepID, runID, stepID)
	}
	return c
}

func noClaim(t *testing.T, st *pgstore.Store) {
	t.Helper()
	if c := claim(t, st, "w", longLease); c != nil {
		t.Fatalf("claimed %s/%s (attempt %d), want nothing runnable", c.RunID, c.StepID, c.Attempt)
	}
}

func complete(t *testing.T, st *pgstore.Store, c *store.Claim) store.RunStatus {
	t.Helper()
	status, err := st.CompleteStep(context.Background(), c, nil)
	if err != nil {
		t.Fatalf("CompleteStep(%s): %v", c.StepID, err)
	}
	return status
}

func getRun(t *testing.T, st *pgstore.Store, id string) store.Run {
	t.Helper()
	run, err := st.GetRun(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	return run
}

// statuses returns "step=status" pairs in the order GetRun returns them.
func statuses(t *testing.T, st *pgstore.Store, runID string) string {
	t.Helper()
	var parts []string
	for _, s := range getRun(t, st, runID).Steps {
		parts = append(parts, s.StepID+"="+string(s.Status))
	}
	return strings.Join(parts, " ")
}

func wantStatuses(t *testing.T, st *pgstore.Store, runID, want string) {
	t.Helper()
	if got := statuses(t, st, runID); got != want {
		t.Fatalf("steps: %s\n          want: %s", got, want)
	}
}

func wantErr(t *testing.T, what string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s: err = %v, want %v", what, err, want)
	}
}

func jsonEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("invalid JSON %q: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("JSON = %s, want %s", got, want)
	}
}

func TestWorkflowRoundTrip(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()

	def := workflow.Definition{Name: "deploy", Steps: []workflow.Step{
		{ID: "build", Type: workflow.StepHTTP, HTTP: &workflow.HTTPSpec{
			Method: "POST", URL: "http://example.com/build",
			Headers: map[string]string{"X-Team": "infra"}, Body: json.RawMessage(`{"ref":"main"}`),
		}},
		noop("test", "build"),
	}}
	wf, err := st.CreateWorkflow(ctx, def)
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	if def.Steps[0].Retry.MaxAttempts != 0 {
		t.Fatal("CreateWorkflow changed the caller's definition")
	}

	got, err := st.GetWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.ID != wf.ID || got.Name != "deploy" || !got.CreatedAt.Equal(wf.CreatedAt) {
		t.Fatalf("GetWorkflow = %+v, want %+v", got, wf)
	}
	build := got.Definition.Steps[0]
	if build.Retry.MaxAttempts != workflow.DefaultMaxAttempts || build.TimeoutMS != int(workflow.DefaultTimeout.Milliseconds()) {
		t.Fatalf("stored step lacks defaults: %+v", build)
	}
	if build.HTTP.Headers["X-Team"] != "infra" {
		t.Fatalf("headers = %v", build.HTTP.Headers)
	}
	jsonEqual(t, build.HTTP.Body, `{"ref":"main"}`)
	if deps := got.Definition.Steps[1].DependsOn; !reflect.DeepEqual(deps, []string{"build"}) {
		t.Fatalf("depends_on = %v", deps)
	}

	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "not-a-uuid", ""} {
		_, err := st.GetWorkflow(ctx, id)
		wantErr(t, "GetWorkflow("+id+")", err, store.ErrNotFound)
		_, err = st.CreateRun(ctx, id, nil)
		wantErr(t, "CreateRun("+id+")", err, store.ErrNotFound)
	}

	_, err = st.CreateWorkflow(ctx, workflow.Definition{Name: "bad", Steps: []workflow.Step{noop("a", "a")}})
	var ve *workflow.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("CreateWorkflow(invalid) = %v, want a *workflow.ValidationError", err)
	}
}

func TestCreateAndGetRun(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()

	// Defined out of order: GetRun must return the topological order
	// a, c, b, d (ties go to the step defined first).
	wf := createWorkflow(t, st, noop("d", "b", "c"), noop("c", "a"), noop("b", "a"), noop("a"))
	run, err := st.CreateRun(ctx, wf.ID, json.RawMessage(`{"env": "staging"}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if run.WorkflowID != wf.ID || run.Status != store.RunRunning || run.FinishedAt != nil || run.CreatedAt.IsZero() {
		t.Fatalf("CreateRun = %+v", run)
	}
	jsonEqual(t, run.Input, `{"env":"staging"}`)

	got := getRun(t, st, run.ID)
	if got.ID != run.ID || got.Status != store.RunRunning {
		t.Fatalf("GetRun = %+v", got)
	}
	jsonEqual(t, got.Input, `{"env":"staging"}`)
	wantStatuses(t, st, run.ID, "a=ready c=pending b=pending d=pending")
	d := got.Steps[3]
	if !reflect.DeepEqual(d.DependsOn, []string{"b", "c"}) || d.MaxAttempts != 3 || d.Attempt != 0 || d.StartedAt != nil {
		t.Fatalf("step d = %+v", d)
	}

	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "nope"} {
		_, err := st.GetRun(ctx, id)
		wantErr(t, "GetRun("+id+")", err, store.ErrNotFound)
	}

	// No input is stored as NULL and comes back empty.
	bare := createRun(t, st, wf.ID)
	if got := getRun(t, st, bare.ID); got.Input != nil {
		t.Fatalf("input = %s, want none", got.Input)
	}
}

func TestClaimOrderAndPayload(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)

	wf := createWorkflow(t, st, noop("a"), noop("b"), noop("c"))
	run1, err := st.CreateRun(context.Background(), wf.ID, json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatal(err)
	}
	run2 := createRun(t, st, wf.ID)

	// Older runs first; within a run, topological (here: definition) order.
	first := mustClaim(t, st, "w1", run1.ID, "a")
	mustClaim(t, st, "w1", run1.ID, "b")
	mustClaim(t, st, "w1", run1.ID, "c")
	mustClaim(t, st, "w1", run2.ID, "a")
	mustClaim(t, st, "w1", run2.ID, "b")
	mustClaim(t, st, "w1", run2.ID, "c")
	noClaim(t, st)

	if first.Owner != "w1" || first.Attempt != 1 || first.Step.ID != "a" || first.Step.Type != workflow.StepNoop {
		t.Fatalf("claim = %+v", first)
	}
	if first.Step.Retry.MaxAttempts != workflow.DefaultMaxAttempts {
		t.Fatalf("claimed step lacks defaults: %+v", first.Step)
	}
	jsonEqual(t, first.Input, `{"n":1}`)
	if left := time.Until(first.LeaseExpiresAt); left <= 0 || left > longLease+time.Second {
		t.Fatalf("lease expires in %v, want about %v", left, longLease)
	}
	step := getRun(t, st, run1.ID).Steps[0]
	if step.Status != store.StepRunning || step.Attempt != 1 || step.StartedAt == nil {
		t.Fatalf("claimed step = %+v", step)
	}
}

func TestClaimRespectsNotBefore(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()

	later := newRun(t, st, noop("a"))
	sooner := newRun(t, st, noop("a"))
	c1 := mustClaim(t, st, "w", later.ID, "a")
	c2 := mustClaim(t, st, "w", sooner.ID, "a")

	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Second)
	if _, err := st.FailStep(ctx, c1, "try later", &future); err != nil {
		t.Fatal(err)
	}
	if _, err := st.FailStep(ctx, c2, "try now", &past); err != nil {
		t.Fatal(err)
	}
	// Only the step whose retry time has passed is runnable.
	c := mustClaim(t, st, "w", sooner.ID, "a")
	if c.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", c.Attempt)
	}
	noClaim(t, st)

	// A retry becomes claimable once its time comes.
	complete(t, st, c)
	run := newRun(t, st, noop("a"))
	c = mustClaim(t, st, "w", run.ID, "a")
	soon := time.Now().Add(300 * time.Millisecond)
	if _, err := st.FailStep(ctx, c, "try soon", &soon); err != nil {
		t.Fatal(err)
	}
	noClaim(t, st)
	time.Sleep(time.Until(soon) + 50*time.Millisecond)
	mustClaim(t, st, "w", run.ID, "a")
}

// Many workers claiming at once must never get the same step: FOR UPDATE
// SKIP LOCKED hands each row to exactly one of them.
func TestConcurrentClaimsNeverShareAStep(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	const runs, stepsPerRun, workers = 10, 5, 16

	steps := make([]workflow.Step, stepsPerRun)
	for i := range steps {
		steps[i] = noop(fmt.Sprintf("s%d", i))
	}
	wf := createWorkflow(t, st, steps...)
	for range runs {
		createRun(t, st, wf.ID)
	}

	var mu sync.Mutex
	claimedBy := make(map[string]string) // "run/step" -> owner
	perWorker := make([]int, workers)
	var wg sync.WaitGroup
	for w := range workers {
		owner := fmt.Sprintf("worker-%d", w)
		wg.Go(func() {
			for {
				c, err := st.ClaimStep(context.Background(), owner, longLease)
				if err != nil {
					t.Errorf("%s: ClaimStep: %v", owner, err)
					return
				}
				if c == nil {
					return // nothing left
				}
				key := c.RunID + "/" + c.StepID
				mu.Lock()
				if prev, dup := claimedBy[key]; dup {
					t.Errorf("%s claimed by both %s and %s", key, prev, owner)
				}
				claimedBy[key] = owner
				perWorker[w]++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(claimedBy) != runs*stepsPerRun {
		t.Fatalf("claimed %d distinct steps, want %d", len(claimedBy), runs*stepsPerRun)
	}
	total := 0
	for _, n := range perWorker {
		total += n
	}
	if total != runs*stepsPerRun {
		t.Fatalf("made %d claims for %d steps: some step was handed out twice", total, runs*stepsPerRun)
	}
	t.Logf("claims per worker: %v", perWorker)
}

// Several Opens racing on an empty schema must all succeed.
func TestConcurrentOpen(t *testing.T) {
	t.Parallel()
	url := pgtest.NewSchemaURL(t)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			st, err := pgstore.Open(context.Background(), url)
			if err != nil {
				t.Errorf("Open: %v", err)
				return
			}
			defer st.Close()
			if err := st.Ping(context.Background()); err != nil {
				t.Errorf("Ping: %v", err)
			}
		})
	}
	wg.Wait()
}

// Starting flowd next to busy instances must not wait for their
// transactions. Here another session holds the table lock that every UPDATE
// of runs and steps takes. Re-running CREATE INDEX IF NOT EXISTS on an
// existing schema would wait for that lock.
func TestOpenDoesNotWaitForWriters(t *testing.T) {
	t.Parallel()
	url := pgtest.NewSchemaURL(t)
	ctx := context.Background()
	first, err := pgstore.Open(ctx, url)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer first.Close()

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) // runs before the schema is dropped, which would wait for it
	if _, err := tx.Exec(ctx, `LOCK TABLE runs, steps IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	openCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	second, err := pgstore.Open(openCtx, url)
	if err != nil {
		t.Fatalf("Open while another session is writing: %v", err)
	}
	second.Close()
}
