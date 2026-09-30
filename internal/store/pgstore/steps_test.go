package pgstore_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore"
	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore/pgtest"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// shortLease is long enough for the next statement to see a live lease, and
// short enough that waitExpiry keeps the tests fast.
const shortLease = 100 * time.Millisecond

func waitExpiry(c *store.Claim) { time.Sleep(time.Until(c.LeaseExpiresAt) + 50*time.Millisecond) }

func TestFencingRejectsStaleClaims(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()
	run := newRun(t, st, noop("a"))
	c := mustClaim(t, st, "w1", run.ID, "a")

	otherOwner := *c
	otherOwner.Owner = "w2"
	oldAttempt := *c
	oldAttempt.Attempt = c.Attempt - 1
	for name, stale := range map[string]*store.Claim{"other owner": &otherOwner, "old attempt": &oldAttempt} {
		_, err := st.Heartbeat(ctx, stale, longLease)
		wantErr(t, name+": Heartbeat", err, store.ErrLeaseLost)
		_, err = st.CompleteStep(ctx, stale, nil)
		wantErr(t, name+": CompleteStep", err, store.ErrLeaseLost)
		_, err = st.FailStep(ctx, stale, "boom", nil)
		wantErr(t, name+": FailStep", err, store.ErrLeaseLost)
	}
	wantStatuses(t, st, run.ID, "a=running")

	// The real owner can still renew and complete.
	expires, err := st.Heartbeat(ctx, c, 2*longLease)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !expires.After(c.LeaseExpiresAt) {
		t.Fatalf("Heartbeat expiry %v is not after the claim's %v", expires, c.LeaseExpiresAt)
	}
	if status := complete(t, st, c); status != store.RunSucceeded {
		t.Fatalf("run = %s, want succeeded", status)
	}
	// The step is finished, so the same claim is now stale too.
	_, err = st.CompleteStep(ctx, c, nil)
	wantErr(t, "second CompleteStep", err, store.ErrLeaseLost)
	_, err = st.Heartbeat(ctx, c, longLease)
	wantErr(t, "Heartbeat after completion", err, store.ErrLeaseLost)
}

// A worker that stops heartbeating loses its step: after the lease expires
// another worker re-claims it with the next attempt number, and the first
// worker's late writes are rejected.
func TestExpiredLeaseIsReclaimed(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()
	run := newRun(t, st, noop("a"), noop("b", "a"))

	c1 := claim(t, st, "w1", shortLease)
	if c1 == nil || c1.StepID != "a" {
		t.Fatalf("claim = %+v, want step a", c1)
	}
	noClaim(t, st) // the lease is still valid

	waitExpiry(c1)
	c2 := mustClaim(t, st, "w2", run.ID, "a")
	if c2.Attempt != 2 || c2.Owner != "w2" {
		t.Fatalf("re-claim = attempt %d by %s, want attempt 2 by w2", c2.Attempt, c2.Owner)
	}

	_, err := st.Heartbeat(ctx, c1, longLease)
	wantErr(t, "old owner Heartbeat", err, store.ErrLeaseLost)
	_, err = st.CompleteStep(ctx, c1, json.RawMessage(`"late"`))
	wantErr(t, "old owner CompleteStep", err, store.ErrLeaseLost)

	complete(t, st, c2)
	wantStatuses(t, st, run.ID, "a=succeeded b=ready")
	if a := getRun(t, st, run.ID).Steps[0]; a.Attempt != 2 || a.Output != nil {
		t.Fatalf("step a = %+v, want attempt 2 and no output from the old owner", a)
	}
}

// When the worker dies on the last attempt, the step is not handed out again:
// the next claim fails it, skips what depends on it and fails the run.
func TestExhaustedAttemptsFailTheRun(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	run := newRun(t, st, withAttempts(noop("a"), 2), noop("b", "a"), noop("c", "b"))

	c1 := claim(t, st, "w1", shortLease)
	waitExpiry(c1)
	c2 := claim(t, st, "w2", shortLease)
	if c2 == nil || c2.StepID != "a" || c2.Attempt != 2 {
		t.Fatalf("re-claim = %+v, want step a attempt 2", c2)
	}
	waitExpiry(c2)
	noClaim(t, st)

	got := getRun(t, st, run.ID)
	wantStatuses(t, st, run.ID, "a=failed b=skipped c=skipped")
	if got.Status != store.RunFailed || got.FinishedAt == nil {
		t.Fatalf("run = %s (finished %v), want failed", got.Status, got.FinishedAt)
	}
	if a := got.Steps[0]; a.Attempt != 2 || !strings.Contains(a.LastError, "lease expired") || a.FinishedAt == nil {
		t.Fatalf("step a = %+v", a)
	}
	_, err := st.CompleteStep(context.Background(), c2, nil)
	wantErr(t, "CompleteStep after expiry", err, store.ErrLeaseLost)
}

func TestCompletePromotesDiamond(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()
	run := newRun(t, st, noop("a"), noop("b", "a"), noop("c", "a"), noop("d", "b", "c"))

	a := mustClaim(t, st, "w", run.ID, "a")
	noClaim(t, st) // b and c wait for a
	status, err := st.CompleteStep(ctx, a, json.RawMessage(`{"built": true}`))
	if err != nil || status != store.RunRunning {
		t.Fatalf("CompleteStep(a) = %s, %v", status, err)
	}
	wantStatuses(t, st, run.ID, "a=succeeded b=ready c=ready d=pending")

	b := mustClaim(t, st, "w", run.ID, "b")
	c := mustClaim(t, st, "w", run.ID, "c")
	noClaim(t, st)
	if status := complete(t, st, b); status != store.RunRunning {
		t.Fatalf("run = %s after b, want running", status)
	}
	wantStatuses(t, st, run.ID, "a=succeeded b=succeeded c=running d=pending")
	if status := complete(t, st, c); status != store.RunRunning {
		t.Fatalf("run = %s after c, want running", status)
	}
	wantStatuses(t, st, run.ID, "a=succeeded b=succeeded c=succeeded d=ready")

	d := mustClaim(t, st, "w", run.ID, "d")
	// Output that is not JSON (an HTML error page, say) is kept as a string.
	status, err = st.CompleteStep(ctx, d, json.RawMessage("<html>ok</html>"))
	if err != nil || status != store.RunSucceeded {
		t.Fatalf("CompleteStep(d) = %s, %v; want succeeded", status, err)
	}

	got := getRun(t, st, run.ID)
	if got.Status != store.RunSucceeded || got.FinishedAt == nil {
		t.Fatalf("run = %s (finished %v), want succeeded", got.Status, got.FinishedAt)
	}
	jsonEqual(t, got.Steps[0].Output, `{"built":true}`)
	jsonEqual(t, got.Steps[3].Output, `"<html>ok</html>"`)
	for _, s := range got.Steps {
		if s.Attempt != 1 || s.StartedAt == nil || s.FinishedAt == nil || s.FinishedAt.Before(*s.StartedAt) {
			t.Fatalf("step %s = %+v", s.StepID, s)
		}
	}
}

func TestFailStepRetryThenPermanent(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()
	run := newRun(t, st, noop("a"), noop("b", "a"), noop("c", "b"))
	past := time.Now().Add(-time.Second)

	c := mustClaim(t, st, "w", run.ID, "a")
	status, err := st.FailStep(ctx, c, "boom 1", &past)
	if err != nil || status != store.RunRunning {
		t.Fatalf("FailStep(retry) = %s, %v; want running", status, err)
	}
	a := getRun(t, st, run.ID).Steps[0]
	if a.Status != store.StepReady || a.Attempt != 1 || a.LastError != "boom 1" || a.FinishedAt != nil {
		t.Fatalf("after a retryable failure, step a = %+v", a)
	}
	_, err = st.CompleteStep(ctx, c, nil)
	wantErr(t, "CompleteStep after FailStep", err, store.ErrLeaseLost)

	c = mustClaim(t, st, "w", run.ID, "a")
	if c.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", c.Attempt)
	}
	status, err = st.FailStep(ctx, c, "boom 2: bad request", nil)
	if err != nil || status != store.RunFailed {
		t.Fatalf("FailStep(permanent) = %s, %v; want failed", status, err)
	}
	wantStatuses(t, st, run.ID, "a=failed b=skipped c=skipped")
	got := getRun(t, st, run.ID)
	if got.Status != store.RunFailed || got.FinishedAt == nil || got.Steps[0].LastError != "boom 2: bad request" {
		t.Fatalf("run = %+v", got)
	}
	noClaim(t, st)
}

// The store never lets attempt exceed max_attempts, even if the caller asks
// for another retry.
func TestFailStepWithoutAttemptsLeftIsPermanent(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	run := newRun(t, st, withAttempts(noop("a"), 1), noop("b", "a"))
	c := mustClaim(t, st, "w", run.ID, "a")
	past := time.Now().Add(-time.Second)
	status, err := st.FailStep(context.Background(), c, "boom", &past)
	if err != nil || status != store.RunFailed {
		t.Fatalf("FailStep = %s, %v; want failed", status, err)
	}
	wantStatuses(t, st, run.ID, "a=failed b=skipped")
}

// A failed run starts nothing new: steps on independent branches that have
// not started are skipped, while a step that is already running may finish.
func TestFailedRunIsFailFast(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()
	run := newRun(t, st, noop("a"), noop("b"), noop("c"), noop("after-a", "a"), noop("after-c", "c"))

	a := mustClaim(t, st, "w", run.ID, "a")
	b := mustClaim(t, st, "w", run.ID, "b")
	if status, err := st.FailStep(ctx, a, "boom", nil); err != nil || status != store.RunFailed {
		t.Fatalf("FailStep = %s, %v", status, err)
	}
	wantStatuses(t, st, run.ID, "a=failed b=running c=skipped after-a=skipped after-c=skipped")
	noClaim(t, st)

	// b still holds its lease and may record its result; the run stays failed.
	if _, err := st.Heartbeat(ctx, b, longLease); err != nil {
		t.Fatalf("Heartbeat(b): %v", err)
	}
	if status := complete(t, st, b); status != store.RunFailed {
		t.Fatalf("run = %s, want failed", status)
	}
	wantStatuses(t, st, run.ID, "a=failed b=succeeded c=skipped after-a=skipped after-c=skipped")
}

func TestCancelRun(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()
	run := newRun(t, st, noop("a"), noop("b", "a"), noop("c"))
	a := mustClaim(t, st, "w", run.ID, "a")

	cancelled, err := st.CancelRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if cancelled.Status != store.RunCancelled || cancelled.FinishedAt == nil {
		t.Fatalf("CancelRun = %+v", cancelled)
	}
	wantStatuses(t, st, run.ID, "a=cancelled b=cancelled c=cancelled")

	// The running step learns about it at its next heartbeat, and its
	// result is discarded.
	_, err = st.Heartbeat(ctx, a, longLease)
	wantErr(t, "Heartbeat", err, store.ErrRunCancelled)
	_, err = st.CompleteStep(ctx, a, nil)
	wantErr(t, "CompleteStep", err, store.ErrLeaseLost)
	_, err = st.FailStep(ctx, a, "boom", nil)
	wantErr(t, "FailStep", err, store.ErrLeaseLost)
	noClaim(t, st)

	// Cancelling again returns the run unchanged.
	again, err := st.CancelRun(ctx, run.ID)
	if err != nil || again.Status != store.RunCancelled || !again.FinishedAt.Equal(*cancelled.FinishedAt) {
		t.Fatalf("second CancelRun = %+v, %v", again, err)
	}

	// A finished run cannot be cancelled.
	done := newRun(t, st, noop("x"))
	complete(t, st, mustClaim(t, st, "w", done.ID, "x"))
	_, err = st.CancelRun(ctx, done.ID)
	wantErr(t, "CancelRun(succeeded)", err, store.ErrConflict)

	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "nope"} {
		_, err := st.CancelRun(ctx, id)
		wantErr(t, "CancelRun("+id+")", err, store.ErrNotFound)
	}
}

// Cancelling a run whose step is being claimed at the same moment must never
// leave that step running: either the claim misses it, or the cancel
// catches it right after the claim.
func TestCancelWhileClaiming(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()
	wf := createWorkflow(t, st, noop("a"), noop("b"), noop("c"))
	for range 20 {
		run := createRun(t, st, wf.ID)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range 3 {
				if _, err := st.ClaimStep(ctx, "w", longLease); err != nil {
					t.Errorf("ClaimStep: %v", err)
				}
			}
		}()
		if _, err := st.CancelRun(ctx, run.ID); err != nil {
			t.Fatalf("CancelRun: %v", err)
		}
		<-done
		wantStatuses(t, st, run.ID, "a=cancelled b=cancelled c=cancelled")
	}
}

func TestListRunsPagination(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()
	wf := createWorkflow(t, st, noop("a"))
	other := createWorkflow(t, st, noop("a"))

	var created []string // oldest first
	for range 7 {
		created = append(created, createRun(t, st, wf.ID).ID)
	}
	createRun(t, st, other.ID)

	page := func(cursor string) ([]string, string) {
		t.Helper()
		runs, next, err := st.ListRuns(ctx, store.ListRunsFilter{WorkflowID: wf.ID, Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("ListRuns: %v", err)
		}
		var ids []string
		for _, r := range runs {
			if r.WorkflowID != wf.ID || r.Steps != nil {
				t.Fatalf("ListRuns returned %+v", r)
			}
			ids = append(ids, r.ID)
		}
		return ids, next
	}

	p1, next1 := page("")
	// A run created after the first page must not shift the later pages.
	createRun(t, st, wf.ID)
	p2, next2 := page(next1)
	p3, next3 := page(next2)
	if len(p1) != 3 || len(p2) != 3 || len(p3) != 1 || next1 == "" || next2 == "" || next3 != "" {
		t.Fatalf("pages of %d, %d, %d with cursors %q, %q, %q; want 3, 3, 1 and no cursor after the last",
			len(p1), len(p2), len(p3), next1, next2, next3)
	}
	all := append(append(append([]string{}, p1...), p2...), p3...)
	for i, id := range all {
		if want := created[len(created)-1-i]; id != want {
			t.Fatalf("position %d: got run %s, want %s (newest first, no duplicates)", i, id, want)
		}
	}
	if again, _ := page(next1); strings.Join(again, ",") != strings.Join(p2, ",") {
		t.Fatalf("page 2 changed: %v then %v", p2, again)
	}

	// Filters and defaults.
	runs, next, err := st.ListRuns(ctx, store.ListRunsFilter{})
	if err != nil || len(runs) != 9 || next != "" {
		t.Fatalf("ListRuns(no filter) = %d runs, next %q, %v; want 9 (default limit 20)", len(runs), next, err)
	}
	if _, err := st.CancelRun(ctx, created[2]); err != nil {
		t.Fatal(err)
	}
	runs, _, err = st.ListRuns(ctx, store.ListRunsFilter{Status: store.RunCancelled})
	if err != nil || len(runs) != 1 || runs[0].ID != created[2] {
		t.Fatalf("ListRuns(cancelled) = %+v, %v", runs, err)
	}
	runs, _, err = st.ListRuns(ctx, store.ListRunsFilter{WorkflowID: "not-a-uuid"})
	if err != nil || len(runs) != 0 {
		t.Fatalf("ListRuns(bad workflow id) = %+v, %v; want no runs", runs, err)
	}
	// A forged cursor can hold a time that PostgreSQL's timestamptz cannot
	// represent. It must be reported as a bad cursor, not as a query error.
	forged := func(micros string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(micros + ",00000000-0000-0000-0000-000000000000"))
	}
	for _, bad := range []string{"%%%", "bm90IGEgY3Vyc29y", "MTIzLG5vdC1hLXV1aWQ",
		forged("-300000000000000000"), forged("9223372036854775807"), forged("-9223372036854775808")} {
		_, _, err := st.ListRuns(ctx, store.ListRunsFilter{Cursor: bad})
		wantErr(t, "ListRuns(cursor "+bad+")", err, store.ErrInvalidCursor)
	}
}

// A run lock held elsewhere, for example by a flowd instance that froze in
// the middle of a transaction, must not stop claims. Here the locked run
// has a dead step (its lease expired on its last attempt), which the claim's
// sweep would fail if it could lock the run.
func TestLockedRunDoesNotBlockClaims(t *testing.T) {
	t.Parallel()
	url := pgtest.NewSchemaURL(t)
	ctx := context.Background()
	st, err := pgstore.Open(ctx, url)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	stuck := newRun(t, st, withAttempts(noop("x"), 1))
	c := claim(t, st, "w1", shortLease)
	if c == nil || c.RunID != stuck.ID {
		t.Fatalf("claim = %+v, want step x of run %s", c, stuck.ID)
	}
	waitExpiry(c)

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
	if _, err := tx.Exec(ctx, `SELECT 1 FROM runs WHERE id = $1 FOR UPDATE`, stuck.ID); err != nil {
		t.Fatal(err)
	}

	claimPromptly := func() *store.Claim {
		t.Helper()
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		start := time.Now()
		c, err := st.ClaimStep(cctx, "w2", longLease)
		if err != nil {
			t.Fatalf("ClaimStep while another session locks run %s: %v (after %v)", stuck.ID, err, time.Since(start))
		}
		return c
	}
	// Nothing is runnable, so the claim sweeps, and the sweep must pass
	// over the locked run instead of waiting for it.
	if c := claimPromptly(); c != nil {
		t.Fatalf("claimed %s/%s, want nothing runnable", c.RunID, c.StepID)
	}
	// Work of other runs is still handed out.
	other := newRun(t, st, noop("y"))
	if c := claimPromptly(); c == nil || c.RunID != other.ID || c.StepID != "y" {
		t.Fatalf("claim = %+v, want step y of run %s", c, other.ID)
	}
	wantStatuses(t, st, stuck.ID, "x=running")

	// Skipping is only for now: once the lock is gone, the next sweep fails
	// the dead step and its run.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	noClaim(t, st)
	wantStatuses(t, st, stuck.ID, "x=failed")
	if got := getRun(t, st, stuck.ID); got.Status != store.RunFailed {
		t.Fatalf("run = %s, want failed", got.Status)
	}
}

// A flowd process that freezes or is cut off while it holds a run lock must
// lose that lock soon. The workers of healthy instances that complete steps
// of the same run wait for it, so PostgreSQL has to end the idle session.
func TestFrozenWriterLosesItsRunLock(t *testing.T) {
	t.Parallel()
	url := pgtest.NewSchemaURL(t)
	ctx := context.Background()
	st, err := pgstore.Open(ctx, url)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	pgstore.SetIdleTxTimeout(st, 500*time.Millisecond)
	run := newRun(t, st, noop("a"))

	locked, release := make(chan struct{}), make(chan struct{})
	held := make(chan error, 1)
	go func() { held <- pgstore.HoldRunLock(ctx, st, run.ID, locked, release) }()
	select {
	case <-locked:
	case err := <-held:
		t.Fatalf("HoldRunLock: %v", err)
	}

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := conn.Exec(lockCtx, `SELECT 1 FROM runs WHERE id = $1 FOR UPDATE`, run.ID); err != nil {
		t.Fatalf("locking the run held by a frozen session: %v (after %v)", err, time.Since(start))
	}
	t.Logf("the frozen session's run lock was released after %v", time.Since(start).Round(time.Millisecond))

	close(release)
	if err := <-held; err == nil {
		t.Fatal("the frozen transaction committed; want its session ended by the server")
	}
}

// Output that json.Valid accepts but a jsonb column rejects must still be
// saved. Otherwise CompleteStep fails on every attempt, and a step whose
// target succeeded runs again until its attempts run out and the run fails.
// Such output is kept as text, like output that is not JSON at all.
func TestUnstorableOutputIsSavedAsText(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	ctx := context.Background()
	cases := []struct{ step, output, want string }{
		{"nul-escape", `{"x":"a\u0000b"}`, `{"x":"a\u0000b"}`},
		{"lone-surrogate", `{"x":"\ud800"}`, `{"x":"\ud800"}`},
		{"invalid-utf8", "{\"x\":\"caf\xe9\"}", "{\"x\":\"caf�\"}"},
	}
	var steps []workflow.Step
	for _, tc := range cases {
		steps = append(steps, noop(tc.step))
	}
	run := newRun(t, st, steps...)
	for _, tc := range cases {
		c := mustClaim(t, st, "w", run.ID, tc.step)
		if _, err := st.CompleteStep(ctx, c, json.RawMessage(tc.output)); err != nil {
			t.Fatalf("CompleteStep(%s): %v", tc.step, err)
		}
	}

	got := getRun(t, st, run.ID)
	if got.Status != store.RunSucceeded {
		t.Fatalf("run = %s, want succeeded", got.Status)
	}
	for i, tc := range cases {
		var text string
		if err := json.Unmarshal(got.Steps[i].Output, &text); err != nil {
			t.Fatalf("step %s: output %s is not a JSON string: %v", tc.step, got.Steps[i].Output, err)
		}
		if text != tc.want {
			t.Errorf("step %s: output text = %q, want %q", tc.step, text, tc.want)
		}
	}
}

// Claims hand out the definition stored with the run, including http specs.
func TestClaimCarriesHTTPSpec(t *testing.T) {
	t.Parallel()
	st := pgtest.New(t)
	run := newRun(t, st, workflow.Step{ID: "call", Type: workflow.StepHTTP, TimeoutMS: 1234,
		HTTP: &workflow.HTTPSpec{Method: "put", URL: "https://example.com/x", Body: json.RawMessage(`[1,2]`)}})
	c := mustClaim(t, st, "w", run.ID, "call")
	if c.Step.HTTP == nil || c.Step.HTTP.Method != "PUT" || c.Step.HTTP.URL != "https://example.com/x" || c.Step.Timeout() != 1234*time.Millisecond {
		t.Fatalf("claimed step = %+v (http %+v)", c.Step, c.Step.HTTP)
	}
	jsonEqual(t, c.Step.HTTP.Body, `[1,2]`)
}
