// Command flowd-bench measures how fast one engine drains a burst of runs
// stored in PostgreSQL.
//
// It creates a temporary schema, stores a workflow of -steps noop steps shaped
// as fan-out/fan-in (first -> steps-2 parallel steps -> last), starts an
// engine with -workers workers in this process, submits -runs runs straight to
// the store as fast as it can, and waits for all of them to finish. It then
// reports:
//
//   - wall time: from the first run's created_at to the last run's
//     finished_at;
//   - throughput: steps executed per second of wall time;
//   - run latency p50/p95/p99 (nearest rank): finished_at - created_at.
//
// All timestamps come from the database clock. Every run is submitted at
// once, so latency includes the time a run queues behind earlier runs. The
// schema is dropped on exit, so each invocation starts from empty tables.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kaustubhagarwal21/flowd/internal/engine"
	"github.com/kaustubhagarwal21/flowd/internal/executor"
	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "flowd-bench:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		runs    = flag.Int("runs", 1000, "number of runs to submit")
		steps   = flag.Int("steps", 10, "noop steps per run: first -> steps-2 in parallel -> last")
		workers = flag.Int("workers", 8, "engine worker goroutines")
		dbURL   = flag.String("database-url", os.Getenv("FLOWD_DATABASE_URL"), "PostgreSQL URL (default $FLOWD_DATABASE_URL)")
		timeout = flag.Duration("timeout", 10*time.Minute, "give up after this long")
	)
	flag.Parse()
	if *runs < 1 || *steps < 1 || *workers < 1 {
		return errors.New("-runs, -steps and -workers must be at least 1")
	}
	if *dbURL == "" {
		return errors.New("-database-url (or FLOWD_DATABASE_URL) is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	// Deferred calls run in reverse: stop the engine, close the store, drop
	// the schema, close the admin connection.
	admin, err := pgx.Connect(ctx, *dbURL)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer admin.Close(context.Background())
	env, err := describeEnv(ctx, admin)
	if err != nil {
		return err
	}
	schema := fmt.Sprintf("flowd_bench_%08x", rand.Uint32())
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}
	defer func() {
		// ctx may already be cancelled here, so use a fresh one.
		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.Exec(dropCtx, "DROP SCHEMA "+ident+" CASCADE"); err != nil {
			fmt.Fprintln(os.Stderr, "flowd-bench: dropping schema:", err)
		}
	}()
	st, err := pgstore.Open(ctx, withSearchPath(*dbURL, schema))
	if err != nil {
		return fmt.Errorf("opening store: %w", err)
	}
	defer st.Close()

	def := fanOutFanIn(*steps)
	if _, err := workflow.Validate(&def); err != nil {
		return err
	}
	wf, err := st.CreateWorkflow(ctx, def)
	if err != nil {
		return err
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	execs := map[workflow.StepType]engine.Executor{workflow.StepNoop: executor.NewNoop()}
	eng := engine.New(engine.Config{Workers: *workers}, st, execs, nil, log)
	engCtx, stopEngine := context.WithCancel(ctx)
	engDone := make(chan error, 1)
	go func() { engDone <- eng.Run(engCtx) }()
	defer func() {
		stopEngine()
		<-engDone
	}()

	fmt.Printf("flowd-bench: %d runs x %d noop steps (fan-out/fan-in), %d workers\n", *runs, *steps, *workers)
	fmt.Println(env)

	begin := time.Now()
	for range *runs {
		if _, err := st.CreateRun(ctx, wf.ID, nil); err != nil {
			return fmt.Errorf("creating run: %w", err)
		}
		eng.Wake()
	}
	fmt.Printf("submitted %d runs in %v\n", *runs, time.Since(begin).Round(time.Millisecond))

	if err := waitAllFinished(ctx, st, wf.ID); err != nil {
		return err
	}
	all, err := listRuns(ctx, st, wf.ID)
	if err != nil {
		return err
	}
	if len(all) != *runs {
		return fmt.Errorf("found %d runs, want %d", len(all), *runs)
	}
	return report(all, *steps)
}

// fanOutFanIn returns a workflow of k noop steps: "first", then k-2
// parallel steps that depend on it, then "last", which depends on all of
// them. k=1 is a single step and k=2 a chain of two.
func fanOutFanIn(k int) workflow.Definition {
	noop := func(id string, deps ...string) workflow.Step {
		return workflow.Step{ID: id, Type: workflow.StepNoop, DependsOn: deps, Noop: &workflow.NoopSpec{}}
	}
	def := workflow.Definition{Name: "flowd-bench", Steps: []workflow.Step{noop("first")}}
	if k == 1 {
		return def
	}
	mids := []string{"first"} // what "last" waits for
	if k > 2 {
		mids = nil
		for i := 1; i <= k-2; i++ {
			id := fmt.Sprintf("mid-%d", i)
			def.Steps = append(def.Steps, noop(id, "first"))
			mids = append(mids, id)
		}
	}
	def.Steps = append(def.Steps, noop("last", mids...))
	return def
}

// waitAllFinished polls until no run of the workflow is still running. It
// asks for a single running run each time, so polling adds almost no load.
func waitAllFinished(ctx context.Context, st store.Store, workflowID string) error {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		running, _, err := st.ListRuns(ctx, store.ListRunsFilter{WorkflowID: workflowID, Status: store.RunRunning, Limit: 1})
		if err != nil {
			return fmt.Errorf("waiting for runs: %w", err)
		}
		if len(running) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("runs still running: %w", context.Cause(ctx))
		case <-tick.C:
		}
	}
}

// listRuns returns every run of the workflow, following the page cursor.
func listRuns(ctx context.Context, st store.Store, workflowID string) ([]store.Run, error) {
	var all []store.Run
	cursor := ""
	for {
		page, next, err := st.ListRuns(ctx, store.ListRunsFilter{WorkflowID: workflowID, Limit: 100, Cursor: cursor})
		if err != nil {
			return nil, fmt.Errorf("listing runs: %w", err)
		}
		all = append(all, page...)
		if next == "" {
			return all, nil
		}
		cursor = next
	}
}

// report prints wall time, throughput and latency percentiles. Noop steps
// never fail, so a run that did not succeed means something is wrong, and
// the numbers would not mean what they say.
func report(runs []store.Run, stepsPerRun int) error {
	var first, last time.Time
	latencies := make([]time.Duration, 0, len(runs))
	for _, r := range runs {
		if r.Status != store.RunSucceeded || r.FinishedAt == nil {
			return fmt.Errorf("run %s finished as %s, want succeeded", r.ID, r.Status)
		}
		if first.IsZero() || r.CreatedAt.Before(first) {
			first = r.CreatedAt
		}
		if r.FinishedAt.After(last) {
			last = *r.FinishedAt
		}
		latencies = append(latencies, r.FinishedAt.Sub(r.CreatedAt))
	}
	slices.Sort(latencies)
	wall := last.Sub(first)
	ms := func(d time.Duration) time.Duration { return d.Round(time.Millisecond) }

	fmt.Printf("all %d runs succeeded\n", len(runs))
	fmt.Printf("wall time:   %v (first created_at to last finished_at, database clock)\n", ms(wall))
	if wall > 0 {
		fmt.Printf("throughput:  %.1f steps/s\n", float64(len(runs)*stepsPerRun)/wall.Seconds())
	}
	fmt.Printf("run latency: p50 %v, p95 %v, p99 %v (created_at to finished_at)\n",
		ms(percentile(latencies, 50)), ms(percentile(latencies, 95)), ms(percentile(latencies, 99)))
	return nil
}

// percentile returns the nearest-rank p-th percentile of sorted values: the
// smallest value that at least p% of the values are less than or equal to.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	return sorted[max(rank, 1)-1]
}

// describeEnv returns one line about the Go runtime and the PostgreSQL
// server, including the settings that most affect a commit-heavy workload.
func describeEnv(ctx context.Context, conn *pgx.Conn) (string, error) {
	var version, syncCommit, fsync, sharedBuffers string
	err := conn.QueryRow(ctx, `SELECT current_setting('server_version'), current_setting('synchronous_commit'),
		current_setting('fsync'), current_setting('shared_buffers')`).Scan(&version, &syncCommit, &fsync, &sharedBuffers)
	if err != nil {
		return "", fmt.Errorf("reading server settings: %w", err)
	}
	return fmt.Sprintf("%s %s/%s, GOMAXPROCS=%d; PostgreSQL %s (synchronous_commit=%s, fsync=%s, shared_buffers=%s)",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), version, syncCommit, fsync, sharedBuffers), nil
}

// withSearchPath adds search_path=schema to a connection string, in URL or
// keyword/value form. pgx sends settings it does not know itself to the
// server as session parameters, so every pooled connection uses the schema.
func withSearchPath(dbURL, schema string) string {
	u, err := url.Parse(dbURL)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return dbURL + " search_path=" + schema // "host=... dbname=..." form
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
