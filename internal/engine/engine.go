// Package engine runs claimed steps with a pool of worker goroutines.
//
// Each worker loops: claim a runnable step from the store, execute it under
// a per-step timeout while a heartbeat goroutine keeps its lease alive, then
// record the outcome (complete, retry later, or fail for good). The store
// fences every write with the claim's (owner, attempt), so a worker that lost
// its lease can never overwrite the work of the worker that took over.
//
// Delivery is at-least-once: a step whose worker dies is run again once its
// lease expires, which is why the http executor sends an Idempotency-Key.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/metrics"
	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// Executor runs one attempt of one step. It must respect ctx (the step's
// timeout, lease loss, run cancellation and shutdown all cancel it).
type Executor interface {
	Execute(ctx context.Context, c *store.Claim) (json.RawMessage, error)
}

// permanentError marks an error that retrying cannot fix.
type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent wraps err so the engine fails the step without further retries
// (for example an HTTP 400 from the target).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// Config tunes the engine. Zero values get the defaults noted.
type Config struct {
	Workers       int           // concurrent step executions; default 8
	Lease         time.Duration // claim lease; default 30s (heartbeat every Lease/3)
	Poll          time.Duration // idle wait between empty claims, jittered; default 200ms
	ShutdownGrace time.Duration // how long in-flight steps may finish on shutdown; default 10s
	Owner         string        // lease owner ID; default "<hostname>-<pid>-<random>"
}

// Engine claims runnable steps from the store and executes them.
type Engine struct {
	cfg   Config
	store store.Store
	execs map[workflow.StepType]Executor
	m     *metrics.Metrics
	log   *slog.Logger
	wake  chan struct{}
}

// Defaults for zero Config fields.
const (
	defaultWorkers       = 8
	defaultLease         = 30 * time.Second
	defaultPoll          = 200 * time.Millisecond
	defaultShutdownGrace = 10 * time.Second
)

// withDefaults fills in the zero (or negative) fields of c.
func (c Config) withDefaults() Config {
	if c.Workers <= 0 {
		c.Workers = defaultWorkers
	}
	if c.Lease <= 0 {
		c.Lease = defaultLease
	}
	if c.Poll <= 0 {
		c.Poll = defaultPoll
	}
	if c.ShutdownGrace <= 0 {
		c.ShutdownGrace = defaultShutdownGrace
	}
	if c.Owner == "" {
		c.Owner = defaultOwner()
	}
	return c
}

// defaultOwner builds a lease owner ID that is unique per engine. The random
// suffix matters: two engines in one process share a pid, and in containers
// every process is often pid 1.
func defaultOwner() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d-%08x", host, os.Getpid(), rand.Uint32())
}

// New builds an engine. execs maps each step type to its executor; a step
// whose type has no executor fails permanently. m and log may be nil.
func New(cfg Config, st store.Store, execs map[workflow.StepType]Executor, m *metrics.Metrics, log *slog.Logger) *Engine {
	cfg = cfg.withDefaults()
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("owner", cfg.Owner)
	return &Engine{cfg: cfg, store: st, execs: execs, m: m, log: log, wake: make(chan struct{}, 1)}
}

// errShutdown is the cancellation cause of steps that were still running
// when the shutdown grace period ran out.
var errShutdown = errors.New("engine shut down before the step finished")

// Run starts the workers and blocks until ctx is cancelled, then shuts down
// gracefully: it stops claiming, gives in-flight steps up to ShutdownGrace to
// finish, then cancels them (their leases simply expire).
//
// Run returns nil once every worker goroutine has exited. Call it once.
func (e *Engine) Run(ctx context.Context) error {
	// Steps run under stepsCtx rather than ctx, so cancelling ctx only stops
	// new claims. WithoutCancel keeps ctx's values but not its cancellation;
	// stepsCtx is cancelled by us when the grace period is over.
	stepsCtx, cancelSteps := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelSteps(nil)

	e.log.Info("engine started", "workers", e.cfg.Workers, "lease", e.cfg.Lease, "poll", e.cfg.Poll)
	var wg sync.WaitGroup
	for range e.cfg.Workers {
		wg.Go(func() { e.work(ctx, stepsCtx) })
	}

	<-ctx.Done()
	e.log.Info("engine stopping", "grace", e.cfg.ShutdownGrace)
	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()
	grace := time.NewTimer(e.cfg.ShutdownGrace)
	defer grace.Stop()
	select {
	case <-drained:
	case <-grace.C:
		e.log.Warn("shutdown grace period over, cancelling in-flight steps")
		cancelSteps(errShutdown)
		<-drained
	}
	e.log.Info("engine stopped")
	return nil
}

// Wake nudges idle workers, e.g. right after a run is created. Never blocks.
func (e *Engine) Wake() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// Backoff returns the delay before the retry that follows failed attempt
// number `attempt` (1-based), using exponential backoff with full jitter:
// a uniform random value in [0, min(max, initial*2^(attempt-1))].
// rnd returns a float64 in [0, 1).
//
// Full jitter spreads retries of many failed steps evenly over the window
// instead of having them all hit the target again at the same moment.
func Backoff(attempt int, initial, max time.Duration, rnd func() float64) time.Duration {
	if initial <= 0 || max <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	// ceiling = min(max, initial * 2^(attempt-1)). Doubling is only done
	// while the result stays <= max, so it can never overflow, even for a
	// huge attempt: max>>shift is 0 once shift reaches 63.
	ceiling := max
	if shift := attempt - 1; initial <= max>>shift {
		ceiling = initial << shift
	}
	return time.Duration(rnd() * float64(ceiling))
}
