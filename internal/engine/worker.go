package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// maxErrorBackoff caps how long a worker waits after consecutive store errors.
const maxErrorBackoff = 5 * time.Second

// Step results, used as the "result" label of flowd_steps_executed_total.
const (
	resultSuccess   = "success"   // completed
	resultRetry     = "retry"     // failed, will run again after a backoff
	resultFailure   = "failure"   // failed for good (permanent error or out of attempts)
	resultLost      = "lost"      // lease lost, or the outcome could not be saved
	resultCancelled = "cancelled" // run cancelled, or engine shut down mid-step
)

// work is one worker goroutine: claim a step, run it, repeat. It stops
// claiming as soon as ctx is done. stepsCtx is only cancelled when the
// shutdown grace period is over, so a step that is already running can finish.
func (e *Engine) work(ctx, stepsCtx context.Context) {
	storeErrors := 0 // consecutive failed claims, for the backoff
	for ctx.Err() == nil {
		c, err := e.store.ClaimStep(stepsCtx, e.cfg.Owner, e.cfg.Lease)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return // stopping anyway, so there is nothing to back off for
			}
			storeErrors++
			e.m.ClaimError()
			e.log.Error("claim failed", "err", err, "consecutive_errors", storeErrors)
			// Back off like a step retry so a database outage does not
			// become a hot loop of failing queries.
			sleep(ctx, Backoff(storeErrors, e.cfg.Poll, maxErrorBackoff, rand.Float64))
		case c == nil:
			storeErrors = 0
			e.idle(ctx)
		default:
			storeErrors = 0
			// There may be more runnable steps (a fan-out, a burst of new
			// runs): let one more idle worker look instead of waiting a poll.
			e.Wake()
			e.runStep(stepsCtx, c)
		}
	}
}

// idle waits about one poll interval, or less if Wake is called. The ±20%
// jitter keeps many workers and engines from polling the database in lockstep.
func (e *Engine) idle(ctx context.Context) {
	d := time.Duration(float64(e.cfg.Poll) * (0.8 + 0.4*rand.Float64()))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-e.wake:
	case <-t.C:
	}
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// runStep executes one claimed step and records its outcome in the store.
func (e *Engine) runStep(stepsCtx context.Context, c *store.Claim) {
	log := e.log.With("run_id", c.RunID, "step_id", c.StepID, "attempt", c.Attempt)
	typ := string(c.Step.Type)
	start := time.Now()

	// stepCtx is cancelled, with the reason as its cause, when the heartbeat
	// finds the lease lost or the run cancelled, or when shutdown runs out of
	// grace (through stepsCtx).
	stepCtx, cancelStep := context.WithCancelCause(stepsCtx)
	defer cancelStep(nil)

	stopHeartbeat := e.startHeartbeat(stepCtx, c, cancelStep, log)
	out, err := e.execute(stepCtx, c)
	stopHeartbeat()
	took := time.Since(start)

	if cause := context.Cause(stepCtx); cause != nil {
		// Someone else may own the step now, the run no longer wants it, or
		// we are shutting down (the lease expiring is what gets the step run
		// again). In every case the result must not be written.
		result := resultCancelled
		if errors.Is(cause, store.ErrLeaseLost) {
			result = resultLost
		}
		e.m.StepExecuted(typ, result, took)
		log.Info("step abandoned, result discarded", "reason", cause)
		return
	}

	result, finished, serr := e.record(stepsCtx, c, out, err)
	switch {
	case errors.Is(serr, store.ErrLeaseLost):
		result = resultLost
		log.Info("lease lost before the result was saved, result discarded")
	case errors.Is(serr, store.ErrRunCancelled):
		result = resultCancelled
		log.Info("run cancelled before the result was saved, result discarded")
	case serr != nil:
		// The outcome is unknown to the store. The step stays leased to us
		// until the lease expires, then runs again (at-least-once).
		result = resultLost
		log.Error("could not save step result", "err", serr)
	case result == resultSuccess:
		log.Debug("step succeeded", "took", took)
	case result == resultRetry:
		log.Info("step failed, will retry", "err", err)
	default:
		log.Warn("step failed", "err", err)
	}
	e.m.StepExecuted(typ, result, took)
	if finished != "" {
		e.m.RunFinished(string(finished))
		log.Info("run finished", "status", finished)
	}
}

// execute runs the step's executor under the step's timeout. A timeout is
// reported as an ordinary (retryable) failure.
func (e *Engine) execute(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
	ex, ok := e.execs[c.Step.Type]
	if !ok {
		return nil, Permanent(fmt.Errorf("no executor for step type %q", c.Step.Type))
	}
	timeout := c.Step.Timeout()
	if timeout <= 0 {
		timeout = workflow.DefaultTimeout // defensive: Validate normally fills it in
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	out, err := ex.Execute(ctx, c)
	if err != nil && errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		err = fmt.Errorf("timed out after %s: %w", timeout, err)
	}
	return out, err
}

// record saves the outcome of an attempt: complete on success; otherwise
// retry after a backoff while attempts remain and the error is not
// permanent; otherwise fail the step for good. It returns the step's result
// label and, when this write is what finished the run, the run's status.
func (e *Engine) record(ctx context.Context, c *store.Claim, out json.RawMessage, err error) (result string, finished store.RunStatus, serr error) {
	var status store.RunStatus
	switch r := c.Step.Retry; {
	case err == nil:
		result = resultSuccess
		status, serr = e.store.CompleteStep(ctx, c, out)
	case c.Attempt < r.MaxAttempts && !IsPermanent(err):
		result = resultRetry
		retryAt := time.Now().Add(Backoff(c.Attempt, r.InitialBackoff(), r.MaxBackoff(), rand.Float64))
		status, serr = e.store.FailStep(ctx, c, err.Error(), &retryAt)
	default:
		result = resultFailure
		status, serr = e.store.FailStep(ctx, c, err.Error(), nil)
	}
	switch {
	case serr != nil:
		return result, "", serr
	case result == resultRetry && status.Finished():
		// The run had already finished (for example, another step failed for
		// good), so the store failed this step instead of scheduling a retry.
		return resultFailure, "", nil
	case result == resultSuccess && status == store.RunSucceeded,
		result == resultFailure && status == store.RunFailed:
		// Only the last step's completion makes a run succeed, and a step
		// failing for good makes it fail. The store reports the status after
		// the write, not whether the write changed it, so a run in which two
		// steps fail for good is counted twice. Cancelled runs are counted
		// where they are cancelled, since the engine may never see them.
		return result, status, nil
	}
	return result, "", nil
}

// startHeartbeat renews c's lease every Lease/3 until the returned stop
// function is called. Renewing well before expiry means one slow or failed
// heartbeat does not lose the lease. If the store says the lease is lost or
// the run was cancelled, it cancels the step with that error as the cause.
// stop waits for the goroutine to exit, so no heartbeat outlives its step.
func (e *Engine) startHeartbeat(stepCtx context.Context, c *store.Claim, cancelStep context.CancelCauseFunc, log *slog.Logger) (stop func()) {
	ctx, cancel := context.WithCancel(stepCtx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(max(e.cfg.Lease/3, time.Millisecond))
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			_, err := e.store.Heartbeat(ctx, c, e.cfg.Lease)
			switch {
			case errors.Is(err, store.ErrLeaseLost), errors.Is(err, store.ErrRunCancelled):
				cancelStep(err)
				return
			case err != nil && ctx.Err() == nil:
				// Probably transient; the lease still has time left, so try
				// again at the next tick. Fencing keeps this safe even if the
				// lease does run out meanwhile.
				log.Warn("heartbeat failed", "err", err)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
