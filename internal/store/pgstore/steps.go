package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kaustubhagarwal21/flowd/internal/store"
)

// claimSQL leases one runnable step in a single statement. The inner SELECT
// picks the step that has been runnable the longest, and FOR UPDATE SKIP
// LOCKED makes concurrent claimers pass over a row that another claimer is
// taking instead of waiting for it, so two workers never get the same step.
//
// A step is runnable when its run is running and either it is ready and due,
// or it is running with an expired lease (its worker died) and has attempts
// left. Each claim bumps attempt, which makes (lease_owner, attempt) a fencing
// token: the previous owner's late writes no longer match.
const claimSQL = `
UPDATE steps AS s
SET status           = 'running',
    attempt          = s.attempt + 1,
    lease_owner      = $1,
    lease_expires_at = now() + $2 * interval '1 second',
    started_at       = COALESCE(s.started_at, now())
FROM (
    SELECT st.run_id, st.step_id
    FROM steps AS st
    JOIN runs AS r ON r.id = st.run_id
    WHERE r.status = 'running'
      AND (   (st.status = 'ready' AND st.not_before <= now())
           OR (st.status = 'running' AND st.lease_expires_at < now() AND st.attempt < st.max_attempts))
    ORDER BY st.not_before, st.position
    LIMIT 1
    FOR UPDATE OF st SKIP LOCKED
) AS picked
JOIN runs AS run ON run.id = picked.run_id
WHERE s.run_id = picked.run_id AND s.step_id = picked.step_id
RETURNING s.run_id, s.step_id, s.attempt, s.lease_expires_at, s.step_def, run.input`

// ClaimStep leases one runnable step to owner for lease, or returns (nil,
// nil) when nothing is runnable.
func (s *Store) ClaimStep(ctx context.Context, owner string, lease time.Duration) (*store.Claim, error) {
	if err := s.failDeadSteps(ctx); err != nil {
		return nil, err
	}
	c := &store.Claim{Owner: owner}
	var stepDef, input []byte
	err := s.pool.QueryRow(ctx, claimSQL, owner, lease.Seconds()).
		Scan(&c.RunID, &c.StepID, &c.Attempt, &c.LeaseExpiresAt, &stepDef, &input)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pgstore: claim: %w", err)
	}
	if err := json.Unmarshal(stepDef, &c.Step); err != nil {
		return nil, fmt.Errorf("pgstore: decode step %s/%s: %w", c.RunID, c.StepID, err)
	}
	c.Input = input
	return c, nil
}

// deadStep is a running step whose lease expired and that may not run again.
type deadStep struct {
	RunID   string
	StepID  string
	Attempt int
}

// failDeadSteps fails the running steps whose lease expired but that must
// not be handed out again: they were on their last attempt, or their run has
// already failed. Each one fails like a permanent FailStep, in its own
// transaction. The UPDATE checks the step again under the run lock, because
// another claimer may have failed it already, or its worker may have renewed
// the lease just in time.
func (s *Store) failDeadSteps(ctx context.Context) error {
	rows, _ := s.pool.Query(ctx, `
		SELECT st.run_id, st.step_id, st.attempt
		FROM steps AS st
		JOIN runs AS r ON r.id = st.run_id
		WHERE st.status = 'running' AND st.lease_expires_at < now()
		  AND (st.attempt >= st.max_attempts OR r.status <> 'running')`)
	dead, err := pgx.CollectRows(rows, pgx.RowToStructByPos[deadStep])
	if err != nil {
		return fmt.Errorf("pgstore: find expired steps: %w", err)
	}
	for _, d := range dead {
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			if _, err := lockRun(ctx, tx, d.RunID); err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `
				UPDATE steps SET status = 'failed', last_error = 'lease expired: the worker was lost',
				                 finished_at = now(), lease_owner = NULL, lease_expires_at = NULL
				WHERE run_id = $1 AND step_id = $2 AND attempt = $3
				  AND status = 'running' AND lease_expires_at < now()`,
				d.RunID, d.StepID, d.Attempt)
			if err != nil || tag.RowsAffected() == 0 {
				return err
			}
			return failRun(ctx, tx, d.RunID)
		})
		if err != nil {
			return fmt.Errorf("pgstore: fail expired step %s/%s: %w", d.RunID, d.StepID, err)
		}
	}
	return nil
}

// Heartbeat extends c's lease and returns the new expiry. It returns
// store.ErrRunCancelled if the run was cancelled and store.ErrLeaseLost if c
// no longer holds the step for any other reason.
func (s *Store) Heartbeat(ctx context.Context, c *store.Claim, lease time.Duration) (time.Time, error) {
	var expires time.Time
	err := s.pool.QueryRow(ctx, `
		UPDATE steps SET lease_expires_at = now() + $5 * interval '1 second'
		WHERE run_id = $1 AND step_id = $2 AND lease_owner = $3 AND attempt = $4 AND status = 'running'
		RETURNING lease_expires_at`,
		c.RunID, c.StepID, c.Owner, c.Attempt, lease.Seconds()).Scan(&expires)
	if err == nil {
		return expires, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, fmt.Errorf("pgstore: heartbeat: %w", err)
	}

	// Fenced out. CancelRun also cancels running steps, so check whether
	// that is why: the worker should stop either way, but the reason is
	// worth reporting.
	var status store.RunStatus
	err = s.pool.QueryRow(ctx, `SELECT status FROM runs WHERE id = $1`, c.RunID).Scan(&status)
	switch {
	case err != nil && !errors.Is(err, pgx.ErrNoRows):
		return time.Time{}, fmt.Errorf("pgstore: heartbeat: %w", err)
	case status == store.RunCancelled:
		return time.Time{}, store.ErrRunCancelled
	default:
		return time.Time{}, store.ErrLeaseLost
	}
}

// CompleteStep marks c's step succeeded. In the same transaction it promotes
// the dependents whose dependencies have now all succeeded, and finishes the
// run when every step has succeeded.
func (s *Store) CompleteStep(ctx context.Context, c *store.Claim, output json.RawMessage) (store.RunStatus, error) {
	var status store.RunStatus
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		if status, err = lockRun(ctx, tx, c.RunID); err != nil {
			return fencedOut(err)
		}
		tag, err := tx.Exec(ctx, `
			UPDATE steps SET status = 'succeeded', output = $5, finished_at = now(),
			                 lease_owner = NULL, lease_expires_at = NULL
			WHERE run_id = $1 AND step_id = $2 AND lease_owner = $3 AND attempt = $4 AND status = 'running'`,
			c.RunID, c.StepID, c.Owner, c.Attempt, outputJSON(output))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return store.ErrLeaseLost
		}
		if status != store.RunRunning {
			return nil // the run already failed, so it starts nothing new
		}

		if _, err := tx.Exec(ctx, `
			UPDATE steps AS d SET status = 'ready', not_before = now()
			WHERE d.run_id = $1 AND d.status = 'pending' AND $2 = ANY (d.depends_on)
			  AND NOT EXISTS (
			      SELECT 1 FROM steps AS dep
			      WHERE dep.run_id = d.run_id AND dep.step_id = ANY (d.depends_on)
			        AND dep.status <> 'succeeded')`,
			c.RunID, c.StepID); err != nil {
			return err
		}

		tag, err = tx.Exec(ctx, `
			UPDATE runs SET status = 'succeeded', finished_at = now()
			WHERE id = $1 AND NOT EXISTS (
			    SELECT 1 FROM steps WHERE run_id = $1 AND status <> 'succeeded')`,
			c.RunID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			status = store.RunSucceeded
		}
		return nil
	})
	if err != nil {
		return "", stepError("complete step", err)
	}
	return status, nil
}

// FailStep records a failed attempt of c's step. With retryAt set, and
// attempts left, the step goes back to ready and becomes claimable at
// retryAt. Otherwise it fails for good, and so does the run (see failRun).
func (s *Store) FailStep(ctx context.Context, c *store.Claim, errMsg string, retryAt *time.Time) (store.RunStatus, error) {
	errMsg = cleanText(errMsg)
	var status store.RunStatus
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		if status, err = lockRun(ctx, tx, c.RunID); err != nil {
			return fencedOut(err)
		}
		var maxAttempts int
		err = tx.QueryRow(ctx, `
			SELECT max_attempts FROM steps
			WHERE run_id = $1 AND step_id = $2 AND lease_owner = $3 AND attempt = $4 AND status = 'running'
			FOR UPDATE`,
			c.RunID, c.StepID, c.Owner, c.Attempt).Scan(&maxAttempts)
		if err != nil {
			return fencedOut(err)
		}

		// Retry only while attempts remain and the run can still use the
		// result. The store enforces this itself, so attempt never exceeds
		// max_attempts whatever the caller asks for.
		if retryAt != nil && c.Attempt < maxAttempts && status == store.RunRunning {
			_, err := tx.Exec(ctx, `
				UPDATE steps SET status = 'ready', not_before = $3, last_error = $4,
				                 lease_owner = NULL, lease_expires_at = NULL
				WHERE run_id = $1 AND step_id = $2`,
				c.RunID, c.StepID, *retryAt, errMsg)
			return err
		}

		if _, err := tx.Exec(ctx, `
			UPDATE steps SET status = 'failed', last_error = $3, finished_at = now(),
			                 lease_owner = NULL, lease_expires_at = NULL
			WHERE run_id = $1 AND step_id = $2`,
			c.RunID, c.StepID, errMsg); err != nil {
			return err
		}
		status = store.RunFailed
		return failRun(ctx, tx, c.RunID)
	})
	if err != nil {
		return "", stepError("fail step", err)
	}
	return status, nil
}

// failRun fails a run after one of its steps failed for good. It skips every
// step that has not started yet. That covers the failed step's transitive
// dependents, which can never run now, and also the steps of independent
// branches: a failed run is finished, so it starts nothing new (fail-fast).
// Steps that are already running may finish, and their result is recorded.
func failRun(ctx context.Context, tx pgx.Tx, runID string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE steps SET status = 'skipped', finished_at = now()
		WHERE run_id = $1 AND status IN ('pending', 'ready')`, runID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE runs SET status = 'failed', finished_at = now()
		WHERE id = $1 AND status = 'running'`, runID)
	return err
}

// lockRun locks the run's row until the transaction ends and returns the
// run's status. The package comment explains why.
func lockRun(ctx context.Context, tx pgx.Tx, runID string) (store.RunStatus, error) {
	var status store.RunStatus
	err := tx.QueryRow(ctx, `SELECT status FROM runs WHERE id = $1 FOR UPDATE`, runID).Scan(&status)
	return status, err
}

// fencedOut maps a missing row (no such run, or the fenced step row did not
// match) to store.ErrLeaseLost.
func fencedOut(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrLeaseLost
	}
	return err
}

// stepError returns store.ErrLeaseLost as it is, so callers can compare it
// directly, and adds context to any other error.
func stepError(op string, err error) error {
	if errors.Is(err, store.ErrLeaseLost) {
		return store.ErrLeaseLost
	}
	return fmt.Errorf("pgstore: %s: %w", op, err)
}
