package pgstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// CreateWorkflow stores def. It validates def again: that is cheap, applies
// the defaults, and guarantees that every stored definition is a DAG that
// CreateRun can put in order.
func (s *Store) CreateWorkflow(ctx context.Context, def workflow.Definition) (store.Workflow, error) {
	def.Steps = slices.Clone(def.Steps) // Validate writes defaults into the steps; keep them out of the caller's slice
	if _, err := workflow.Validate(&def); err != nil {
		return store.Workflow{}, err
	}
	body, err := json.Marshal(def)
	if err != nil {
		return store.Workflow{}, fmt.Errorf("pgstore: encode workflow: %w", err)
	}
	w := store.Workflow{Name: def.Name, Definition: def}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO workflows (name, definition) VALUES ($1, $2) RETURNING id, created_at`,
		def.Name, body).Scan(&w.ID, &w.CreatedAt)
	if err != nil {
		return store.Workflow{}, fmt.Errorf("pgstore: create workflow: %w", err)
	}
	return w, nil
}

// GetWorkflow returns the stored workflow, or store.ErrNotFound.
func (s *Store) GetWorkflow(ctx context.Context, id string) (store.Workflow, error) {
	if !isUUID(id) {
		return store.Workflow{}, store.ErrNotFound
	}
	var w store.Workflow
	var body []byte
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, definition, created_at FROM workflows WHERE id = $1`,
		id).Scan(&w.ID, &w.Name, &body, &w.CreatedAt)
	if err != nil {
		return store.Workflow{}, notFound(err)
	}
	if err := json.Unmarshal(body, &w.Definition); err != nil {
		return store.Workflow{}, fmt.Errorf("pgstore: decode workflow %s: %w", id, err)
	}
	return w, nil
}

// CreateRun snapshots the workflow's steps into a new run, in one transaction.
// Each step row keeps its own copy of the step definition, so a run is not
// affected by anything that happens to the workflow later.
func (s *Store) CreateRun(ctx context.Context, workflowID string, input json.RawMessage) (store.Run, error) {
	if !isUUID(workflowID) {
		return store.Run{}, store.ErrNotFound
	}
	var run store.Run
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var body []byte
		err := tx.QueryRow(ctx, `SELECT definition FROM workflows WHERE id = $1`, workflowID).Scan(&body)
		if err != nil {
			return err
		}
		var def workflow.Definition
		if err := json.Unmarshal(body, &def); err != nil {
			return fmt.Errorf("decode workflow %s: %w", workflowID, err)
		}
		// Validate again only for the topological order; the stored
		// definition is already valid.
		order, err := workflow.Validate(&def)
		if err != nil {
			return fmt.Errorf("stored workflow %s is invalid: %w", workflowID, err)
		}

		run, err = scanRun(tx.QueryRow(ctx,
			`INSERT INTO runs (workflow_id, input) VALUES ($1, $2) RETURNING `+runColumns,
			workflowID, nullJSON(input)))
		if err != nil {
			return err
		}

		byID := make(map[string]workflow.Step, len(def.Steps))
		for _, st := range def.Steps {
			byID[st.ID] = st
		}
		// One round trip for all the step rows.
		batch := &pgx.Batch{}
		for pos, id := range order {
			st := byID[id]
			status := store.StepPending
			if len(st.DependsOn) == 0 {
				status = store.StepReady
			}
			deps := st.DependsOn
			if deps == nil {
				deps = []string{} // the column is NOT NULL; a nil slice would be sent as NULL
			}
			stepDef, err := json.Marshal(st)
			if err != nil {
				return fmt.Errorf("encode step %s: %w", id, err)
			}
			batch.Queue(`INSERT INTO steps (run_id, step_id, position, status, depends_on, max_attempts, step_def)
				VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				run.ID, st.ID, pos, string(status), deps, st.Retry.MaxAttempts, stepDef)
		}
		return tx.SendBatch(ctx, batch).Close()
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Run{}, store.ErrNotFound
	}
	if err != nil {
		return store.Run{}, fmt.Errorf("pgstore: create run: %w", err)
	}
	return run, nil
}

// runColumns are the columns scanRun reads, in its order.
const runColumns = `id, workflow_id, status, input, created_at, finished_at`

func scanRun(row pgx.Row) (store.Run, error) {
	var r store.Run
	var input []byte
	err := row.Scan(&r.ID, &r.WorkflowID, &r.Status, &input, &r.CreatedAt, &r.FinishedAt)
	r.Input = input
	return r, err
}

func scanStep(row pgx.CollectableRow) (store.StepState, error) {
	var st store.StepState
	var output []byte
	err := row.Scan(&st.StepID, &st.Status, &st.Attempt, &st.MaxAttempts, &st.DependsOn,
		&st.LastError, &output, &st.StartedAt, &st.FinishedAt)
	st.Output = output
	return st, err
}

// GetRun returns the run with its steps in topological order.
func (s *Store) GetRun(ctx context.Context, id string) (store.Run, error) {
	if !isUUID(id) {
		return store.Run{}, store.ErrNotFound
	}
	var run store.Run
	// Read the run and its steps from one snapshot, so they agree with each
	// other (for example, no "running" run whose steps have all finished).
	opts := pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
	err := pgx.BeginTxFunc(ctx, s.pool, opts, func(tx pgx.Tx) error {
		var err error
		run, err = scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs WHERE id = $1`, id))
		if err != nil {
			return err
		}
		// pgx reports a Query error through the rows, so CollectRows returns it.
		rows, _ := tx.Query(ctx, `
			SELECT step_id, status, attempt, max_attempts, depends_on, COALESCE(last_error, ''),
			       output, started_at, finished_at
			FROM steps WHERE run_id = $1 ORDER BY position`, id)
		run.Steps, err = pgx.CollectRows(rows, scanStep)
		return err
	})
	if err != nil {
		return store.Run{}, notFound(err)
	}
	return run, nil
}

// ListRuns returns runs newest first, using keyset pagination: the cursor
// holds the (created_at, id) of the last run returned, and the next page
// starts strictly after it. Unlike OFFSET, pages stay stable while new runs
// are being created, and the index serves every page equally fast.
func (s *Store) ListRuns(ctx context.Context, f store.ListRunsFilter) ([]store.Run, string, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 20
	}
	limit = min(limit, 100)

	// nil arguments are sent as NULL, which switches that filter off.
	var workflowID, status, afterTime, afterID any
	if f.WorkflowID != "" {
		if !isUUID(f.WorkflowID) {
			return []store.Run{}, "", nil // no run can belong to it
		}
		workflowID = f.WorkflowID
	}
	if f.Status != "" {
		status = string(f.Status)
	}
	if f.Cursor != "" {
		t, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return nil, "", store.ErrInvalidCursor
		}
		afterTime, afterID = t, id
	}

	// Fetch one extra row to learn whether another page exists.
	rows, _ := s.pool.Query(ctx, `
		SELECT `+runColumns+` FROM runs
		WHERE ($1::uuid IS NULL OR workflow_id = $1)
		  AND ($2::text IS NULL OR status = $2)
		  AND ($3::timestamptz IS NULL OR (created_at, id) < ($3, $4::uuid))
		ORDER BY created_at DESC, id DESC
		LIMIT $5`,
		workflowID, status, afterTime, afterID, limit+1)
	runs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Run, error) { return scanRun(row) })
	if err != nil {
		return nil, "", fmt.Errorf("pgstore: list runs: %w", err)
	}
	next := ""
	if len(runs) > limit {
		runs = runs[:limit]
		last := runs[limit-1]
		next = encodeCursor(last.CreatedAt, last.ID)
	}
	return runs, next, nil
}

// encodeCursor packs a run's position in the list. PostgreSQL timestamps
// have microsecond precision, so Unix microseconds round-trip exactly.
func encodeCursor(createdAt time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(createdAt.UnixMicro(), 10) + "," + id))
}

func decodeCursor(c string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, "", err
	}
	micros, id, ok := strings.Cut(string(raw), ",")
	if !ok || !isUUID(id) {
		return time.Time{}, "", store.ErrInvalidCursor
	}
	n, err := strconv.ParseInt(micros, 10, 64)
	if err != nil {
		return time.Time{}, "", err
	}
	return time.UnixMicro(n), id, nil
}

// CancelRun cancels a running run. Its pending and ready steps are
// cancelled, and so are its running steps: their workers' fenced writes now
// fail, and their next Heartbeat returns store.ErrRunCancelled, so they stop
// and discard the result. Nothing is left half-finished.
func (s *Store) CancelRun(ctx context.Context, id string) (store.Run, error) {
	if !isUUID(id) {
		return store.Run{}, store.ErrNotFound
	}
	var run store.Run
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		status, err := lockRun(ctx, tx, id)
		if err != nil {
			return err
		}
		switch status {
		case store.RunCancelled:
			// Already cancelled: return it unchanged.
		case store.RunRunning:
			if _, err := tx.Exec(ctx, `
				UPDATE steps SET status = 'cancelled', finished_at = now(),
				                 lease_owner = NULL, lease_expires_at = NULL
				WHERE run_id = $1 AND status IN ('pending', 'ready', 'running')`, id); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`UPDATE runs SET status = 'cancelled', finished_at = now() WHERE id = $1`, id); err != nil {
				return err
			}
		default:
			return store.ErrConflict // it already succeeded or failed
		}
		run, err = scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs WHERE id = $1`, id))
		return err
	})
	if err != nil {
		return store.Run{}, notFound(err)
	}
	return run, nil
}
