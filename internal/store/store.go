// Package store defines how flowd persists workflows, runs and step state.
//
// The only implementation is pgstore (PostgreSQL). The interface exists so
// the engine and API can be unit-tested with small fakes.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// RunStatus is the state of a whole run.
type RunStatus string

const (
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
)

// Finished reports whether the run can no longer change.
func (s RunStatus) Finished() bool { return s != RunRunning }

// StepStatus is the state of one step within a run.
type StepStatus string

const (
	StepPending   StepStatus = "pending"   // waiting for its dependencies
	StepReady     StepStatus = "ready"     // runnable once not_before has passed
	StepRunning   StepStatus = "running"   // leased by a worker
	StepSucceeded StepStatus = "succeeded" // done
	StepFailed    StepStatus = "failed"    // out of attempts, or a permanent error
	StepSkipped   StepStatus = "skipped"   // an upstream step failed
	StepCancelled StepStatus = "cancelled" // the run was cancelled before it ran
)

// Terminal reports whether the step can no longer change.
func (s StepStatus) Terminal() bool {
	switch s {
	case StepSucceeded, StepFailed, StepSkipped, StepCancelled:
		return true
	}
	return false
}

// Workflow is a stored, validated definition.
type Workflow struct {
	ID         string              `json:"id"`
	Name       string              `json:"name"`
	Definition workflow.Definition `json:"definition"`
	CreatedAt  time.Time           `json:"created_at"`
}

// Run is one execution of a workflow.
type Run struct {
	ID         string          `json:"id"`
	WorkflowID string          `json:"workflow_id"`
	Status     RunStatus       `json:"status"`
	Input      json.RawMessage `json:"input,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
	// Steps is filled by GetRun only, in the workflow's topological order.
	Steps []StepState `json:"steps,omitempty"`
}

// StepState is the observable state of one step of a run.
type StepState struct {
	StepID      string          `json:"step_id"`
	Status      StepStatus      `json:"status"`
	Attempt     int             `json:"attempt"`
	MaxAttempts int             `json:"max_attempts"`
	DependsOn   []string        `json:"depends_on,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	FinishedAt  *time.Time      `json:"finished_at,omitempty"`
}

// Claim is a leased step handed to exactly one worker. (Owner, Attempt) is
// the fencing token: every later write for this step must present it.
type Claim struct {
	RunID          string
	StepID         string
	Attempt        int // 1-based; incremented by every claim, including re-claims after a crash
	Owner          string
	LeaseExpiresAt time.Time
	Step           workflow.Step   // the step's definition, defaults applied
	Input          json.RawMessage // the run's input
}

// ListRunsFilter selects and pages runs for ListRuns.
type ListRunsFilter struct {
	WorkflowID string    // optional
	Status     RunStatus // optional
	Limit      int       // 1..100; 0 means 20
	Cursor     string    // opaque; NextCursor from the previous page
}

var (
	// ErrNotFound: no workflow or run with that ID.
	ErrNotFound = errors.New("not found")
	// ErrLeaseLost: the claim no longer holds the step (fenced out, or the
	// step was re-claimed after the lease expired). Discard the result.
	ErrLeaseLost = errors.New("lease lost")
	// ErrRunCancelled: returned by Heartbeat when the run was cancelled; the
	// worker must stop the step and discard its result.
	ErrRunCancelled = errors.New("run cancelled")
	// ErrConflict: the operation does not apply in the current state,
	// e.g. cancelling a run that already finished.
	ErrConflict = errors.New("conflict")
	// ErrInvalidCursor: the pagination cursor is malformed.
	ErrInvalidCursor = errors.New("invalid cursor")
)

// Store persists everything. All methods are safe for concurrent use, and
// several flowd processes may share one database.
type Store interface {
	// CreateWorkflow stores an already-validated definition.
	CreateWorkflow(ctx context.Context, def workflow.Definition) (Workflow, error)
	GetWorkflow(ctx context.Context, id string) (Workflow, error)

	// CreateRun snapshots the workflow's steps into a new run. Steps without
	// dependencies start ready; the rest start pending.
	CreateRun(ctx context.Context, workflowID string, input json.RawMessage) (Run, error)
	// GetRun returns the run with its steps.
	GetRun(ctx context.Context, id string) (Run, error)
	// ListRuns returns runs newest first (without steps) and a cursor for
	// the next page ("" when there is none).
	ListRuns(ctx context.Context, f ListRunsFilter) (runs []Run, nextCursor string, err error)
	// CancelRun moves a running run to cancelled and its pending/ready steps
	// to cancelled. Running steps notice at their next Heartbeat. Cancelling
	// an already-cancelled run returns it unchanged; a succeeded or failed run
	// returns ErrConflict.
	CancelRun(ctx context.Context, id string) (Run, error)

	// ClaimStep leases one runnable step: ready and due, or running with an
	// expired lease (its owner died). An expired step that has used all its
	// attempts is failed (dependents skipped, run failed) instead of being
	// handed out. Returns (nil, nil) when nothing is runnable.
	ClaimStep(ctx context.Context, owner string, lease time.Duration) (*Claim, error)
	// Heartbeat extends the lease and returns the new expiry. ErrLeaseLost if
	// c no longer holds the step; ErrRunCancelled if the run was cancelled.
	Heartbeat(ctx context.Context, c *Claim, lease time.Duration) (time.Time, error)
	// CompleteStep marks the step succeeded, promotes dependents whose
	// dependencies have all succeeded to ready, and finishes the run when all
	// steps are terminal, in one transaction. Returns the run's status after
	// the change. ErrLeaseLost if fenced out.
	CompleteStep(ctx context.Context, c *Claim, output json.RawMessage) (RunStatus, error)
	// FailStep records a failed attempt. With retryAt set, the step returns
	// to ready and becomes claimable at retryAt. With retryAt nil it fails
	// permanently: its transitive dependents are skipped and the run fails.
	// Returns the run's status after the change. ErrLeaseLost if fenced out.
	FailStep(ctx context.Context, c *Claim, errMsg string, retryAt *time.Time) (RunStatus, error)

	Ping(ctx context.Context) error
	Close()
}
