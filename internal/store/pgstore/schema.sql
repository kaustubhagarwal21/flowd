-- flowd schema. Every statement is idempotent, so Open applies the whole
-- file on every start.

CREATE TABLE IF NOT EXISTS workflows (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text        NOT NULL,
    definition  jsonb       NOT NULL,  -- validated, with defaults applied
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runs (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id uuid        NOT NULL REFERENCES workflows (id),
    status      text        NOT NULL DEFAULT 'running'
                CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
    input       jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
);

-- ListRuns pages newest first with keyset pagination on (created_at, id).
CREATE INDEX IF NOT EXISTS runs_created_idx ON runs (created_at, id);
CREATE INDEX IF NOT EXISTS runs_workflow_created_idx ON runs (workflow_id, created_at, id);

-- One row per step of a run: a snapshot of the step definition plus its
-- execution state. A lease (lease_owner, lease_expires_at) exists only
-- while the step is running.
CREATE TABLE IF NOT EXISTS steps (
    run_id           uuid        NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    step_id          text        NOT NULL,
    position         int         NOT NULL,  -- index in the workflow's topological order
    status           text        NOT NULL
                     CHECK (status IN ('pending', 'ready', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')),
    depends_on       text[]      NOT NULL DEFAULT '{}',
    attempt          int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL CHECK (max_attempts >= 1),
    not_before       timestamptz NOT NULL DEFAULT now(),  -- ready steps are claimable from this time
    lease_owner      text,
    lease_expires_at timestamptz,
    last_error       text,
    output           jsonb,
    started_at       timestamptz,
    finished_at      timestamptz,
    step_def         jsonb       NOT NULL,
    PRIMARY KEY (run_id, step_id),
    CHECK (attempt BETWEEN 0 AND max_attempts)
);

-- The claim query walks runnable steps in (not_before, position) order.
CREATE INDEX IF NOT EXISTS steps_claim_idx ON steps (not_before, position)
    WHERE status IN ('ready', 'running');
