// Package pgstore implements store.Store on PostgreSQL.
//
// Claims use SELECT ... FOR UPDATE SKIP LOCKED with time-limited leases, and
// every write for a claimed step is fenced on (lease_owner, attempt).
//
// Locking rule: a transaction that changes more than the one step row it was
// given locks its run's row first (lockRun). This serializes the state
// changes of one run. Without it, the last two dependencies of a step could
// succeed in concurrent transactions that each miss the other's update, and
// the step would never be promoted. Taking locks in one order (run, then
// steps) also means these transactions cannot deadlock.
//
// A run lock must never be waited for by something that every worker needs,
// such as a claim: its holder may be a flowd instance that froze or lost its
// network in the middle of a transaction. Claims therefore lock only step
// rows, the sweep for dead steps skips locked runs, and the transactions that
// lock a run limit how long they may sit idle (see idleTxTimeout).
package pgstore

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaustubhagarwal21/flowd/internal/jsonb"
	"github.com/kaustubhagarwal21/flowd/internal/store"
)

//go:embed schema.sql
var schema string

// idleTxTimeout is how long a transaction that locks a run may wait for its
// client's next statement before PostgreSQL ends the session. flowd sends
// those statements back to back, so a transaction takes milliseconds. A
// session idle for this long belongs to a process that froze or was cut off.
// It still holds the run lock, and without the timeout PostgreSQL would keep
// it until TCP keepalives notice, which is two hours by default. Meanwhile
// the workers of healthy instances that complete steps of that run wait.
const idleTxTimeout = 10 * time.Second

// Store is a store.Store backed by a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
	// writeTx begins every transaction that locks a run. SET LOCAL applies
	// idleTxTimeout to that transaction only, and sending it with BEGIN
	// costs no extra round trip. A startup parameter would cover every
	// session too, but connection poolers such as PgBouncer refuse
	// parameters they do not know.
	writeTx pgx.TxOptions
	// lastSweep is when a claim last looked for dead steps, in Unix
	// nanoseconds. See ClaimStep.
	lastSweep atomic.Int64
}

var _ store.Store = (*Store)(nil)

// Open connects to PostgreSQL at url and applies the embedded schema
// migrations. Tests may pass a URL whose search_path points at a private
// schema (see package pgtest).
func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("pgstore: connect: %w", err)
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	s := &Store{pool: pool}
	s.setIdleTxTimeout(idleTxTimeout)
	return s, nil
}

// setIdleTxTimeout sets the idle limit of the transactions that lock a run.
// Tests shorten it.
func (s *Store) setIdleTxTimeout(d time.Duration) {
	s.writeTx = pgx.TxOptions{BeginQuery: fmt.Sprintf(
		"BEGIN; SET LOCAL idle_in_transaction_session_timeout = %d", d.Milliseconds())}
}

// migrate applies the schema in one transaction, unless it is already there.
//
// CREATE TABLE IF NOT EXISTS is not safe against a concurrent CREATE of the
// same table, so the advisory lock makes concurrent Opens take turns. The
// lock key includes the schema name, so Opens on different schemas (parallel
// tests) do not wait for each other.
//
// An applied schema is left alone because CREATE INDEX IF NOT EXISTS locks
// its table in SHARE mode even when the index exists. Re-running it while
// other flowd instances are busy would block their writes, and could
// deadlock with a CompleteStep that has updated steps and waits to update
// runs.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('flowd migrate ' || current_schema()))`); err != nil {
			return err
		}
		// schema.sql is applied in one transaction, so if the last object
		// it creates exists, all of them do. to_regclass takes no lock.
		var applied bool
		if err := tx.QueryRow(ctx, `SELECT to_regclass('steps_claim_idx') IS NOT NULL`).Scan(&applied); err != nil {
			return err
		}
		if applied {
			return nil
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
	if err != nil {
		return fmt.Errorf("pgstore: migrate: %w", err)
	}
	return nil
}

// Ping checks that the database answers.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Close closes every connection in the pool.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// isUUID reports whether id has the canonical UUID text form. Every ID
// flowd hands out is a UUID, so anything else cannot exist. Checking first
// turns bad input into ErrNotFound instead of a database syntax error.
func isUUID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case i == 8 || i == 13 || i == 18 || i == 23:
			if c != '-' {
				return false
			}
		case '0' <= c && c <= '9', 'a' <= c && c <= 'f', 'A' <= c && c <= 'F':
		default:
			return false
		}
	}
	return true
}

// notFound maps "no rows" to store.ErrNotFound and leaves other errors as they are.
func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	}
	return err
}

// nullJSON returns nil, stored as SQL NULL, for an absent or JSON null value.
func nullJSON(b json.RawMessage) []byte {
	if t := bytes.TrimSpace(b); len(t) == 0 || string(t) == "null" {
		return nil
	}
	return b
}

// outputJSON makes a step output storable in a jsonb column. Executors may
// return any bytes (for example a truncated HTML page), and jsonb rejects
// anything that is not valid JSON, so such output is kept as a JSON string.
// json.Valid is not enough: it also accepts invalid UTF-8, \u0000 and lone
// surrogate escapes, which jsonb rejects. CompleteStep would then fail on
// every attempt, and a step whose target succeeded would be run again until
// its run failed.
func outputJSON(out json.RawMessage) []byte {
	out = nullJSON(out)
	if out == nil || jsonb.Storable(out) {
		return out
	}
	b, _ := json.Marshal(cleanText(string(out))) // marshalling a string cannot fail
	return b
}

// cleanText makes s storable in a PostgreSQL text column, which rejects NUL
// bytes and invalid UTF-8. Error messages can quote arbitrary response bytes.
func cleanText(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "�")
}
