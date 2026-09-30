// Package pgtest gives tests an isolated pgstore.Store.
//
// New skips the test when FLOWD_TEST_DATABASE_URL is unset. Otherwise it
// creates a fresh schema with a random name, opens a pgstore.Store whose
// search_path is that schema (so tests in different packages can run in
// parallel without seeing each other's rows), and drops the schema in
// t.Cleanup. NewSchemaURL returns such a connection URL instead, so tests can
// open several Stores (several "flowd processes") on one schema.
package pgtest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore"
)

// envURL names the environment variable that holds the test database URL.
const envURL = "FLOWD_TEST_DATABASE_URL"

// New returns a Store on a fresh private schema, closed and dropped at cleanup.
func New(t testing.TB) *pgstore.Store {
	t.Helper()
	schemaURL := NewSchemaURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := pgstore.Open(ctx, schemaURL)
	if err != nil {
		t.Fatalf("pgtest: open store: %v", err)
	}
	// Cleanups run last-in first-out, so the pool closes before the schema
	// is dropped.
	t.Cleanup(st.Close)
	return st
}

// NewSchemaURL creates a fresh private schema (dropped at cleanup) and returns
// a connection URL whose search_path points at it. Open it as many times as
// needed to simulate several processes sharing one database.
func NewSchemaURL(t testing.TB) string {
	t.Helper()
	base := os.Getenv(envURL)
	if base == "" {
		t.Skip(envURL + " is not set; skipping PostgreSQL test")
	}
	// Lower-case letters and digits only, because search_path folds
	// unquoted names to lower case.
	schema := fmt.Sprintf("flowd_test_%016x", rand.Uint64())
	ident := pgx.Identifier{schema}.Sanitize()

	if err := exec(base, "CREATE SCHEMA "+ident); err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	t.Cleanup(func() {
		if err := exec(base, "DROP SCHEMA "+ident+" CASCADE"); err != nil {
			t.Errorf("pgtest: %v", err)
		}
	})
	return withSearchPath(base, schema)
}

// exec runs one statement on a short-lived connection to base.
func exec(base, sql string) error {
	// Not t.Context(): it is already cancelled when cleanup functions run.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("%s: %w", sql, err)
	}
	return nil
}

// withSearchPath adds search_path to a connection string. pgx sends the
// connection-string settings it does not use itself to the server as
// run-time parameters, so every connection opened with the result starts
// with the private schema as its search_path.
func withSearchPath(base, schema string) string {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		// The keyword/value form: "host=localhost dbname=flowd_test".
		return base + " search_path=" + schema
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
