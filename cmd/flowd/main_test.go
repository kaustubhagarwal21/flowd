package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore/pgtest"
)

// envFrom turns a map into a getenv function.
func envFrom(m map[string]string) func(string) string {
	return func(key string) string { return m[key] }
}

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := parseConfig(nil, envFrom(map[string]string{"FLOWD_DATABASE_URL": "postgres://db"}), io.Discard)
	want := config{addr: ":8080", databaseURL: "postgres://db", workers: 8, lease: 30 * time.Second, poll: 200 * time.Millisecond}
	if err != nil || cfg != want {
		t.Fatalf("parseConfig = %+v, %v; want %+v", cfg, err, want)
	}
}

func TestParseConfigEnvThenFlags(t *testing.T) {
	getenv := envFrom(map[string]string{
		"FLOWD_ADDR":         ":9000",
		"FLOWD_DATABASE_URL": "postgres://from-env",
		"FLOWD_WORKERS":      "4",
		"FLOWD_LEASE":        "1m",
		"FLOWD_POLL":         "1s",
	})
	cfg, err := parseConfig(nil, getenv, io.Discard)
	want := config{addr: ":9000", databaseURL: "postgres://from-env", workers: 4, lease: time.Minute, poll: time.Second}
	if err != nil || cfg != want {
		t.Fatalf("env only: parseConfig = %+v, %v; want %+v", cfg, err, want)
	}

	args := []string{"-addr", "127.0.0.1:7000", "-database-url", "postgres://from-flag",
		"-workers", "2", "-lease", "10s", "-poll", "50ms"}
	cfg, err = parseConfig(args, getenv, io.Discard)
	want = config{addr: "127.0.0.1:7000", databaseURL: "postgres://from-flag", workers: 2, lease: 10 * time.Second, poll: 50 * time.Millisecond}
	if err != nil || cfg != want {
		t.Fatalf("flags over env: parseConfig = %+v, %v; want %+v", cfg, err, want)
	}
}

func TestParseConfigErrors(t *testing.T) {
	withDB := func(extra map[string]string) map[string]string {
		m := map[string]string{"FLOWD_DATABASE_URL": "postgres://db"}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	cases := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"no database URL", nil, nil},
		{"FLOWD_WORKERS not a number", nil, withDB(map[string]string{"FLOWD_WORKERS": "many"})},
		{"FLOWD_LEASE without a unit", nil, withDB(map[string]string{"FLOWD_LEASE": "30"})},
		{"zero workers", []string{"-workers=0"}, withDB(nil)},
		{"negative lease", []string{"-lease=-1s"}, withDB(nil)},
		{"zero poll", []string{"-poll=0s"}, withDB(nil)},
		{"unknown flag", []string{"-store=memory"}, withDB(nil)},
		{"extra argument", []string{"serve"}, withDB(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if _, err := parseConfig(tc.args, envFrom(tc.env), &out); err == nil {
				t.Fatal("parseConfig accepted a bad configuration")
			}
			if out.Len() == 0 {
				t.Fatal("the problem was not reported on the output")
			}
		})
	}
}

func TestParseConfigVersionAndHelp(t *testing.T) {
	// -version works without a database URL.
	cfg, err := parseConfig([]string{"-version"}, envFrom(nil), io.Discard)
	if err != nil || !cfg.showVersion {
		t.Fatalf("-version: parseConfig = %+v, %v", cfg, err)
	}
	if _, err := parseConfig([]string{"-h"}, envFrom(nil), io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("-h: err = %v, want flag.ErrHelp", err)
	}
}

// fakeEngine behaves like engine.Run: it blocks until ctx is cancelled,
// unless err is set, in which case it fails at once.
type fakeEngine struct {
	err     error
	stopped chan struct{}
}

func (f *fakeEngine) Run(ctx context.Context) error {
	defer close(f.stopped)
	if f.err != nil {
		return f.err
	}
	<-ctx.Done()
	return ctx.Err()
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func wantClosed(t *testing.T, addr string) {
	t.Helper()
	if conn, err := net.Dial("tcp", addr); err == nil {
		conn.Close()
		t.Fatalf("%s still accepts connections after shutdown", addr)
	}
}

// An in-flight request survives the shutdown, and both halves stop.
func TestServeShutsDownGracefully(t *testing.T) {
	started, release, shuttingDown := make(chan struct{}), make(chan struct{}), make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusNoContent)
	})}
	srv.RegisterOnShutdown(func() { close(shuttingDown) })
	ln, eng := listen(t), &fakeEngine{stopped: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- serve(ctx, srv, ln, eng, slog.New(slog.DiscardHandler)) }()

	client := &http.Client{}
	defer client.CloseIdleConnections()
	status := make(chan int, 1)
	go func() {
		resp, err := client.Get("http://" + ln.Addr().String() + "/")
		if err != nil {
			status <- 0
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()

	waitFor(t, started, "the request to reach the handler")
	cancel() // like SIGTERM, while the request is still in flight
	waitFor(t, shuttingDown, "the server to start shutting down")
	close(release)

	if got := <-status; got != http.StatusNoContent {
		t.Fatalf("in-flight request got status %d, want %d", got, http.StatusNoContent)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after shutdown")
	}
	waitFor(t, eng.stopped, "the engine to stop")
	wantClosed(t, ln.Addr().String())
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// If the engine fails, serve returns its error and stops the server too.
func TestServeStopsWhenTheEngineFails(t *testing.T) {
	boom := errors.New("boom")
	ln, eng := listen(t), &fakeEngine{err: boom, stopped: make(chan struct{})}
	err := serve(context.Background(), &http.Server{Handler: http.NotFoundHandler()}, ln, eng, slog.New(slog.DiscardHandler))
	if !errors.Is(err, boom) {
		t.Fatalf("serve = %v, want the engine's error", err)
	}
	wantClosed(t, ln.Addr().String())
}

// lockedBuffer is a bytes.Buffer that is safe to write from the server's
// goroutines while the test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestEndToEnd runs the real flowd (PostgreSQL, engine, executors, API) and
// drives a run through the HTTP API. It needs FLOWD_TEST_DATABASE_URL.
func TestEndToEnd(t *testing.T) {
	dbURL := pgtest.NewSchemaURL(t) // skips when no test database is configured
	ln := listen(t)
	addr := ln.Addr().String()
	ln.Close() // run listens on addr itself

	var logs lockedBuffer
	cfg := config{addr: addr, databaseURL: dbURL, workers: 4, lease: 5 * time.Second, poll: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	var runErr error
	finished := make(chan struct{})
	go func() {
		runErr = run(ctx, cfg, slog.New(slog.NewTextHandler(&logs, nil)))
		close(finished)
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
		if runErr != nil {
			t.Errorf("flowd stopped with an error: %v", runErr)
		}
		if t.Failed() {
			t.Logf("flowd log:\n%s", logs.String())
		}
	})
	base := "http://" + addr

	// Wait until flowd answers /healthz.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		select {
		case <-finished:
			t.Fatalf("flowd exited during startup: %v", runErr)
		default:
		}
		if resp, err := http.Get(base + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("flowd did not become healthy")
		}
	}

	var wf struct {
		ID string `json:"id"`
	}
	postJSON(t, base+"/v1/workflows", http.StatusCreated, `{"name":"e2e","steps":[
		{"id":"first","type":"noop"},
		{"id":"second","type":"noop","depends_on":["first"]}]}`, &wf)
	var created struct {
		RunID string `json:"run_id"`
	}
	postJSON(t, base+"/v1/workflows/"+wf.ID+"/runs", http.StatusAccepted, `{"input":{"from":"e2e"}}`, &created)

	// Poll the run until it finishes.
	var got store.Run
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		getJSON(t, base+"/v1/runs/"+created.RunID, &got)
		if got.Status.Finished() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run still %s after 15s: %+v", got.Status, got)
		}
	}
	if got.Status != store.RunSucceeded || len(got.Steps) != 2 {
		t.Fatalf("run finished as %+v, want succeeded with 2 steps", got)
	}
	for _, s := range got.Steps {
		if s.Status != store.StepSucceeded {
			t.Fatalf("step %s is %s, want succeeded", s.StepID, s.Status)
		}
	}
}

func postJSON(t *testing.T, url string, wantStatus int, body string, v any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	decodeResponse(t, resp, wantStatus, v)
}

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	decodeResponse(t, resp, http.StatusOK, v)
}

func decodeResponse(t *testing.T, resp *http.Response, wantStatus int, v any) {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status %d, want %d; body: %s", resp.Request.Method, resp.Request.URL, resp.StatusCode, wantStatus, body)
	}
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
}
