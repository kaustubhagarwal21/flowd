package executor_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/engine"
	"github.com/kaustubhagarwal21/flowd/internal/executor"
	"github.com/kaustubhagarwal21/flowd/internal/jsonb"
	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// seen is what a test server received.
type seen struct {
	method, contentType, key, custom string
	body                             string
}

// newServer starts a test server that records requests and replies with
// status and body.
func newServer(t *testing.T, status int, body string) (*httptest.Server, func() []seen) {
	t.Helper()
	var (
		mu   sync.Mutex
		reqs []seen
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		reqs = append(reqs, seen{
			method: r.Method, contentType: r.Header.Get("Content-Type"),
			key: r.Header.Get("Idempotency-Key"), custom: r.Header.Get("X-Custom"), body: string(b),
		})
		mu.Unlock()
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []seen {
		mu.Lock()
		defer mu.Unlock()
		return append([]seen(nil), reqs...)
	}
}

// httpClaim returns attempt n of step "call" of run "run-1", calling url.
func httpClaim(url string, n int) *store.Claim {
	return &store.Claim{
		RunID: "run-1", StepID: "call", Attempt: n, Owner: "test",
		Step: workflow.Step{ID: "call", Type: workflow.StepHTTP, HTTP: &workflow.HTTPSpec{
			Method:  http.MethodPost,
			URL:     url,
			Headers: map[string]string{"X-Custom": "yes"},
			Body:    json.RawMessage(`{"n": 1}`),
		}},
	}
}

func TestHTTPSuccessSendsRequestAndRecordsOutput(t *testing.T) {
	srv, reqs := newServer(t, http.StatusCreated, `{"ok": true}`)
	out, err := executor.NewHTTP(nil).Execute(context.Background(), httpClaim(srv.URL, 1))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if want := `{"status":201,"body":{"ok":true}}`; string(out) != want {
		t.Errorf("output = %s, want %s", out, want)
	}
	got := reqs()
	if len(got) != 1 {
		t.Fatalf("server saw %d requests, want 1", len(got))
	}
	want := seen{method: "POST", contentType: "application/json", key: "run-1/call", custom: "yes", body: `{"n": 1}`}
	if got[0] != want {
		t.Errorf("request = %+v, want %+v", got[0], want)
	}
}

func TestHTTPIdempotencyKeyIsStableAcrossRetries(t *testing.T) {
	srv, reqs := newServer(t, http.StatusServiceUnavailable, "busy")
	ex := executor.NewHTTP(nil)
	for n := 1; n <= 3; n++ {
		if _, err := ex.Execute(context.Background(), httpClaim(srv.URL, n)); err == nil {
			t.Fatalf("attempt %d: want an error for 503", n)
		}
	}
	got := reqs()
	if len(got) != 3 {
		t.Fatalf("server saw %d requests, want 3", len(got))
	}
	for i, r := range got {
		if r.key != "run-1/call" {
			t.Errorf("attempt %d sent Idempotency-Key %q, want run-1/call", i+1, r.key)
		}
	}
}

func TestHTTPUserCannotOverrideIdempotencyKey(t *testing.T) {
	srv, reqs := newServer(t, http.StatusOK, "")
	c := httpClaim(srv.URL, 1)
	c.Step.HTTP.Headers["Idempotency-Key"] = "mine"
	if _, err := executor.NewHTTP(nil).Execute(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if key := reqs()[0].key; key != "run-1/call" {
		t.Errorf("Idempotency-Key = %q, want run-1/call", key)
	}
}

func TestHTTPContentType(t *testing.T) {
	srv, reqs := newServer(t, http.StatusOK, "")
	ex := executor.NewHTTP(nil)

	custom := httpClaim(srv.URL, 1)
	custom.Step.HTTP.Headers["Content-Type"] = "application/vnd.api+json"
	noBody := httpClaim(srv.URL, 1)
	noBody.Step.HTTP.Method, noBody.Step.HTTP.Body = http.MethodGet, nil
	for _, c := range []*store.Claim{custom, noBody} {
		if _, err := ex.Execute(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	got := reqs()
	if got[0].contentType != "application/vnd.api+json" {
		t.Errorf("custom Content-Type replaced with %q", got[0].contentType)
	}
	if got[1].method != "GET" || got[1].contentType != "" || got[1].body != "" {
		t.Errorf("request without a body = %+v, want a GET with no Content-Type", got[1])
	}
}

func TestHTTPNonJSONBodyIsStoredAsString(t *testing.T) {
	srv, _ := newServer(t, http.StatusOK, "plain <b>text</b>")
	out, err := executor.NewHTTP(nil).Execute(context.Background(), httpClaim(srv.URL, 1))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	if err := json.Unmarshal(out, &got); err != nil || got.Status != 200 || got.Body != "plain <b>text</b>" {
		t.Errorf("output = %s (%v), want status 200 and the body as a string", out, err)
	}
}

// PostgreSQL's jsonb rejects \u0000, so the output must never contain it.
func TestHTTPOutputHasNoNULCharacters(t *testing.T) {
	tests := map[string]struct{ body, want string }{
		"binary":            {"PNG\x00\x01 data\x00", "PNG\x01 data"},
		"JSON with \\u0000": {`{"a":"x\u0000y"}`, `{"a":"x\u0000y"}`}, // kept as text
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			srv, _ := newServer(t, http.StatusOK, tt.body)
			out, err := executor.NewHTTP(nil).Execute(context.Background(), httpClaim(srv.URL, 1))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(out), `\u0000`) && !strings.Contains(string(out), `\\u0000`) {
				t.Errorf("output %s contains a NUL escape", out)
			}
			var got struct{ Body string }
			if err := json.Unmarshal(out, &got); err != nil || got.Body != tt.want {
				t.Errorf("body = %q (%v), want %q", got.Body, err, tt.want)
			}
		})
	}
}

// jsonb rejects some JSON that json.Valid accepts. The output must be
// storable whatever the body, or saving the step would fail on every attempt
// and a target that succeeded would be called again and again.
func TestHTTPOutputIsAlwaysStorable(t *testing.T) {
	tests := []struct {
		name, body string
		want       any // the body, as decoded from the output
	}{
		// Unstorable JSON is kept as text, where escapes are plain characters.
		{"NUL escape", `{"a":"x\u0000y"}`, `{"a":"x\u0000y"}`},
		{"lone surrogate escape", `{"a":"\ud800"}`, `{"a":"\ud800"}`},
		{"invalid UTF-8 in a string", "{\"a\":\"caf\xe9\"}", "{\"a\":\"caf\U0000FFFD\"}"},
		{"invalid UTF-8 and NUL in text", "caf\xe9\x00!", "caf\U0000FFFD!"},
		// Storable JSON is still kept as JSON. \x5c is a backslash, so the
		// body holds the escaped surrogate pair of U+1F600.
		{"surrogate pair", "{\"a\":\"\x5cud83d\x5cude00\"}", map[string]any{"a": "\U0001F600"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newServer(t, http.StatusOK, tt.body)
			out, err := executor.NewHTTP(nil).Execute(context.Background(), httpClaim(srv.URL, 1))
			if err != nil {
				t.Fatal(err)
			}
			if !jsonb.Storable(out) {
				t.Errorf("output %q cannot be stored in jsonb", out)
			}
			var got struct {
				Status int `json:"status"`
				Body   any `json:"body"`
			}
			if err := json.Unmarshal(out, &got); err != nil || got.Status != 200 || !reflect.DeepEqual(got.Body, tt.want) {
				t.Errorf("output = %q (%v), want status 200 and body %#v", out, err, tt.want)
			}
		})
	}
}

func TestHTTPOutputIsTruncated(t *testing.T) {
	// A JSON body just over the cap: cut short it is no longer valid JSON,
	// so the first MaxOutputBytes bytes are kept as a string.
	big := `["` + strings.Repeat("a", executor.MaxOutputBytes) + `"]`
	srv, _ := newServer(t, http.StatusOK, big)
	out, err := executor.NewHTTP(nil).Execute(context.Background(), httpClaim(srv.URL, 1))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not {status, body string}: %v", err)
	}
	if got.Status != 200 || got.Body != big[:executor.MaxOutputBytes] {
		t.Errorf("status %d, body of %d bytes; want 200 and the first %d bytes", got.Status, len(got.Body), executor.MaxOutputBytes)
	}
}

func TestHTTPStatusClassification(t *testing.T) {
	tests := []struct {
		status    int
		permanent bool
	}{
		{http.StatusBadRequest, true},
		{http.StatusUnauthorized, true},
		{http.StatusNotFound, true},
		{http.StatusUnprocessableEntity, true},
		{http.StatusRequestTimeout, false},
		{http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false},
		{http.StatusServiceUnavailable, false},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			srv, _ := newServer(t, tt.status, `{"error":"nope"}`)
			out, err := executor.NewHTTP(nil).Execute(context.Background(), httpClaim(srv.URL, 1))
			if err == nil || out != nil {
				t.Fatalf("got (%s, %v), want an error and no output", out, err)
			}
			if engine.IsPermanent(err) != tt.permanent {
				t.Errorf("IsPermanent(%v) = %v, want %v", err, !tt.permanent, tt.permanent)
			}
			if !strings.Contains(err.Error(), http.StatusText(tt.status)) || !strings.Contains(err.Error(), `{"error":"nope"}`) {
				t.Errorf("error %q should name the status and quote the body", err)
			}
		})
	}
}

func TestHTTPNetworkErrorIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close() // nothing listens there any more
	_, err := executor.NewHTTP(nil).Execute(context.Background(), httpClaim(srv.URL, 1))
	if err == nil || engine.IsPermanent(err) {
		t.Errorf("err = %v, want a retryable error", err)
	}
}

func TestHTTPRespectsContextDeadline(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done(): // the client gave up
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release) // runs before srv.Close, so a stuck handler cannot block it

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	begin := time.Now()
	_, err := executor.NewHTTP(nil).Execute(ctx, httpClaim(srv.URL, 1))
	if !errors.Is(err, context.DeadlineExceeded) || engine.IsPermanent(err) {
		t.Errorf("err = %v, want a retryable deadline error", err)
	}
	if took := time.Since(begin); took > 2*time.Second {
		t.Errorf("Execute took %v, want it to stop at the 50ms deadline", took)
	}
}

func TestHTTPBadSpecIsPermanent(t *testing.T) {
	tests := map[string]*workflow.HTTPSpec{
		"no http settings": nil,
		"bad method":       {Method: "NOT A METHOD", URL: "http://example.invalid"},
		"bad scheme":       {Method: "GET", URL: "ftp://example.invalid/file"},
		"no scheme":        {Method: "GET", URL: "example.invalid/path"},
	}
	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			c := &store.Claim{RunID: "r", StepID: "s", Attempt: 1, Step: workflow.Step{Type: workflow.StepHTTP, HTTP: spec}}
			if _, err := executor.NewHTTP(nil).Execute(context.Background(), c); !engine.IsPermanent(err) {
				t.Errorf("err = %v, want a permanent error", err)
			}
		})
	}
}

func noopClaim(attempt, sleepMS, failTimes int) *store.Claim {
	return &store.Claim{RunID: "r", StepID: "s", Attempt: attempt, Step: workflow.Step{
		Type: workflow.StepNoop, Noop: &workflow.NoopSpec{SleepMS: sleepMS, FailTimes: failTimes},
	}}
}

func TestNoopFailsTheFirstFailTimesAttempts(t *testing.T) {
	ex := executor.NewNoop()
	for attempt := 1; attempt <= 4; attempt++ {
		_, err := ex.Execute(context.Background(), noopClaim(attempt, 0, 2))
		if wantErr := attempt <= 2; (err != nil) != wantErr {
			t.Errorf("attempt %d: err = %v, want error: %v", attempt, err, wantErr)
		}
		if engine.IsPermanent(err) {
			t.Errorf("attempt %d: a noop failure should be retryable", attempt)
		}
	}
	// No noop settings at all: succeed at once.
	c := noopClaim(1, 0, 0)
	c.Step.Noop = nil
	if out, err := ex.Execute(context.Background(), c); err != nil || out != nil {
		t.Errorf("bare noop = (%s, %v), want (nil, nil)", out, err)
	}
}

func TestNoopSleepRespectsContext(t *testing.T) {
	ex := executor.NewNoop()
	begin := time.Now()
	if _, err := ex.Execute(context.Background(), noopClaim(1, 30, 0)); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(begin); took < 30*time.Millisecond {
		t.Errorf("slept %v, want at least 30ms", took)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	begin = time.Now()
	_, err := ex.Execute(ctx, noopClaim(1, 10_000, 0))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(begin); took > 2*time.Second {
		t.Errorf("a cancelled sleep took %v", took)
	}
}
