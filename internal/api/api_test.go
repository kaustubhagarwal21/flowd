package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kaustubhagarwal21/flowd/internal/api"
	"github.com/kaustubhagarwal21/flowd/internal/metrics"
	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

const jsonType = "application/json"

// apiProblem mirrors the problem+json body. It is decoded strictly, so a
// renamed or unexpected member fails the test.
type apiProblem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
	Field    string `json:"field"`
}

// countingWaker records how often the API nudged the engine.
type countingWaker struct{ n atomic.Int32 }

func (w *countingWaker) Wake() { w.n.Add(1) }

type harness struct {
	store   *fakeStore
	waker   *countingWaker
	handler http.Handler
}

func newHarness() *harness {
	st, wk := newFakeStore(), &countingWaker{}
	return &harness{store: st, waker: wk, handler: api.New(st, wk, metrics.New(), nil)}
}

// do sends one request through h. An empty contentType sends no header, and
// an empty body sends no body.
func do(t *testing.T, h http.Handler, method, target, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rd)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// wantProblem checks that rec is an RFC 7807 problem with the given status.
func wantProblem(t *testing.T, rec *httptest.ResponseRecorder, status int) apiProblem {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, status, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json", ct)
	}
	var p apiProblem
	dec := json.NewDecoder(rec.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if p.Type != "about:blank" || p.Title != http.StatusText(status) || p.Status != status || p.Detail == "" || p.Instance == "" {
		t.Fatalf("malformed problem: %+v", p)
	}
	return p
}

// wantJSON checks the status and content type, then decodes the body into v.
func wantJSON(t *testing.T, rec *httptest.ResponseRecorder, status int, v any) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, status, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != jsonType {
		t.Fatalf("Content-Type = %q, want %s", ct, jsonType)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode body: %v; body: %s", err, rec.Body)
	}
}

// demoDefinition is stored straight into the fake store by tests that do not
// go through POST /v1/workflows (and so do not need workflow.Validate).
func demoDefinition() workflow.Definition {
	return workflow.Definition{Name: "demo", Steps: []workflow.Step{{ID: "only", Type: workflow.StepNoop}}}
}

// TestCreateWorkflow needs the real workflow.Validate (defaults filled in).
func TestCreateWorkflow(t *testing.T) {
	a := newHarness()
	body := `{
		"name": "deploy-demo",
		"steps": [
			{"id": "build", "type": "noop", "noop": {"sleep_ms": 10}},
			{"id": "test", "type": "noop", "depends_on": ["build"],
			 "retry": {"max_attempts": 5, "initial_backoff_ms": 100, "max_backoff_ms": 1000},
			 "timeout_ms": 5000}
		]
	}`
	rec := do(t, a.handler, "POST", "/v1/workflows", jsonType, body)
	var wf store.Workflow
	wantJSON(t, rec, http.StatusCreated, &wf)
	if wf.ID == "" || wf.Name != "deploy-demo" || len(wf.Definition.Steps) != 2 {
		t.Fatalf("unexpected workflow: %+v", wf)
	}
	if got, want := rec.Header().Get("Location"), "/v1/workflows/"+wf.ID; got != want {
		t.Fatalf("Location = %q, want %q", got, want)
	}
	build, test := wf.Definition.Steps[0], wf.Definition.Steps[1]
	if build.Retry.MaxAttempts != workflow.DefaultMaxAttempts || build.Timeout() != workflow.DefaultTimeout {
		t.Errorf("defaults not applied to step %q: %+v", build.ID, build)
	}
	if test.Retry.MaxAttempts != 5 || test.TimeoutMS != 5000 {
		t.Errorf("explicit settings of step %q changed: %+v", test.ID, test)
	}

	// The Location header leads to the stored workflow.
	var got store.Workflow
	wantJSON(t, do(t, a.handler, "GET", rec.Header().Get("Location"), "", ""), http.StatusOK, &got)
	if got.ID != wf.ID || len(got.Definition.Steps) != 2 {
		t.Fatalf("GET Location returned %+v, want workflow %s", got, wf.ID)
	}
}

// TestCreateWorkflowValidationError needs the real workflow.Validate: the
// problem must carry the ValidationError's field and message, and they must
// point at the offending part of the definition.
func TestCreateWorkflowValidationError(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		mentions []string // the field or the detail must contain one of these
	}{
		{"no steps", `{"name":"empty","steps":[]}`, []string{"steps"}},
		{"cycle", `{"name":"loop","steps":[
			{"id":"alpha","type":"noop","depends_on":["beta"]},
			{"id":"beta","type":"noop","depends_on":["alpha"]}]}`, []string{"alpha", "beta"}},
		{"unknown dependency", `{"name":"x","steps":[{"id":"build","type":"noop","depends_on":["ghost"]}]}`, []string{"ghost", "depends_on"}},
		{"duplicate step id", `{"name":"x","steps":[{"id":"twin","type":"noop"},{"id":"twin","type":"noop"}]}`, []string{"twin", "steps[1]"}},
		{"bad step id", `{"name":"x","steps":[{"id":"Not Valid!","type":"noop"}]}`, []string{"steps[0]", "Not Valid!"}},
		{"unknown step type", `{"name":"x","steps":[{"id":"run","type":"shell"}]}`, []string{"steps[0]", "shell"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newHarness()
			p := wantProblem(t, do(t, a.handler, "POST", "/v1/workflows", jsonType, tc.body), http.StatusBadRequest)

			// The API passes the ValidationError through unchanged.
			var def workflow.Definition
			if err := json.Unmarshal([]byte(tc.body), &def); err != nil {
				t.Fatal(err)
			}
			_, err := workflow.Validate(&def)
			var ve *workflow.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("Validate(%s) = %v, want a *ValidationError", tc.name, err)
			}
			if p.Field != ve.Field || p.Detail != ve.Message {
				t.Errorf("problem field/detail = %q/%q, want %q/%q", p.Field, p.Detail, ve.Field, ve.Message)
			}

			if !slices.ContainsFunc(tc.mentions, func(s string) bool { return strings.Contains(p.Field+" "+p.Detail, s) }) {
				t.Errorf("problem %+v mentions none of %q", p, tc.mentions)
			}
			if n := a.store.workflowCount(); n != 0 {
				t.Errorf("an invalid workflow was stored (%d workflows)", n)
			}
		})
	}
}

func TestCreateWorkflowRejectsBadRequests(t *testing.T) {
	big := `{"name":"` + strings.Repeat("a", 1<<20) + `","steps":[]}`
	cases := []struct {
		name        string
		contentType string
		body        string
		status      int
		mention     string // optional; must appear in the detail
	}{
		{"unknown field", jsonType, `{"name":"x","steps":[],"owner":"me"}`, http.StatusBadRequest, "owner"},
		{"unknown step field", jsonType, `{"name":"x","steps":[{"id":"a","type":"noop","depends_no":["b"]}]}`, http.StatusBadRequest, "depends_no"},
		{"malformed JSON", jsonType, `{"name":`, http.StatusBadRequest, ""},
		{"wrong JSON type", jsonType, `{"name":"x","steps":"a,b"}`, http.StatusBadRequest, ""},
		{"two JSON values", jsonType, `{"name":"x","steps":[]} {}`, http.StatusBadRequest, "single JSON value"},
		{"empty body", jsonType, "", http.StatusBadRequest, "empty"},
		{"no content type", "", `{"name":"x","steps":[]}`, http.StatusUnsupportedMediaType, "application/json"},
		{"text content type", "text/plain", `{"name":"x","steps":[]}`, http.StatusUnsupportedMediaType, ""},
		{"form content type", "application/x-www-form-urlencoded", "name=x", http.StatusUnsupportedMediaType, ""},
		{"body over 1 MiB", jsonType, big, http.StatusRequestEntityTooLarge, "1048576"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newHarness()
			p := wantProblem(t, do(t, a.handler, "POST", "/v1/workflows", tc.contentType, tc.body), tc.status)
			if !strings.Contains(p.Detail, tc.mention) {
				t.Errorf("detail %q does not mention %q", p.Detail, tc.mention)
			}
			if n := a.store.workflowCount(); n != 0 {
				t.Errorf("a workflow was stored (%d workflows)", n)
			}
		})
	}
}

func TestGetWorkflow(t *testing.T) {
	a := newHarness()
	wf := a.store.addWorkflow(demoDefinition())

	var got store.Workflow
	wantJSON(t, do(t, a.handler, "GET", "/v1/workflows/"+wf.ID, "", ""), http.StatusOK, &got)
	if got.ID != wf.ID || got.Name != "demo" || len(got.Definition.Steps) != 1 {
		t.Fatalf("got %+v, want workflow %s", got, wf.ID)
	}

	p := wantProblem(t, do(t, a.handler, "GET", "/v1/workflows/wf-missing", "", ""), http.StatusNotFound)
	if p.Instance != "/v1/workflows/wf-missing" || !strings.Contains(p.Detail, "wf-missing") {
		t.Fatalf("unexpected 404 problem: %+v", p)
	}
}

func TestCreateRun(t *testing.T) {
	a := newHarness()
	wf := a.store.addWorkflow(demoDefinition())
	target := "/v1/workflows/" + wf.ID + "/runs"

	cases := []struct {
		name        string
		contentType string
		body        string
		wantInput   string // as stored; "" means no input
	}{
		{"with input", jsonType, `{"input":{"env":"prod"}}`, `{"env":"prod"}`},
		{"no body", "", "", ""},
		{"empty object", jsonType, `{}`, ""},
		{"null input", jsonType, `{"input":null}`, ""},
		{"charset parameter", "application/json; charset=utf-8", `{"input":{"n":1}}`, `{"n":1}`},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, a.handler, "POST", target, tc.contentType, tc.body)
			var resp map[string]string
			wantJSON(t, rec, http.StatusAccepted, &resp)
			runID := resp["run_id"]
			if runID == "" || len(resp) != 1 {
				t.Fatalf("body = %v, want exactly {run_id}", resp)
			}
			if got, want := rec.Header().Get("Location"), "/v1/runs/"+runID; got != want {
				t.Fatalf("Location = %q, want %q", got, want)
			}
			run, err := a.store.GetRun(context.Background(), runID)
			if err != nil {
				t.Fatal(err)
			}
			if run.WorkflowID != wf.ID || string(run.Input) != tc.wantInput {
				t.Fatalf("stored run %+v, want workflow %s and input %q", run, wf.ID, tc.wantInput)
			}
			if got := a.waker.n.Load(); got != int32(i+1) {
				t.Fatalf("engine woken %d times, want %d", got, i+1)
			}
		})
	}

	// Every created run is counted, and /metrics shows it.
	rec := do(t, a.handler, "GET", "/metrics", "", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "flowd_runs_started_total 5") {
		t.Fatalf("GET /metrics: status %d, body does not count 5 started runs", rec.Code)
	}
}

func TestCreateRunRejectsBadRequests(t *testing.T) {
	big := `{"input":{"blob":"` + strings.Repeat("a", 1<<20) + `"}}`
	cases := []struct {
		name        string
		workflowID  string // "" means the stored workflow
		contentType string
		body        string
		status      int
	}{
		{"unknown workflow", "wf-missing", jsonType, `{"input":{}}`, http.StatusNotFound},
		{"unknown workflow without body", "wf-missing", "", "", http.StatusNotFound},
		{"input is an array", "", jsonType, `{"input":[1,2]}`, http.StatusBadRequest},
		{"input is a string", "", jsonType, `{"input":"prod"}`, http.StatusBadRequest},
		{"unknown field", "", jsonType, `{"inputs":{}}`, http.StatusBadRequest},
		{"malformed JSON", "", jsonType, `{"input":`, http.StatusBadRequest},
		{"wrong content type", "", "text/plain", `{"input":{}}`, http.StatusUnsupportedMediaType},
		{"body over 1 MiB", "", jsonType, big, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newHarness()
			wf := a.store.addWorkflow(demoDefinition())
			id := tc.workflowID
			if id == "" {
				id = wf.ID
			}
			wantProblem(t, do(t, a.handler, "POST", "/v1/workflows/"+id+"/runs", tc.contentType, tc.body), tc.status)
			if n := a.waker.n.Load(); n != 0 {
				t.Errorf("engine woken %d times for a rejected request", n)
			}
			if runs, _, _ := a.store.ListRuns(context.Background(), store.ListRunsFilter{}); len(runs) != 0 {
				t.Errorf("a run was created: %+v", runs)
			}
		})
	}
}

func TestGetRun(t *testing.T) {
	a := newHarness()
	wf := a.store.addWorkflow(demoDefinition())
	run := a.store.addRun(wf.ID, store.RunRunning,
		store.StepState{StepID: "build", Status: store.StepSucceeded, Attempt: 1, MaxAttempts: 3},
		store.StepState{StepID: "test", Status: store.StepReady, MaxAttempts: 3, DependsOn: []string{"build"}, LastError: "HTTP 503"},
	)

	rec := do(t, a.handler, "GET", "/v1/runs/"+run.ID, "", "")
	var got store.Run
	wantJSON(t, rec, http.StatusOK, &got)
	if got.ID != run.ID || got.Status != store.RunRunning || len(got.Steps) != 2 {
		t.Fatalf("got %+v, want run %s with 2 steps", got, run.ID)
	}
	if s := got.Steps[1]; s.StepID != "test" || s.Status != store.StepReady || s.LastError != "HTTP 503" {
		t.Fatalf("unexpected step state: %+v", s)
	}
	for _, key := range []string{`"workflow_id"`, `"step_id"`, `"max_attempts"`, `"last_error"`, `"depends_on"`} {
		if !strings.Contains(rec.Body.String(), key) {
			t.Errorf("body has no %s member: %s", key, rec.Body)
		}
	}

	wantProblem(t, do(t, a.handler, "GET", "/v1/runs/run-missing", "", ""), http.StatusNotFound)
}

// listPage is the body of GET /v1/runs.
type listPage struct {
	Runs       []store.Run `json:"runs"`
	NextCursor string      `json:"next_cursor"`
}

func TestListRunsPagination(t *testing.T) {
	a := newHarness()
	wf := a.store.addWorkflow(demoDefinition())
	other := a.store.addWorkflow(demoDefinition())
	var want []string
	for range 5 {
		want = append(want, a.store.addRun(wf.ID, store.RunRunning).ID)
	}
	slices.Reverse(want) // newest first
	a.store.addRun(other.ID, store.RunRunning)

	var got []string
	target := "/v1/runs?limit=2&workflow_id=" + url.QueryEscape(wf.ID)
	for pages := 1; ; pages++ {
		if pages > len(want) {
			t.Fatal("pagination never ends")
		}
		var page listPage
		wantJSON(t, do(t, a.handler, "GET", target, "", ""), http.StatusOK, &page)
		if len(page.Runs) > 2 {
			t.Fatalf("page has %d runs, limit is 2", len(page.Runs))
		}
		for _, run := range page.Runs {
			got = append(got, run.ID)
		}
		if page.NextCursor == "" {
			break
		}
		target = "/v1/runs?limit=2&workflow_id=" + url.QueryEscape(wf.ID) + "&cursor=" + url.QueryEscape(page.NextCursor)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("paged runs = %v, want %v", got, want)
	}
}

func TestListRunsPassesFilters(t *testing.T) {
	a := newHarness()
	wantJSON(t, do(t, a.handler, "GET", "/v1/runs?workflow_id=wf-9&status=succeeded&limit=7&cursor=3", "", ""),
		http.StatusOK, &listPage{})
	want := store.ListRunsFilter{WorkflowID: "wf-9", Status: store.RunSucceeded, Limit: 7, Cursor: "3"}
	if a.store.lastList != want {
		t.Fatalf("store got filter %+v, want %+v", a.store.lastList, want)
	}

	// Without parameters the store applies its defaults (Limit 0 means 20),
	// and an empty page is [] rather than null, with no next_cursor.
	rec := do(t, a.handler, "GET", "/v1/runs", "", "")
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"runs":[]}` {
		t.Fatalf("empty list: status %d, body %s", rec.Code, rec.Body)
	}
	if a.store.lastList != (store.ListRunsFilter{}) {
		t.Fatalf("store got filter %+v, want the zero filter", a.store.lastList)
	}
}

func TestListRunsRejectsBadParameters(t *testing.T) {
	for _, query := range []string{
		"status=bogus",
		"status=RUNNING",
		"limit=0",
		"limit=101",
		"limit=-1",
		"limit=ten",
		"cursor=not-a-cursor", // the fake store reports store.ErrInvalidCursor
	} {
		t.Run(query, func(t *testing.T) {
			a := newHarness()
			wantProblem(t, do(t, a.handler, "GET", "/v1/runs?"+query, "", ""), http.StatusBadRequest)
		})
	}
}

func TestCancelRun(t *testing.T) {
	a := newHarness()
	wf := a.store.addWorkflow(demoDefinition())
	running := a.store.addRun(wf.ID, store.RunRunning)
	succeeded := a.store.addRun(wf.ID, store.RunSucceeded)
	failed := a.store.addRun(wf.ID, store.RunFailed)

	// Cancelling twice is fine: the second call returns the cancelled run.
	for range 2 {
		var got store.Run
		wantJSON(t, do(t, a.handler, "POST", "/v1/runs/"+running.ID+"/cancel", "", ""), http.StatusOK, &got)
		if got.ID != running.ID || got.Status != store.RunCancelled || got.FinishedAt == nil {
			t.Fatalf("cancel returned %+v", got)
		}
	}
	for _, run := range []store.Run{succeeded, failed} {
		wantProblem(t, do(t, a.handler, "POST", "/v1/runs/"+run.ID+"/cancel", "", ""), http.StatusConflict)
	}
	wantProblem(t, do(t, a.handler, "POST", "/v1/runs/run-missing/cancel", "", ""), http.StatusNotFound)

	// The run counts as cancelled once, however often it was cancelled.
	metricsBody := do(t, a.handler, "GET", "/metrics", "", "").Body.String()
	if !strings.Contains(metricsBody, `flowd_runs_finished_total{status="cancelled"} 1`+"\n") {
		t.Errorf("/metrics does not count exactly one cancelled run")
	}
}

func TestHealthz(t *testing.T) {
	a := newHarness()
	var body map[string]string
	wantJSON(t, do(t, a.handler, "GET", "/healthz", "", ""), http.StatusOK, &body)
	if body["status"] != "ok" {
		t.Fatalf("body = %v, want status ok", body)
	}

	a.store.pingErr = errors.New("dial tcp 10.0.0.7:5432: connection refused")
	p := wantProblem(t, do(t, a.handler, "GET", "/healthz", "", ""), http.StatusServiceUnavailable)
	if strings.Contains(p.Detail, "10.0.0.7") {
		t.Fatalf("detail leaks the store error: %q", p.Detail)
	}
}

func TestMetrics(t *testing.T) {
	rec := do(t, newHarness().handler, "GET", "/metrics", "", "")
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("status %d, Content-Type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	if !strings.Contains(rec.Body.String(), "flowd_runs_started_total") {
		t.Fatal("flowd_runs_started_total is missing")
	}
}

// With a nil Waker, nil metrics and a nil logger the API still works, and
// /metrics is simply not there.
func TestNilDependencies(t *testing.T) {
	st := newFakeStore()
	wf := st.addWorkflow(demoDefinition())
	h := api.New(st, nil, nil, nil)
	if rec := do(t, h, "POST", "/v1/workflows/"+wf.ID+"/runs", "", ""); rec.Code != http.StatusAccepted {
		t.Fatalf("create run: status %d, body %s", rec.Code, rec.Body)
	}
	wantProblem(t, do(t, h, "GET", "/metrics", "", ""), http.StatusNotFound)
}

func TestUnknownRoutesAndMethods(t *testing.T) {
	cases := []struct {
		method string
		target string
		status int
		allow  []string // for 405: methods the Allow header must list
	}{
		{"GET", "/", http.StatusNotFound, nil},
		{"GET", "/v2/workflows", http.StatusNotFound, nil},
		{"GET", "/v1/workflows/wf-1/runs/extra", http.StatusNotFound, nil},
		{"DELETE", "/v1/workflows", http.StatusMethodNotAllowed, []string{"POST"}},
		{"GET", "/v1/workflows", http.StatusMethodNotAllowed, []string{"POST"}},
		{"PUT", "/v1/runs/run-1", http.StatusMethodNotAllowed, []string{"GET", "HEAD"}},
		{"POST", "/v1/runs", http.StatusMethodNotAllowed, []string{"GET", "HEAD"}},
		{"GET", "/v1/runs/run-1/cancel", http.StatusMethodNotAllowed, []string{"POST"}},
		{"POST", "/healthz", http.StatusMethodNotAllowed, []string{"GET", "HEAD"}},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			rec := do(t, newHarness().handler, tc.method, tc.target, "", "")
			p := wantProblem(t, rec, tc.status)
			if p.Instance != tc.target {
				t.Errorf("instance = %q, want %q", p.Instance, tc.target)
			}
			allow := rec.Header().Get("Allow")
			for _, m := range tc.allow {
				if !strings.Contains(allow, m) {
					t.Errorf("Allow = %q, want it to list %s", allow, m)
				}
			}
			if tc.allow == nil && allow != "" {
				t.Errorf("a 404 has an Allow header: %q", allow)
			}
		})
	}
}

func TestPanicBecomes500(t *testing.T) {
	var logs bytes.Buffer
	h := api.New(panicStore{newFakeStore()}, nil, nil, slog.New(slog.NewJSONHandler(&logs, nil)))
	wantProblem(t, do(t, h, "GET", "/v1/runs/run-1", "", ""), http.StatusInternalServerError)
	for _, want := range []string{`"msg":"handler panic"`, `"panic":"boom"`, `"status":500`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log lacks %s:\n%s", want, logs.String())
		}
	}
}

func TestStoreErrorsStayInTheLog(t *testing.T) {
	var logs bytes.Buffer
	h := api.New(brokenStore{newFakeStore()}, nil, nil, slog.New(slog.NewJSONHandler(&logs, nil)))
	rec := do(t, h, "GET", "/v1/workflows/wf-1", "", "")
	wantProblem(t, rec, http.StatusInternalServerError)
	if strings.Contains(rec.Body.String(), "secret_user") {
		t.Fatalf("response leaks the store error: %s", rec.Body)
	}
	if !strings.Contains(logs.String(), "secret_user") {
		t.Fatalf("the store error is not logged:\n%s", logs.String())
	}
}

func TestRequestLog(t *testing.T) {
	var logs bytes.Buffer
	h := api.New(newFakeStore(), nil, nil, slog.New(slog.NewJSONHandler(&logs, nil)))
	do(t, h, "GET", "/v1/workflows/wf-missing", "", "")
	do(t, h, "GET", "/healthz", "", "") // debug level: not in an info log

	var line struct {
		Msg    string `json:"msg"`
		Method string `json:"method"`
		Path   string `json:"path"`
		Route  string `json:"route"`
		Status int    `json:"status"`
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1:\n%s", len(lines), logs.String())
	}
	if err := json.Unmarshal([]byte(lines[0]), &line); err != nil {
		t.Fatal(err)
	}
	if line.Msg != "http request" || line.Method != "GET" || line.Path != "/v1/workflows/wf-missing" ||
		line.Route != "GET /v1/workflows/{id}" || line.Status != http.StatusNotFound {
		t.Fatalf("unexpected log line: %s", lines[0])
	}
}
