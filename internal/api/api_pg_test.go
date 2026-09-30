package api_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/kaustubhagarwal21/flowd/internal/api"
	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore/pgtest"
)

// The fake store accepts any bytes, so only the real store shows that input
// jsonb cannot hold gets a 400 instead of reaching the database and failing
// there with a 500.
func TestUnstorableJSONAgainstPostgres(t *testing.T) {
	st := pgtest.New(t)
	var logs bytes.Buffer
	h := api.New(st, nil, nil, slog.New(slog.NewJSONHandler(&logs, nil)))

	var wf store.Workflow
	wantJSON(t, do(t, h, "POST", "/v1/workflows", jsonType, `{"name":"ok","steps":[{"id":"a","type":"noop"}]}`),
		http.StatusCreated, &wf)

	for _, tc := range unstorableWorkflows {
		t.Run("workflow/"+tc.name, func(t *testing.T) {
			wantProblem(t, do(t, h, "POST", "/v1/workflows", jsonType, tc.body), http.StatusBadRequest)
		})
	}
	for _, tc := range unstorableRuns {
		t.Run("run/"+tc.name, func(t *testing.T) {
			wantProblem(t, do(t, h, "POST", "/v1/workflows/"+wf.ID+"/runs", jsonType, tc.body), http.StatusBadRequest)
		})
	}
	runs, _, err := st.ListRuns(context.Background(), store.ListRunsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Errorf("%d runs were created, want none", len(runs))
	}
	if strings.Contains(logs.String(), `"level":"ERROR"`) {
		t.Errorf("a request failed inside the server:\n%s", logs.String())
	}
}

// Unicode that jsonb can store, including a surrogate pair and an escaped
// backslash before "u0000", is accepted and read back unchanged.
func TestStorableUnicodeAgainstPostgres(t *testing.T) {
	h := api.New(pgtest.New(t), nil, nil, nil)
	const text = `café ✓ 😀 \\u0000`
	const want = "café ✓ \U0001F600 \\u0000"

	var wf store.Workflow
	wantJSON(t, do(t, h, "POST", "/v1/workflows", jsonType,
		`{"name":"`+text+`","steps":[{"id":"a","type":"noop"}]}`), http.StatusCreated, &wf)
	var created map[string]string
	wantJSON(t, do(t, h, "POST", "/v1/workflows/"+wf.ID+"/runs", jsonType, `{"input":{"k":"`+text+`"}}`),
		http.StatusAccepted, &created)

	var got store.Workflow
	wantJSON(t, do(t, h, "GET", "/v1/workflows/"+wf.ID, "", ""), http.StatusOK, &got)
	if got.Name != want {
		t.Errorf("workflow name = %q, want %q", got.Name, want)
	}
	var run struct {
		Input map[string]string `json:"input"`
	}
	wantJSON(t, do(t, h, "GET", "/v1/runs/"+created["run_id"], "", ""), http.StatusOK, &run)
	if run.Input["k"] != want {
		t.Errorf("run input = %q, want %q", run.Input["k"], want)
	}
}
