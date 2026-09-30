// Package executor implements the step types: http and noop.
//
// The http executor sends Idempotency-Key: <run_id>/<step_id> (stable across
// retries), treats 2xx as success, wraps non-retryable responses (4xx other
// than 408 and 429) with engine.Permanent, and stores at most 16 KiB of the
// response body as the step output.
package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/engine"
	"github.com/kaustubhagarwal21/flowd/internal/store"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// MaxOutputBytes caps how much of a response body is kept as step output.
const MaxOutputBytes = 16 << 10

// errorBodyBytes is how much of an error response goes into the error
// message (and so into the step's last_error).
const errorBodyBytes = 256

type httpExecutor struct{ client *http.Client }

// NewHTTP returns the executor for "http" steps. client may be nil
// (http.DefaultClient's transport is used, with no client-level timeout:
// the step's context carries the deadline).
func NewHTTP(client *http.Client) engine.Executor {
	if client == nil {
		client = &http.Client{}
	}
	return &httpExecutor{client: client}
}

// httpOutput is the step output of a successful http step.
type httpOutput struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"` // JSON if the body was valid JSON, else a JSON string
}

func (h *httpExecutor) Execute(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
	spec := c.Step.HTTP
	if spec == nil {
		return nil, engine.Permanent(errors.New("http step has no http settings"))
	}
	var body io.Reader
	// A JSON null body means "no body", the same as leaving it out.
	if len(spec.Body) > 0 && string(spec.Body) != "null" {
		body = bytes.NewReader(spec.Body)
	}
	req, err := http.NewRequestWithContext(ctx, spec.Method, spec.URL, body)
	if err != nil {
		return nil, engine.Permanent(fmt.Errorf("building request: %w", err))
	}
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return nil, engine.Permanent(fmt.Errorf("unsupported URL scheme %q", req.URL.Scheme))
	}
	for k, v := range spec.Headers {
		req.Header.Set(k, v)
	}
	if body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	// Set last so a user header cannot replace it: the key must stay the
	// same across retries and re-runs after a crash, so the target can
	// recognise a repeated call and not act on it twice.
	req.Header.Set("Idempotency-Key", c.RunID+"/"+c.StepID)

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err // network errors and timeouts are worth retrying
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, MaxOutputBytes))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		err := statusError(resp.Status, data)
		if retryableStatus(resp.StatusCode) {
			return nil, err
		}
		return nil, engine.Permanent(err)
	}
	if readErr != nil {
		return nil, fmt.Errorf("reading response body: %w", readErr)
	}
	return marshalOutput(resp.StatusCode, data)
}

// retryableStatus reports whether a failed response may succeed if repeated:
// the target timed out (408), asked us to slow down (429), or had a server
// error (5xx). Anything else that is not 2xx (another 4xx, or a redirect the
// client did not follow) means the request itself is wrong, so retrying
// cannot help.
func retryableStatus(code int) bool {
	return code == http.StatusRequestTimeout || code == http.StatusTooManyRequests || code >= 500
}

// statusError describes a non-2xx response, with the start of its body.
func statusError(status string, body []byte) error {
	b := bytes.TrimSpace(body)
	if len(b) == 0 {
		return fmt.Errorf("HTTP %s", status)
	}
	if len(b) > errorBodyBytes {
		b = b[:errorBodyBytes]
	}
	return fmt.Errorf("HTTP %s: %s", status, b)
}

// marshalOutput builds {"status":code,"body":...}. A body that is valid JSON
// is embedded as is; anything else (binary data, or JSON cut short by the
// size cap) is embedded as a string.
//
// PostgreSQL's jsonb cannot store the NUL character (\u0000), and a step
// whose output cannot be saved would be re-run forever. So NUL bytes are
// dropped from the string form, and JSON that contains a \u0000 escape is
// kept as a string too (where the escape is just six ordinary characters).
func marshalOutput(status int, body []byte) (json.RawMessage, error) {
	b := json.RawMessage(body)
	if !json.Valid(body) || bytes.Contains(body, []byte(`\u0000`)) {
		s, err := json.Marshal(strings.ReplaceAll(string(body), "\x00", ""))
		if err != nil {
			return nil, err
		}
		b = s
	}
	return json.Marshal(httpOutput{Status: status, Body: b})
}

type noopExecutor struct{}

// NewNoop returns the executor for "noop" steps.
func NewNoop() engine.Executor { return noopExecutor{} }

// Execute sleeps for sleep_ms (cut short if ctx ends), then fails on purpose
// while the attempt number is at most fail_times, and otherwise succeeds
// with no output.
func (noopExecutor) Execute(ctx context.Context, c *store.Claim) (json.RawMessage, error) {
	var spec workflow.NoopSpec
	if c.Step.Noop != nil {
		spec = *c.Step.Noop
	}
	if spec.SleepMS > 0 {
		t := time.NewTimer(time.Duration(spec.SleepMS) * time.Millisecond)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
		}
	}
	if c.Attempt <= spec.FailTimes {
		return nil, fmt.Errorf("noop: attempt %d fails on purpose (fail_times=%d)", c.Attempt, spec.FailTimes)
	}
	return nil, nil
}
