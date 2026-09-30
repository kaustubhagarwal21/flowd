package workflow

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func noop(id string, deps ...string) Step {
	return Step{ID: id, Type: StepNoop, DependsOn: deps}
}

func httpStep(id, method, rawURL string) Step {
	return Step{ID: id, Type: StepHTTP, HTTP: &HTTPSpec{Method: method, URL: rawURL}}
}

// withHeaders returns an http step that sends the given headers.
func withHeaders(headers map[string]string) Step {
	s := httpStep("call", "GET", "http://example.com")
	s.HTTP.Headers = headers
	return s
}

// manySteps returns n independent noop steps named s0, s1, ...
func manySteps(n int) []Step {
	steps := make([]Step, n)
	for i := range steps {
		steps[i] = noop(fmt.Sprintf("s%d", i))
	}
	return steps
}

func TestValidateOrder(t *testing.T) {
	tests := []struct {
		name  string
		steps []Step
		want  []string
	}{
		{"single step", []Step{noop("a")}, []string{"a"}},
		{"independent steps keep definition order", []Step{noop("z"), noop("x"), noop("y")}, []string{"z", "x", "y"}},
		{"chain defined backwards", []Step{noop("c", "b"), noop("b", "a"), noop("a")}, []string{"a", "b", "c"}},
		{
			// Ties between ready steps go to the step defined first.
			"diamond defined out of order",
			[]Step{noop("d", "b", "c"), noop("c", "a"), noop("b", "a"), noop("a")},
			[]string{"a", "c", "b", "d"},
		},
		{
			// "late" becomes ready before "early" is placed, but "early"
			// was defined first, so it goes first.
			"ready steps sorted by definition order",
			[]Step{noop("root"), noop("early"), noop("late", "root")},
			[]string{"root", "early", "late"},
		},
		{
			"fan out and in",
			[]Step{noop("start"), noop("a", "start"), noop("b", "start"), noop("c", "start"), noop("end", "a", "b", "c")},
			[]string{"start", "a", "b", "c", "end"},
		},
		{"maximum number of steps", manySteps(MaxSteps), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Definition{Name: "wf", Steps: tt.steps}
			order, err := Validate(d)
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if tt.want == nil {
				if len(order) != len(tt.steps) {
					t.Fatalf("got %d steps in order, want %d", len(order), len(tt.steps))
				}
				return
			}
			if !reflect.DeepEqual(order, tt.want) {
				t.Fatalf("order = %v, want %v", order, tt.want)
			}
			// The same definition must always give the same order.
			again, err := Validate(d)
			if err != nil || !reflect.DeepEqual(again, order) {
				t.Fatalf("second Validate = %v, %v; want %v", again, err, order)
			}
		})
	}
}

func TestValidateDefaults(t *testing.T) {
	ms := func(d time.Duration) int { return int(d.Milliseconds()) }
	tests := []struct {
		name        string
		step        Step
		wantRetry   RetryPolicy
		wantTimeout int
	}{
		{
			name:        "zero values get defaults",
			step:        noop("a"),
			wantRetry:   RetryPolicy{MaxAttempts: 3, InitialBackoffMS: 200, MaxBackoffMS: 30000},
			wantTimeout: 30000,
		},
		{
			name: "explicit values are kept",
			step: Step{ID: "a", Type: StepNoop, TimeoutMS: 1500,
				Retry: RetryPolicy{MaxAttempts: 5, InitialBackoffMS: 50, MaxBackoffMS: 2000}},
			wantRetry:   RetryPolicy{MaxAttempts: 5, InitialBackoffMS: 50, MaxBackoffMS: 2000},
			wantTimeout: 1500,
		},
		{
			name: "values above the caps are clamped",
			step: Step{ID: "a", Type: StepNoop, TimeoutMS: ms(time.Hour),
				Retry: RetryPolicy{MaxAttempts: 99, InitialBackoffMS: ms(time.Hour), MaxBackoffMS: ms(time.Hour)}},
			wantRetry:   RetryPolicy{MaxAttempts: MaxAttemptsCap, InitialBackoffMS: ms(MaxBackoffCap), MaxBackoffMS: ms(MaxBackoffCap)},
			wantTimeout: ms(MaxTimeout),
		},
		{
			name:        "initial backoff above the max is lowered to the max",
			step:        Step{ID: "a", Type: StepNoop, Retry: RetryPolicy{InitialBackoffMS: 60000}},
			wantRetry:   RetryPolicy{MaxAttempts: 3, InitialBackoffMS: 30000, MaxBackoffMS: 30000},
			wantTimeout: 30000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Definition{Name: "wf", Steps: []Step{tt.step}}
			if _, err := Validate(d); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			got := d.Steps[0]
			if got.Retry != tt.wantRetry {
				t.Errorf("retry = %+v, want %+v", got.Retry, tt.wantRetry)
			}
			if got.TimeoutMS != tt.wantTimeout {
				t.Errorf("timeout_ms = %d, want %d", got.TimeoutMS, tt.wantTimeout)
			}
		})
	}
}

func TestValidateHTTP(t *testing.T) {
	d := &Definition{Name: "wf", Steps: []Step{httpStep("call", "post", "https://example.com/hook?x=1")}}
	if _, err := Validate(d); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := d.Steps[0].HTTP.Method; got != "POST" {
		t.Fatalf("method = %q, want it upper-cased to POST", got)
	}
}

// The header rules must match Go's http client exactly: rejecting a header
// the client would send breaks working workflows, and accepting one it
// refuses gives a step that can never succeed. Each case goes through both.
func TestHeaderRulesMatchGoClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(srv.Close)

	cases := []struct{ name, value string }{
		{"X-Api-Key", "k-123"},
		{"x-lower-case", "v"},
		{"X-Every_Token.Char!#$%&'*+^`|~9", "v"},
		{"X-Tab", "a\tb"},
		{"X-Spaces", " a b "},
		{"X-Empty", ""},
		{"X-Non-ASCII-Value", "café ✓"},
		{"Bad Header", "v"},
		{"X-A:B", "v"},
		{"X-A\r\nX-B", "v"},
		{"X-Café", "v"},
		{"", "v"},
		{"X-(Paren)", "v"},
		{"X-CR", "a\rb"},
		{"X-LF", "a\nb"},
		{"X-NUL", "a\x00b"},
		{"X-Ctrl", "a\x01b"},
		{"X-DEL", "a\x7fb"},
	}
	for _, tc := range cases {
		_, verr := Validate(&Definition{Steps: []Step{withHeaders(map[string]string{tc.name: tc.value})}})

		// Set the header the way the http executor does.
		req, err := http.NewRequest("GET", srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(tc.name, tc.value)
		resp, cerr := srv.Client().Do(req)
		if cerr == nil {
			resp.Body.Close()
		}
		if (verr == nil) != (cerr == nil) {
			t.Errorf("header %q: %q: Validate error = %v, but Go's client error = %v", tc.name, tc.value, verr, cerr)
		}
	}
}

func TestValidateErrors(t *testing.T) {
	tooManyDeps := manySteps(MaxDependsOn + 2)
	for i := 1; i < len(tooManyDeps); i++ {
		tooManyDeps[0].DependsOn = append(tooManyDeps[0].DependsOn, tooManyDeps[i].ID)
	}
	withRetry := func(r RetryPolicy) Step { s := noop("a"); s.Retry = r; return s }

	tests := []struct {
		name      string
		steps     []Step
		wantField string
		wantMsg   string
	}{
		{"no steps", nil, "steps", "at least one step"},
		{"too many steps", manySteps(MaxSteps + 1), "steps", "at most 100"},

		{"empty id", []Step{noop("")}, "steps[0].id", "is required"},
		{"upper-case id", []Step{noop("Build")}, "steps[0].id", `"Build" must match`},
		{"id with a space", []Step{noop("a b")}, "steps[0].id", "must match"},
		{"id too long", []Step{noop(strings.Repeat("a", 65))}, "steps[0].id", "must match"},
		{"duplicate id", []Step{noop("a"), noop("b"), noop("a")}, "steps[2].id", `duplicate step id "a" (already used by steps[0])`},

		{"missing type", []Step{{ID: "a"}}, "steps[0].type", `step "a": type is required`},
		{"unknown type", []Step{{ID: "a", Type: "shell"}}, "steps[0].type", `unknown type "shell"`},

		{"http without spec", []Step{{ID: "a", Type: StepHTTP}}, "steps[0].http", "http is required"},
		{"http with noop spec", []Step{{ID: "a", Type: StepHTTP, HTTP: &HTTPSpec{Method: "GET", URL: "http://x"}, Noop: &NoopSpec{}}}, "steps[0].noop", "must be empty"},
		{"noop with http spec", []Step{{ID: "a", Type: StepNoop, HTTP: &HTTPSpec{}}}, "steps[0].http", "must be empty"},
		{"bad method", []Step{httpStep("a", "FETCH", "http://x")}, "steps[0].http.method", `method "FETCH" is not one of`},
		{"empty method", []Step{httpStep("a", "", "http://x")}, "steps[0].http.method", `method ""`},
		{"relative url", []Step{httpStep("a", "GET", "/hook")}, "steps[0].http.url", "not an absolute http or https URL"},
		{"ftp url", []Step{httpStep("a", "GET", "ftp://example.com/f")}, "steps[0].http.url", "not an absolute"},
		{"url without host", []Step{httpStep("a", "GET", "http://:8080/x")}, "steps[0].http.url", "not an absolute"},
		{"unparsable url", []Step{httpStep("a", "GET", "http://[::1")}, "steps[0].http.url", "not an absolute"},

		// Go's http client refuses these headers, so no attempt could ever
		// reach the target. The message names the header.
		{"header name with a space", []Step{withHeaders(map[string]string{"Bad Header": "v"})}, "steps[0].http.headers", `header name "Bad Header" is not a valid HTTP token`},
		{"header name with a colon", []Step{withHeaders(map[string]string{"X-A:B": "v"})}, "steps[0].http.headers", `"X-A:B"`},
		{"header name with CRLF", []Step{withHeaders(map[string]string{"X-A\r\nX-B": "v"})}, "steps[0].http.headers", `"X-A\r\nX-B"`},
		{"non-ASCII header name", []Step{withHeaders(map[string]string{"X-Café": "v"})}, "steps[0].http.headers", `"X-Café"`},
		{"empty header name", []Step{withHeaders(map[string]string{"": "v"})}, "steps[0].http.headers", `header name "" is not`},
		{"header value with CR", []Step{withHeaders(map[string]string{"X-Token": "a\rb"})}, "steps[0].http.headers", `header "X-Token" contains a control character`},
		{"header value with LF", []Step{withHeaders(map[string]string{"X-Token": "a\nX-Injected: 1"})}, "steps[0].http.headers", `header "X-Token" contains a control character`},
		{"header value with NUL", []Step{withHeaders(map[string]string{"X-Token": "a\x00b"})}, "steps[0].http.headers", `header "X-Token" contains a control character`},
		{"header value with DEL", []Step{withHeaders(map[string]string{"X-Token": "a\x7fb"})}, "steps[0].http.headers", `header "X-Token"`},
		{
			// With several bad headers the first in sorted order is named,
			// so the error does not depend on map iteration order.
			"first bad header in sorted order",
			[]Step{withHeaders(map[string]string{"Z Bad": "v", "A Bad": "v", "M-Bad": "\n"})},
			"steps[0].http.headers", `"A Bad"`,
		},

		{"negative sleep", []Step{{ID: "a", Type: StepNoop, Noop: &NoopSpec{SleepMS: -1}}}, "steps[0].noop.sleep_ms", "between 0 and"},
		{"sleep above max timeout", []Step{{ID: "a", Type: StepNoop, Noop: &NoopSpec{SleepMS: int(MaxTimeout.Milliseconds()) + 1}}}, "steps[0].noop.sleep_ms", "between 0 and"},
		{"negative fail_times", []Step{{ID: "a", Type: StepNoop, Noop: &NoopSpec{FailTimes: -1}}}, "steps[0].noop.fail_times", "must not be negative"},
		{"negative max_attempts", []Step{withRetry(RetryPolicy{MaxAttempts: -1})}, "steps[0].retry.max_attempts", "must not be negative"},
		{"negative initial backoff", []Step{withRetry(RetryPolicy{InitialBackoffMS: -5})}, "steps[0].retry.initial_backoff_ms", "must not be negative"},
		{"negative max backoff", []Step{withRetry(RetryPolicy{MaxBackoffMS: -5})}, "steps[0].retry.max_backoff_ms", "must not be negative"},
		{"negative timeout", []Step{{ID: "a", Type: StepNoop, TimeoutMS: -1}}, "steps[0].timeout_ms", "must not be negative"},

		{"unknown dependency", []Step{noop("a"), noop("b", "nope")}, "steps[1].depends_on", `step "b" depends on unknown step "nope"`},
		{"self dependency", []Step{noop("a", "a")}, "steps[0].depends_on", `step "a" depends on itself`},
		{"duplicate dependency", []Step{noop("a"), noop("b", "a", "a")}, "steps[1].depends_on", `lists dependency "a" twice`},
		{"too many dependencies", tooManyDeps, "steps[0].depends_on", "at most 32 are allowed"},

		{"two-step cycle", []Step{noop("a", "b"), noop("b", "a")}, "steps[0].depends_on", "dependency cycle: a -> b -> a"},
		{
			"cycle behind a valid root",
			[]Step{noop("root"), noop("a", "root", "c"), noop("b", "a"), noop("c", "b")},
			"steps[1].depends_on", "dependency cycle: a -> c -> b -> a",
		},
		{
			// "tail" cannot run because of the cycle, but it is not part of it.
			"step downstream of a cycle is not reported as a member",
			[]Step{noop("tail", "c"), noop("a", "c"), noop("b", "a"), noop("c", "b")},
			"steps[3].depends_on", "dependency cycle: c -> b -> a -> c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := Validate(&Definition{Name: "wf", Steps: tt.steps})
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("Validate = %v, %v; want a *ValidationError", order, err)
			}
			if ve.Field != tt.wantField || !strings.Contains(ve.Message, tt.wantMsg) {
				t.Fatalf("error = %q; want field %q and a message containing %q", err, tt.wantField, tt.wantMsg)
			}
			if order != nil {
				t.Fatalf("order = %v, want nil on error", order)
			}
		})
	}
}

func TestValidateNil(t *testing.T) {
	_, err := Validate(nil)
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Field != "definition" {
		t.Fatalf("Validate(nil) = %v; want a definition error", err)
	}
}

func TestValidationErrorText(t *testing.T) {
	err := &ValidationError{Field: "steps[2].depends_on", Message: `step "c" depends on itself`}
	want := `steps[2].depends_on: step "c" depends on itself`
	if err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
}

// Validating an already valid definition must not write to it, because
// callers may share a definition between goroutines. Under -race, any write
// made by the concurrent Validate calls below is reported as a data race.
func TestValidateIsIdempotent(t *testing.T) {
	d := &Definition{Name: "wf", Steps: []Step{
		httpStep("call", "get", "http://example.com"),
		{ID: "wait", Type: StepNoop, DependsOn: []string{"call"}, Retry: RetryPolicy{MaxAttempts: 50}},
	}}
	if _, err := Validate(d); err != nil {
		t.Fatalf("first Validate: %v", err)
	}
	before := fmt.Sprintf("%+v %+v", d.Steps, *d.Steps[0].HTTP)

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if _, err := Validate(d); err != nil {
				t.Errorf("Validate again: %v", err)
			}
		})
	}
	wg.Wait()

	if after := fmt.Sprintf("%+v %+v", d.Steps, *d.Steps[0].HTTP); after != before {
		t.Fatalf("Validate changed a valid definition:\n before %s\n after  %s", before, after)
	}
}
