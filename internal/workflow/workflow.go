// Package workflow defines workflow definitions and validates them as DAGs.
package workflow

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
)

// StepType names what a step does when it runs.
type StepType string

const (
	// StepHTTP calls an HTTP endpoint; any 2xx response is success.
	StepHTTP StepType = "http"
	// StepNoop does nothing (optionally sleeps). Useful for demos, tests and benchmarks.
	StepNoop StepType = "noop"
)

// Limits and defaults applied by Validate.
const (
	MaxSteps       = 100
	MaxDependsOn   = 32
	MaxAttemptsCap = 10

	DefaultMaxAttempts    = 3
	DefaultInitialBackoff = 200 * time.Millisecond
	DefaultMaxBackoff     = 30 * time.Second
	MaxBackoffCap         = 10 * time.Minute
	DefaultTimeout        = 30 * time.Second
	MaxTimeout            = 10 * time.Minute
)

// Definition is a workflow as submitted through the API.
type Definition struct {
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}

// Step is one node of the DAG.
type Step struct {
	ID        string      `json:"id"`
	Type      StepType    `json:"type"`
	DependsOn []string    `json:"depends_on,omitempty"`
	HTTP      *HTTPSpec   `json:"http,omitempty"`
	Noop      *NoopSpec   `json:"noop,omitempty"`
	Retry     RetryPolicy `json:"retry"`
	TimeoutMS int         `json:"timeout_ms"`
}

// HTTPSpec configures a StepHTTP step.
type HTTPSpec struct {
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

// NoopSpec configures a StepNoop step.
type NoopSpec struct {
	SleepMS int `json:"sleep_ms,omitempty"`
	// FailTimes makes the first N attempts fail. It exists to demonstrate and
	// test retries without needing a flaky external service.
	FailTimes int `json:"fail_times,omitempty"`
}

// RetryPolicy controls how often and how fast a failed step is retried.
type RetryPolicy struct {
	MaxAttempts      int `json:"max_attempts"`
	InitialBackoffMS int `json:"initial_backoff_ms"`
	MaxBackoffMS     int `json:"max_backoff_ms"`
}

// Timeout is the per-attempt deadline for the step.
func (s Step) Timeout() time.Duration { return time.Duration(s.TimeoutMS) * time.Millisecond }

// InitialBackoff is the base delay before the first retry.
func (r RetryPolicy) InitialBackoff() time.Duration {
	return time.Duration(r.InitialBackoffMS) * time.Millisecond
}

// MaxBackoff caps the delay between retries.
func (r RetryPolicy) MaxBackoff() time.Duration {
	return time.Duration(r.MaxBackoffMS) * time.Millisecond
}

// ValidationError says which field of a definition is wrong and why.
type ValidationError struct {
	Field   string // e.g. "steps[2].depends_on"
	Message string
}

func (e *ValidationError) Error() string { return fmt.Sprintf("%s: %s", e.Field, e.Message) }

func invalid(field, format string, args ...any) *ValidationError {
	return &ValidationError{Field: field, Message: fmt.Sprintf(format, args...)}
}

// idPattern is the allowed shape of a step ID. IDs appear in URLs, logs and
// the Idempotency-Key header, so they are kept short and plain.
var idPattern = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)

// httpMethods are the methods an http step may use.
var httpMethods = []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}

// Validate checks d, fills in defaults (retry policy, timeout) in place, and
// returns the step IDs in a deterministic topological order. Any problem is
// reported as a *ValidationError.
//
// Validate only writes to d when a value actually changes, so validating an
// already valid definition leaves it untouched.
func Validate(d *Definition) ([]string, error) {
	if d == nil {
		return nil, invalid("definition", "is required")
	}
	if len(d.Steps) == 0 {
		return nil, invalid("steps", "at least one step is required")
	}
	if len(d.Steps) > MaxSteps {
		return nil, invalid("steps", "has %d steps; at most %d are allowed", len(d.Steps), MaxSteps)
	}

	// First pass: each step on its own. It also indexes the IDs, so the
	// second pass can resolve dependencies that point forward.
	index := make(map[string]int, len(d.Steps)) // step ID -> position in d.Steps
	for i := range d.Steps {
		if err := checkStep(i, &d.Steps[i]); err != nil {
			return nil, err
		}
		id := d.Steps[i].ID
		if j, dup := index[id]; dup {
			return nil, invalid(fmt.Sprintf("steps[%d].id", i), "duplicate step id %q (already used by steps[%d])", id, j)
		}
		index[id] = i
	}
	for i, s := range d.Steps {
		if err := checkDeps(i, s, index); err != nil {
			return nil, err
		}
	}
	return topoOrder(d.Steps, index)
}

// checkStep validates one step and fills in its defaults.
func checkStep(i int, s *Step) error {
	field := func(name string) string { return fmt.Sprintf("steps[%d].%s", i, name) }

	if s.ID == "" {
		return invalid(field("id"), "is required")
	}
	if !idPattern.MatchString(s.ID) {
		return invalid(field("id"), "%q must match [a-z0-9_-]{1,64}", s.ID)
	}

	switch s.Type {
	case StepHTTP:
		if s.Noop != nil {
			return invalid(field("noop"), "step %q has type http, so noop must be empty", s.ID)
		}
		if err := checkHTTP(field, s.ID, s.HTTP); err != nil {
			return err
		}
	case StepNoop:
		if s.HTTP != nil {
			return invalid(field("http"), "step %q has type noop, so http must be empty", s.ID)
		}
		if n := s.Noop; n != nil {
			if n.SleepMS < 0 || n.SleepMS > ms(MaxTimeout) {
				return invalid(field("noop.sleep_ms"), "step %q: must be between 0 and %d", s.ID, ms(MaxTimeout))
			}
			if n.FailTimes < 0 {
				return invalid(field("noop.fail_times"), "step %q: must not be negative", s.ID)
			}
		}
	case "":
		return invalid(field("type"), "step %q: type is required (http or noop)", s.ID)
	default:
		return invalid(field("type"), "step %q: unknown type %q (want http or noop)", s.ID, s.Type)
	}

	r := &s.Retry
	numbers := []struct {
		name string
		v    int
	}{
		{"retry.max_attempts", r.MaxAttempts},
		{"retry.initial_backoff_ms", r.InitialBackoffMS},
		{"retry.max_backoff_ms", r.MaxBackoffMS},
		{"timeout_ms", s.TimeoutMS},
	}
	for _, n := range numbers {
		if n.v < 0 {
			return invalid(field(n.name), "step %q: must not be negative", s.ID)
		}
	}
	// Zero means "not set". Values above a cap are clamped rather than
	// rejected: the cap is a safety limit, not something users must know.
	setDefault(&r.MaxAttempts, DefaultMaxAttempts, MaxAttemptsCap)
	setDefault(&r.InitialBackoffMS, ms(DefaultInitialBackoff), ms(MaxBackoffCap))
	setDefault(&r.MaxBackoffMS, ms(DefaultMaxBackoff), ms(MaxBackoffCap))
	setDefault(&s.TimeoutMS, ms(DefaultTimeout), ms(MaxTimeout))
	// Backoff is min(max, initial*2^n), so an initial delay above the max
	// behaves exactly like the max. Store the value that takes effect.
	if r.InitialBackoffMS > r.MaxBackoffMS {
		r.InitialBackoffMS = r.MaxBackoffMS
	}
	return nil
}

// checkHTTP validates the http spec of step id.
func checkHTTP(field func(string) string, id string, h *HTTPSpec) error {
	if h == nil {
		return invalid(field("http"), "step %q has type http, so http is required", id)
	}
	if up := strings.ToUpper(h.Method); up != h.Method {
		h.Method = up
	}
	if !slices.Contains(httpMethods, h.Method) {
		return invalid(field("http.method"), "step %q: method %q is not one of %s", id, h.Method, strings.Join(httpMethods, ", "))
	}
	u, err := url.Parse(h.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return invalid(field("http.url"), "step %q: %q is not an absolute http or https URL", id, h.URL)
	}
	return nil
}

// checkDeps validates the depends_on list of step i.
func checkDeps(i int, s Step, index map[string]int) error {
	field := fmt.Sprintf("steps[%d].depends_on", i)
	if len(s.DependsOn) > MaxDependsOn {
		return invalid(field, "step %q has %d dependencies; at most %d are allowed", s.ID, len(s.DependsOn), MaxDependsOn)
	}
	seen := make(map[string]bool, len(s.DependsOn))
	for _, dep := range s.DependsOn {
		if dep == s.ID {
			return invalid(field, "step %q depends on itself", s.ID)
		}
		if seen[dep] {
			return invalid(field, "step %q lists dependency %q twice", s.ID, dep)
		}
		if _, ok := index[dep]; !ok {
			return invalid(field, "step %q depends on unknown step %q", s.ID, dep)
		}
		seen[dep] = true
	}
	return nil
}

// topoOrder orders the steps with Kahn's algorithm: repeatedly place a step
// whose dependencies are all placed. When several steps are ready at once,
// the one defined first goes first, so a definition always gives the same
// order. Steps that can never be placed are on, or behind, a cycle.
func topoOrder(steps []Step, index map[string]int) ([]string, error) {
	waiting := make([]int, len(steps))      // unplaced dependencies per step
	dependents := make([][]int, len(steps)) // step -> steps that depend on it
	for i, s := range steps {
		waiting[i] = len(s.DependsOn)
		for _, dep := range s.DependsOn {
			j := index[dep]
			dependents[j] = append(dependents[j], i)
		}
	}

	var ready []int // steps with nothing left to wait for, sorted by position
	for i := range steps {
		if waiting[i] == 0 {
			ready = append(ready, i)
		}
	}
	order := make([]string, 0, len(steps))
	for len(ready) > 0 {
		i := ready[0]
		ready = ready[1:]
		order = append(order, steps[i].ID)
		for _, j := range dependents[i] {
			waiting[j]--
			if waiting[j] == 0 {
				pos, _ := slices.BinarySearch(ready, j)
				ready = slices.Insert(ready, pos, j)
			}
		}
	}

	if len(order) < len(steps) {
		cycle := findCycle(steps, index, waiting)
		return nil, invalid(fmt.Sprintf("steps[%d].depends_on", index[cycle[0]]),
			"dependency cycle: %s (each step depends on the next)", strings.Join(cycle, " -> "))
	}
	return order, nil
}

// findCycle returns one cycle among the steps Kahn's algorithm could not
// place (waiting > 0), as step IDs with the first repeated at the end. Each
// such step still waits on another unplaced step, so following those
// dependencies must eventually revisit a step; the walk from that step
// onwards is a cycle.
func findCycle(steps []Step, index map[string]int, waiting []int) []string {
	seenAt := make(map[int]int) // step -> its position in path
	var path []int
	i := slices.IndexFunc(waiting, func(w int) bool { return w > 0 })
	for {
		if p, ok := seenAt[i]; ok {
			var ids []string
			for _, j := range path[p:] {
				ids = append(ids, steps[j].ID)
			}
			return append(ids, steps[i].ID)
		}
		seenAt[i] = len(path)
		path = append(path, i)
		for _, dep := range steps[i].DependsOn {
			if j := index[dep]; waiting[j] > 0 {
				i = j
				break
			}
		}
	}
}

// setDefault replaces an unset (zero) value with def and clamps it to max.
// It writes only when the value changes.
func setDefault(v *int, def, max int) {
	switch {
	case *v == 0:
		*v = def
	case *v > max:
		*v = max
	}
}

func ms(d time.Duration) int { return int(d.Milliseconds()) }
