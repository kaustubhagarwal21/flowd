package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

// maxBodyBytes caps request bodies. A workflow at the 100-step limit fits
// easily; the cap stops a client from making the server buffer a huge body.
const maxBodyBytes = 1 << 20 // 1 MiB

// readJSON decodes the request body into dst. It enforces, in order, the
// JSON content type (415), the size cap (413), and exactly one well-formed
// JSON value without unknown fields (400). On failure it has already written
// the problem response and returns false.
func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeProblem(w, r, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}

	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	// Rejecting unknown fields turns a typo such as "depends_no" into a clear
	// 400 instead of a setting that is silently ignored.
	dec.DisallowUnknownFields()
	err = dec.Decode(dst)
	if err == nil {
		// Anything after the first value, such as "{} {}", is a client bug.
		if err = dec.Decode(&json.RawMessage{}); err == nil {
			err = errors.New("body must contain a single JSON value")
		} else if errors.Is(err, io.EOF) {
			return true
		}
	}

	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		writeProblem(w, r, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("request body is larger than %d bytes", tooLarge.Limit))
	case errors.Is(err, io.EOF):
		writeProblem(w, r, http.StatusBadRequest, "request body is empty")
	default:
		writeProblem(w, r, http.StatusBadRequest, "invalid JSON body: "+strings.TrimPrefix(err.Error(), "json: "))
	}
	return false
}

// writeJSON sends v as JSON. It encodes before writing the status line, so
// an encoding failure can still become a clean 500.
func (s *server) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		s.internalError(w, r, fmt.Errorf("encode response: %w", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(append(body, '\n'))
}

// problem is an RFC 7807 "problem details" body. Its type is always
// "about:blank", which says the HTTP status explains the problem, so the
// title is the standard status text and detail carries the specifics.
type problem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Instance string `json:"instance,omitempty"`
	// Field is an extension member (RFC 7807 section 3.2) naming the part of
	// a workflow definition that failed validation, e.g. "steps[2].depends_on".
	Field string `json:"field,omitempty"`
}

func newProblem(r *http.Request, status int, detail string) *problem {
	return &problem{
		Type:     "about:blank",
		Title:    http.StatusText(status),
		Status:   status,
		Detail:   detail,
		Instance: r.URL.Path,
	}
}

func (p *problem) write(w http.ResponseWriter) {
	body, _ := json.Marshal(p) // cannot fail: only strings and an int
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	w.Write(append(body, '\n'))
}

// writeProblem sends a problem response with the given status and detail.
func writeProblem(w http.ResponseWriter, r *http.Request, status int, detail string) {
	newProblem(r, status, detail).write(w)
}
