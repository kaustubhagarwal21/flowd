package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

// problemMux serves requests through an http.ServeMux, but turns the mux's
// built-in plain-text 404 and 405 answers into problem+json. The mux still
// decides which of the two applies (and sets the Allow header for 405), so
// the routing rules live in one place.
type problemMux struct{ mux *http.ServeMux }

func (p problemMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h, pattern := p.mux.Handler(r)
	if pattern == "" {
		// No route matched: h is the mux's own 404 or 405 handler.
		h.ServeHTTP(&routeErrorWriter{ResponseWriter: w, r: r}, r)
		return
	}
	// Serve through the mux rather than h, because the mux is what fills in
	// r.PathValue("id") for the handler.
	p.mux.ServeHTTP(w, r)
}

// routeErrorWriter replaces the body of a 404 or 405 with a problem. Any
// other status (such as a redirect to a cleaned-up path) passes through.
type routeErrorWriter struct {
	http.ResponseWriter
	r        *http.Request
	replaced bool
}

func (w *routeErrorWriter) WriteHeader(status int) {
	if status != http.StatusNotFound && status != http.StatusMethodNotAllowed {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	w.replaced = true
	detail := fmt.Sprintf("no endpoint at %s", w.r.URL.Path)
	if status == http.StatusMethodNotAllowed {
		detail = fmt.Sprintf("method %s is not allowed on %s; allowed: %s",
			w.r.Method, w.r.URL.Path, w.Header().Get("Allow"))
	}
	writeProblem(w.ResponseWriter, w.r, status, detail)
}

func (w *routeErrorWriter) Write(b []byte) (int, error) {
	if w.replaced {
		return len(b), nil // drop the mux's plain-text body
	}
	return w.ResponseWriter.Write(b)
}

// statusRecorder remembers the status and size of a response. The access log
// reports them, and recoverPanics uses them to tell whether it is too late
// to send an error.
type statusRecorder struct {
	http.ResponseWriter
	status int // 0 until the handler writes something
	bytes  int
}

func (rec *statusRecorder) WriteHeader(status int) {
	if rec.status == 0 {
		rec.status = status
	}
	rec.ResponseWriter.WriteHeader(status)
}

func (rec *statusRecorder) Write(b []byte) (int, error) {
	if rec.status == 0 {
		rec.status = http.StatusOK // an implicit WriteHeader(200)
	}
	n, err := rec.ResponseWriter.Write(b)
	rec.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the real writer.
func (rec *statusRecorder) Unwrap() http.ResponseWriter { return rec.ResponseWriter }

// logRequests writes one structured log line per request.
func logRequests(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		if rec.status == 0 {
			rec.status = http.StatusOK // the handler wrote nothing at all
		}
		level := slog.LevelInfo
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			// Probes and scrapes arrive every few seconds and would drown out
			// the interesting lines, so they only show at debug level.
			level = slog.LevelDebug
		}
		log.LogAttrs(r.Context(), level, "http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("route", r.Pattern), // set by the mux; low-cardinality
			slog.Int("status", rec.status),
			slog.Int("bytes", rec.bytes),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

// recoverPanics turns a panicking handler into a 500 problem, so one bad
// request cannot crash the process or leave the client without an answer.
func recoverPanics(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w}
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if v == http.ErrAbortHandler {
				panic(v) // a deliberate abort; net/http handles it quietly
			}
			log.Error("handler panic", "method", r.Method, "path", r.URL.Path,
				"panic", fmt.Sprint(v), "stack", string(debug.Stack()))
			if rec.status != 0 {
				// Part of the response is already on the wire, so a clean 500
				// is impossible. Abort the connection so the client sees a
				// failure rather than a truncated success.
				panic(http.ErrAbortHandler)
			}
			writeProblem(w, r, http.StatusInternalServerError, "the server could not complete the request")
		}()
		next.ServeHTTP(rec, r)
	})
}
