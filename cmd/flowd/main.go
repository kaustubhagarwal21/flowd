// Command flowd runs the workflow engine and its REST API in one process.
//
// Every flag falls back to an environment variable, then to its default:
//
//	-addr          FLOWD_ADDR          HTTP listen address (default ":8080")
//	-database-url  FLOWD_DATABASE_URL  PostgreSQL connection URL (required)
//	-workers       FLOWD_WORKERS       concurrent step executions (default 8)
//	-lease         FLOWD_LEASE         step lease (default 30s)
//	-poll          FLOWD_POLL          idle wait between empty claims (default 200ms)
//	-version                           print the version and exit
//
// SIGINT or SIGTERM starts a graceful shutdown: the server stops accepting
// requests and the engine stops claiming steps, and both let in-flight work
// finish before the process exits.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/api"
	"github.com/kaustubhagarwal21/flowd/internal/engine"
	"github.com/kaustubhagarwal21/flowd/internal/executor"
	"github.com/kaustubhagarwal21/flowd/internal/metrics"
	"github.com/kaustubhagarwal21/flowd/internal/store/pgstore"
	"github.com/kaustubhagarwal21/flowd/internal/workflow"
)

// version is set at build time: go build -ldflags "-X main.version=v0.1.0".
var version = "dev"

// shutdownTimeout bounds how long in-flight HTTP requests get to finish
// after a signal. The engine has its own grace period for in-flight steps.
const shutdownTimeout = 10 * time.Second

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Getenv, os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return // -h: the usage has been printed
	}
	if err != nil {
		os.Exit(2) // parseConfig has printed the problem
	}
	if cfg.showVersion {
		fmt.Println("flowd", version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	// SIGINT is Ctrl+C; SIGTERM is what docker stop and Kubernetes send.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = run(ctx, cfg, log)
	stop()
	if err != nil {
		log.Error("flowd stopped with an error", "err", err)
		os.Exit(1)
	}
	log.Info("flowd stopped")
}

type config struct {
	addr        string
	databaseURL string
	workers     int
	lease       time.Duration
	poll        time.Duration
	showVersion bool
}

// parseConfig reads the flags, falling back to the FLOWD_* environment
// variables and then to the defaults. Like the flag package, it reports any
// problem on output as well as returning it; flag.ErrHelp means -h.
func parseConfig(args []string, getenv func(string) string, output io.Writer) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("flowd", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&cfg.addr, "addr", ":8080", "HTTP listen address (env FLOWD_ADDR)")
	fs.StringVar(&cfg.databaseURL, "database-url", "", "PostgreSQL connection URL, required (env FLOWD_DATABASE_URL)")
	fs.IntVar(&cfg.workers, "workers", 8, "concurrent step executions (env FLOWD_WORKERS)")
	fs.DurationVar(&cfg.lease, "lease", 30*time.Second, "step lease; renewed every lease/3 (env FLOWD_LEASE)")
	fs.DurationVar(&cfg.poll, "poll", 200*time.Millisecond, "idle wait between empty claims (env FLOWD_POLL)")
	fs.BoolVar(&cfg.showVersion, "version", false, "print the version and exit")

	fail := func(err error) (config, error) {
		fmt.Fprintln(output, "flowd:", err)
		return config{}, err
	}

	// Setting a flag from its environment variable before parsing the
	// command line gives the order flag > env > default, and reuses each
	// flag's own parser for ints and durations.
	for _, e := range []struct{ flag, env string }{
		{"addr", "FLOWD_ADDR"},
		{"database-url", "FLOWD_DATABASE_URL"},
		{"workers", "FLOWD_WORKERS"},
		{"lease", "FLOWD_LEASE"},
		{"poll", "FLOWD_POLL"},
	} {
		if v := getenv(e.env); v != "" {
			if err := fs.Set(e.flag, v); err != nil {
				return fail(fmt.Errorf("invalid %s %q: %w", e.env, v, err))
			}
		}
	}
	if err := fs.Parse(args); err != nil {
		return config{}, err // the flag package has printed it, with the usage
	}
	if cfg.showVersion {
		return cfg, nil
	}

	switch {
	case fs.NArg() > 0:
		return fail(fmt.Errorf("unexpected arguments: %q", fs.Args()))
	case cfg.databaseURL == "":
		return fail(errors.New("-database-url (or FLOWD_DATABASE_URL) is required"))
	case cfg.workers < 1:
		return fail(errors.New("-workers must be at least 1"))
	case cfg.lease <= 0:
		return fail(errors.New("-lease must be positive"))
	case cfg.poll <= 0:
		return fail(errors.New("-poll must be positive"))
	}
	return cfg, nil
}

// run connects to PostgreSQL, wires the engine and the API together, and
// serves until ctx is cancelled.
func run(ctx context.Context, cfg config, log *slog.Logger) error {
	st, err := pgstore.Open(ctx, cfg.databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer st.Close()

	m := metrics.New()
	execs := map[workflow.StepType]engine.Executor{
		workflow.StepHTTP: executor.NewHTTP(nil),
		workflow.StepNoop: executor.NewNoop(),
	}
	eng := engine.New(engine.Config{Workers: cfg.workers, Lease: cfg.lease, Poll: cfg.poll}, st, execs, m, log)

	// Listening before anything starts turns a busy port into a clear
	// startup error.
	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler: api.New(st, eng, m, log),
		// Without timeouts, slow or idle clients could hold connections
		// and goroutines forever. ReadHeaderTimeout stops slowloris-style
		// clients; request bodies are small (at most 1 MiB).
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	log.Info("flowd started", "version", version, "addr", ln.Addr().String(),
		"workers", cfg.workers, "lease", cfg.lease.String(), "poll", cfg.poll.String())
	return serve(ctx, srv, ln, eng, log)
}

// runner is what serve needs from the engine, so tests can pass a fake.
type runner interface {
	Run(ctx context.Context) error
}

// serve runs the HTTP server and the engine side by side. When ctx is
// cancelled, or when either of them stops on its own, it shuts both down
// gracefully and returns their errors.
func serve(ctx context.Context, srv *http.Server, ln net.Listener, eng runner, log *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	engineDone := make(chan error, 1)
	go func() {
		err := eng.Run(ctx)
		cancel() // an engine that stops by itself takes the API down too
		engineDone <- err
	}()
	serverDone := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		cancel() // and a server that fails stops the engine
		serverDone <- err
	}()

	<-ctx.Done()
	log.Info("shutting down")

	// Shutdown closes the listener, then waits for in-flight requests. The
	// engine drains its in-flight steps at the same time, since its ctx is
	// already cancelled.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	var errs []error
	if err := srv.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
	}
	if err := <-serverDone; !errors.Is(err, http.ErrServerClosed) {
		errs = append(errs, fmt.Errorf("http server: %w", err))
	}
	if err := <-engineDone; err != nil && !errors.Is(err, context.Canceled) {
		errs = append(errs, fmt.Errorf("engine: %w", err))
	}
	return errors.Join(errs...)
}
