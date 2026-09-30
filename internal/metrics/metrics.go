// Package metrics exposes flowd's Prometheus metrics. Every method is safe to
// call on a nil *Metrics, so tests can pass nil.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds flowd's collectors in their own registry.
type Metrics struct {
	registry      *prometheus.Registry
	runsStarted   prometheus.Counter
	runsFinished  *prometheus.CounterVec
	stepsExecuted *prometheus.CounterVec
	stepDuration  *prometheus.HistogramVec
	claimErrors   prometheus.Counter
}

// New creates and registers flowd's metrics, plus the Go runtime and
// process collectors.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		runsStarted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "flowd_runs_started_total",
			Help: "Runs created.",
		}),
		runsFinished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "flowd_runs_finished_total",
			Help: "Runs that reached a final status, by status.",
		}, []string{"status"}),
		stepsExecuted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "flowd_steps_executed_total",
			Help: "Step attempts executed, by step type and result (success, retry, failure, lost, cancelled).",
		}, []string{"type", "result"}),
		stepDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "flowd_step_duration_seconds",
			Help:    "Wall time of one step attempt, by step type.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 10), // 1 ms .. ~262 s
		}, []string{"type"}),
		claimErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "flowd_claim_errors_total",
			Help: "Errors while claiming steps from the store.",
		}),
	}
	m.registry.MustRegister(
		m.runsStarted, m.runsFinished, m.stepsExecuted, m.stepDuration, m.claimErrors,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// RunStarted counts a newly created run.
func (m *Metrics) RunStarted() {
	if m == nil {
		return
	}
	m.runsStarted.Inc()
}

// RunFinished counts a run that reached a final status.
func (m *Metrics) RunFinished(status string) {
	if m == nil {
		return
	}
	m.runsFinished.WithLabelValues(status).Inc()
}

// StepExecuted records one step attempt and how long it took.
func (m *Metrics) StepExecuted(stepType, result string, d time.Duration) {
	if m == nil {
		return
	}
	m.stepsExecuted.WithLabelValues(stepType, result).Inc()
	m.stepDuration.WithLabelValues(stepType).Observe(d.Seconds())
}

// ClaimError counts a failed claim attempt.
func (m *Metrics) ClaimError() {
	if m == nil {
		return
	}
	m.claimErrors.Inc()
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
