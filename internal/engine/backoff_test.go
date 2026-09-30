package engine_test

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/kaustubhagarwal21/flowd/internal/engine"
)

// almostOne is the largest float64 below 1, the most rnd may return.
const almostOne = 1 - 0x1p-53

func TestBackoffCeilingDoublesUntilCapped(t *testing.T) {
	const initial, maxDelay = 100 * time.Millisecond, 1 * time.Second
	top := func() float64 { return almostOne }
	tests := []struct {
		attempt int
		ceiling time.Duration
	}{
		{0, 100 * time.Millisecond}, // treated as attempt 1
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1 * time.Second}, // 1.6s capped
		{50, 1 * time.Second},
	}
	for _, tt := range tests {
		got := engine.Backoff(tt.attempt, initial, maxDelay, top)
		// The top of the window is ceiling*(1-2^-53), a few ns below ceiling.
		if got > tt.ceiling || got < tt.ceiling-time.Microsecond {
			t.Errorf("Backoff(%d) at rnd=max = %v, want just under %v", tt.attempt, got, tt.ceiling)
		}
		if got := engine.Backoff(tt.attempt, initial, maxDelay, func() float64 { return 0 }); got != 0 {
			t.Errorf("Backoff(%d) at rnd=0 = %v, want 0", tt.attempt, got)
		}
	}
}

func TestBackoffFullJitterStaysInWindow(t *testing.T) {
	const initial, maxDelay = 50 * time.Millisecond, 3 * time.Second
	r := rand.New(rand.NewPCG(1, 2))
	for attempt := 1; attempt <= 12; attempt++ {
		ceiling := min(maxDelay, initial<<(attempt-1))
		var lo, hi time.Duration = math.MaxInt64, 0
		for range 2000 {
			d := engine.Backoff(attempt, initial, maxDelay, r.Float64)
			if d < 0 || d > ceiling {
				t.Fatalf("attempt %d: %v outside [0, %v]", attempt, d, ceiling)
			}
			lo, hi = min(lo, d), max(hi, d)
		}
		// Full jitter uses the whole window, not just its top or bottom.
		if lo > ceiling/10 || hi < ceiling*9/10 {
			t.Errorf("attempt %d: samples span [%v, %v], want most of [0, %v]", attempt, lo, hi, ceiling)
		}
	}
}

func TestBackoffCeilingIsMonotone(t *testing.T) {
	top := func() float64 { return almostOne }
	prev := time.Duration(0)
	for attempt := 1; attempt <= 200; attempt++ {
		got := engine.Backoff(attempt, 7*time.Millisecond, 10*time.Minute, top)
		if got < prev {
			t.Fatalf("ceiling went down at attempt %d: %v < %v", attempt, got, prev)
		}
		prev = got
	}
}

func TestBackoffDoesNotOverflow(t *testing.T) {
	top := func() float64 { return almostOne }
	for _, attempt := range []int{62, 63, 64, 65, 1000, math.MaxInt} {
		got := engine.Backoff(attempt, time.Second, time.Hour, top)
		if got <= 0 || got > time.Hour {
			t.Errorf("Backoff(%d) = %v, want in (0, 1h]", attempt, got)
		}
	}
	// The largest possible cap must not overflow either.
	if got := engine.Backoff(math.MaxInt, time.Nanosecond, math.MaxInt64, top); got <= 0 {
		t.Errorf("Backoff with max=MaxInt64 = %v, want > 0", got)
	}
}

func TestBackoffEdgeCases(t *testing.T) {
	top := func() float64 { return almostOne }
	if got := engine.Backoff(3, 0, time.Second, top); got != 0 {
		t.Errorf("zero initial: got %v, want 0", got)
	}
	if got := engine.Backoff(3, time.Second, 0, top); got != 0 {
		t.Errorf("zero max: got %v, want 0", got)
	}
	if got := engine.Backoff(1, 5*time.Second, time.Second, top); got > time.Second {
		t.Errorf("initial above max: got %v, want <= 1s", got)
	}
	if got := engine.Backoff(-4, time.Second, time.Minute, top); got > time.Second {
		t.Errorf("negative attempt: got %v, want <= 1s (treated as attempt 1)", got)
	}
}
