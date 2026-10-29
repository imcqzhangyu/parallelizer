package cli

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"parallelizer/internal/job"
)

// ParseTermSeq parses "TERM,200,KILL,25" into steps.
// Each pair is a signal name and a millisecond grace period.
// It returns job.SignalStep values directly: cli validates the syntax and
// job owns the signal table, so there is one source of truth.
func ParseTermSeq(s string) ([]job.SignalStep, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	if len(parts) == 0 || len(parts)%2 != 0 {
		return nil, fmt.Errorf("invalid --termseq %q: want SIGNAL,MS pairs", s)
	}
	steps := make([]job.SignalStep, 0, len(parts)/2)
	for i := 0; i < len(parts); i += 2 {
		name := strings.ToUpper(strings.TrimSpace(parts[i]))
		sig, err := job.SignalByName(name)
		if err != nil {
			return nil, fmt.Errorf("invalid --termseq: unknown signal %q", parts[i])
		}
		ms, err := strconv.Atoi(strings.TrimSpace(parts[i+1]))
		if err != nil || ms < 0 {
			return nil, fmt.Errorf("invalid --termseq: bad duration %q", parts[i+1])
		}
		// Guard the ms -> time.Duration conversion: multiplying an oversized
		// value by time.Millisecond would silently wrap to a negative wait.
		if int64(ms) > math.MaxInt64/int64(time.Millisecond) {
			return nil, fmt.Errorf("invalid --termseq: duration too large %q", parts[i+1])
		}
		steps = append(steps, job.SignalStep{Signal: sig, Wait: time.Duration(ms) * time.Millisecond})
	}
	return steps, nil
}
