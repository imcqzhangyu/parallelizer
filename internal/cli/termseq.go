package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"parallelizer/internal/job"
)

// ParseTermSeq parses "TERM,200,KILL,25" into steps.
// Each pair is a signal name and a millisecond grace period.
func ParseTermSeq(s string) ([]SignalStep, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	if len(parts) == 0 || len(parts)%2 != 0 {
		return nil, fmt.Errorf("invalid --termseq %q: want SIGNAL,MS pairs", s)
	}
	steps := make([]SignalStep, 0, len(parts)/2)
	for i := 0; i < len(parts); i += 2 {
		name := strings.ToUpper(strings.TrimSpace(parts[i]))
		if _, err := job.SignalByName(name); err != nil {
			return nil, fmt.Errorf("invalid --termseq: unknown signal %q", parts[i])
		}
		ms, err := strconv.Atoi(strings.TrimSpace(parts[i+1]))
		if err != nil || ms < 0 {
			return nil, fmt.Errorf("invalid --termseq: bad duration %q", parts[i+1])
		}
		steps = append(steps, SignalStep{Name: name, Wait: time.Duration(ms) * time.Millisecond})
	}
	return steps, nil
}
