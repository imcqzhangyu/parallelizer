package cli

import (
	"fmt"
	"strconv"
	"strings"
)

// When controls whether running jobs are killed or allowed to finish.
type When int

const (
	Never When = iota
	Soon
	Now
)

func (w When) String() string {
	switch w {
	case Now:
		return "now"
	case Soon:
		return "soon"
	default:
		return "never"
	}
}

// On selects the counter that triggers the halt.
type On int

const (
	OnFail On = iota
	OnSuccess
	OnDone
)

func (o On) String() string {
	switch o {
	case OnSuccess:
		return "success"
	case OnDone:
		return "done"
	default:
		return "fail"
	}
}

// HaltRule is the parsed form of --halt.
type HaltRule struct {
	Set   bool
	When  When
	On    On
	Count int // absolute number of jobs

	// Pct is the percentage as a fraction in (0,1]; PctPPM is the same value
	// in parts-per-million and is what ShouldHalt/ExitCode use, so that
	// thresholds like "7% of 100" are exact integers instead of floating-point
	// approximations (7% of 100 is 7.0000000000000009 as a float64).
	// Pct and PctPPM are mutually exclusive with Count.
	Pct    float64
	pctPPM int64
}

// ParseHalt accepts the GNU parallel syntax:
//
//	never | now | soon
//	now,fail=1 | soon,fail=3 | now,success=2 | soon,done=3
//	now,fail=5% | soon,success=10%
//	0 1 2 -1 -2 (legacy)
//	5% (short for soon,fail=5%)
func ParseHalt(s string) (HaltRule, error) {
	s = strings.TrimSpace(s)
	switch s {
	case "", "never", "0":
		return HaltRule{When: Never}, nil
	}

	// Legacy numeric forms.
	legacy := map[string]string{
		"1":  "soon,fail=1",
		"2":  "now,fail=1",
		"-1": "soon,success=1",
		"-2": "now,success=1",
	}
	if v, ok := legacy[s]; ok {
		s = v
	} else if strings.HasSuffix(s, "%") && !strings.Contains(s, ",") {
		s = "soon,fail=" + s
	}

	parts := strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == '=' })
	if len(parts) != 3 {
		return HaltRule{}, fmt.Errorf("invalid --halt %q: expected WHEN,WHAT=N", s)
	}

	var w When
	switch parts[0] {
	case "now":
		w = Now
	case "soon":
		w = Soon
	case "never":
		return HaltRule{When: Never}, nil
	default:
		return HaltRule{}, fmt.Errorf("invalid --halt %q: WHEN must be now, soon or never", s)
	}

	var on On
	switch parts[1] {
	case "fail":
		on = OnFail
	case "success":
		on = OnSuccess
	case "done":
		on = OnDone
	default:
		return HaltRule{}, fmt.Errorf("invalid --halt %q: WHAT must be fail, success or done", s)
	}

	r := HaltRule{Set: true, When: w, On: on}
	if strings.HasSuffix(parts[2], "%") {
		v, err := strconv.Atoi(strings.TrimSuffix(parts[2], "%"))
		if err != nil || v < 1 || v > 100 {
			return HaltRule{}, fmt.Errorf("invalid --halt percentage %q: want 1%%..100%%", parts[2])
		}
		r.Pct = float64(v) / 100
		r.pctPPM = int64(v) * 10000
	} else {
		v, err := strconv.Atoi(parts[2])
		if err != nil || v < 1 {
			return HaltRule{}, fmt.Errorf("invalid --halt count %q: want a positive integer", parts[2])
		}
		r.Count = v
	}
	return r, nil
}

// ShouldHalt reports whether the rule has been satisfied.
// total is only meaningful for percentage rules (0 means unknown).
func (r HaltRule) ShouldHalt(failed, succeeded, finished, total int) bool {
	if !r.Set || r.When == Never {
		return false
	}
	var n int
	switch r.On {
	case OnFail:
		n = failed
	case OnSuccess:
		n = succeeded
	case OnDone:
		n = finished
	}
	if r.pctPPM > 0 {
		if total <= 0 {
			return false
		}
		// n >= total * pct, all integer: pct = pctPPM / 1e6.
		return int64(n)*1_000_000 >= int64(total)*r.pctPPM
	}
	return n >= r.Count
}

// ExitCode computes the process exit status once the rule has fired.
// lastExit/lastSignal describe the job that triggered the halt.
func (r HaltRule) ExitCode(failed, total, lastExit, lastSignal int) int {
	if !r.Set || r.When == Never {
		return cap101(failed)
	}
	if r.On == OnSuccess {
		return 0
	}
	if r.pctPPM > 0 {
		// Like GNU parallel: the exit status is the number of failed jobs.
		return cap101(failed)
	}
	if r.Count == 1 {
		if lastSignal != 0 {
			return 128 + lastSignal
		}
		if lastExit != 0 {
			return lastExit
		}
		if r.On == OnFail {
			return 1
		}
		return 0
	}
	return cap101(failed)
}

func cap101(n int) int {
	switch {
	case n > 101:
		return 101
	case n < 0:
		return 0
	default:
		return n
	}
}
