package cli

import (
	"syscall"
	"testing"
	"time"
)

func TestParseHalt(t *testing.T) {
	tests := []struct {
		in      string
		want    HaltRule
		wantErr bool
	}{
		{in: "", want: HaltRule{When: Never}},
		{in: "never", want: HaltRule{When: Never}},
		{in: "0", want: HaltRule{When: Never}},
		{in: "1", want: HaltRule{Set: true, When: Soon, On: OnFail, Count: 1}},
		{in: "2", want: HaltRule{Set: true, When: Now, On: OnFail, Count: 1}},
		{in: "-1", want: HaltRule{Set: true, When: Soon, On: OnSuccess, Count: 1}},
		{in: "-2", want: HaltRule{Set: true, When: Now, On: OnSuccess, Count: 1}},
		{in: "now,fail=1", want: HaltRule{Set: true, When: Now, On: OnFail, Count: 1}},
		{in: "soon,fail=3", want: HaltRule{Set: true, When: Soon, On: OnFail, Count: 3}},
		{in: "now,success=2", want: HaltRule{Set: true, When: Now, On: OnSuccess, Count: 2}},
		{in: "soon,done=4", want: HaltRule{Set: true, When: Soon, On: OnDone, Count: 4}},
		{in: "5%", want: HaltRule{Set: true, When: Soon, On: OnFail, Pct: 0.05, pctPPM: 50000}},
		{in: "now,fail=10%", want: HaltRule{Set: true, When: Now, On: OnFail, Pct: 0.10, pctPPM: 100000}},
		{in: "now", wantErr: true},
		{in: "later,fail=1", wantErr: true},
		{in: "now,maybe=1", wantErr: true},
		{in: "now,fail=0", wantErr: true},
		{in: "now,fail=101%", wantErr: true},
	}
	for _, tc := range tests {
		got, err := ParseHalt(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseHalt(%q): expected error, got %+v", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseHalt(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseHalt(%q) = %+v, want %+v", tc.in, got, tc.want)
		}
	}
}

func TestShouldHalt(t *testing.T) {
	rule := HaltRule{Set: true, When: Now, On: OnFail, Count: 2}
	if rule.ShouldHalt(1, 0, 1, 0) {
		t.Error("must not halt after 1 of 2 failures")
	}
	if !rule.ShouldHalt(2, 0, 2, 0) {
		t.Error("must halt after 2 failures")
	}

	pct := HaltRule{Set: true, When: Soon, On: OnFail, Pct: 0.03, pctPPM: 30000}
	if !pct.ShouldHalt(2, 0, 5, 40) { // 40*0.03 = 1.2
		t.Error("percentage rule should fire at 2/40")
	}
	if pct.ShouldHalt(1, 0, 1, 40) {
		t.Error("percentage rule should not fire at 1/40")
	}
	if pct.ShouldHalt(1, 0, 1, 0) {
		t.Error("percentage rule without a total must not fire")
	}

	never := HaltRule{When: Never}
	if never.ShouldHalt(100, 0, 100, 100) {
		t.Error("never must not halt")
	}
}

// Regression test: exact thresholds must fire at the exact count, not one job
// later. The old float64 comparison made "7% of 100" equal
// 7.0000000000000009, so it needed 8 failures instead of 7.
func TestShouldHaltPercentageExactness(t *testing.T) {
	rules := []struct {
		pct   string
		total int
		wantN int
	}{
		{"7%", 100, 7},
		{"7%", 200, 14},
		{"3%", 100, 3},
		{"5%", 20, 1},
		{"30%", 10, 3},
		{"50%", 4, 2},
		{"100%", 5, 5},
	}
	for _, tc := range rules {
		rule, err := ParseHalt(tc.pct)
		if err != nil {
			t.Fatalf("ParseHalt(%q): %v", tc.pct, err)
		}
		if rule.ShouldHalt(tc.wantN-1, 0, tc.wantN-1, tc.total) {
			t.Errorf("%s of %d fired one early (at %d)", tc.pct, tc.total, tc.wantN-1)
		}
		if !rule.ShouldHalt(tc.wantN, 0, tc.wantN, tc.total) {
			t.Errorf("%s of %d did not fire at %d", tc.pct, tc.total, tc.wantN)
		}
	}
}

func TestExitCode(t *testing.T) {
	tests := []struct {
		name     string
		rule     HaltRule
		failed   int
		total    int
		lastExit int
		lastSig  int
		want     int
	}{
		{"no halt, one failure", HaltRule{}, 1, 0, 0, 0, 1},
		{"no halt, many failures", HaltRule{}, 100, 0, 0, 0, 100},
		{"no halt, too many", HaltRule{}, 150, 0, 0, 0, 101},
		{"fail=1 exit code", HaltRule{Set: true, When: Now, On: OnFail, Count: 1}, 1, 0, 3, 0, 3},
		{"fail=1 signal", HaltRule{Set: true, When: Now, On: OnFail, Count: 1}, 1, 0, -1, 9, 137},
		{"fail=2 count", HaltRule{Set: true, When: Now, On: OnFail, Count: 2}, 2, 0, 1, 0, 2},
		{"success=1", HaltRule{Set: true, When: Now, On: OnSuccess, Count: 1}, 0, 0, 0, 0, 0},
		// GNU parallel returns the number of failed jobs for percentage rules,
		// not a percentage.
		{"pct", HaltRule{Set: true, When: Now, On: OnFail, Pct: 0.03, pctPPM: 30000}, 2, 40, 2, 0, 2},
		{"pct, no total", HaltRule{Set: true, When: Soon, On: OnFail, Pct: 0.03, pctPPM: 30000}, 3, 0, 2, 0, 3},
	}
	for _, tc := range tests {
		if got := tc.rule.ExitCode(tc.failed, tc.total, tc.lastExit, tc.lastSig); got != tc.want {
			t.Errorf("%s: ExitCode = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestResolveJobs(t *testing.T) {
	if n, err := resolveJobs(""); err != nil || n < 1 {
		t.Errorf("default jobs = %d, %v", n, err)
	}
	if n, err := resolveJobs("4"); err != nil || n != 4 {
		t.Errorf("-j4 = %d, %v", n, err)
	}
	if n, err := resolveJobs("100%"); err != nil || n < 1 {
		t.Errorf("-j100%% = %d, %v", n, err)
	}
	if n, err := resolveJobs("+2"); err != nil || n < 3 {
		t.Errorf("-j+2 = %d, %v", n, err)
	}
	if _, err := resolveJobs("0"); err == nil {
		t.Error("-j0 must be rejected")
	}
	if _, err := resolveJobs("abc"); err == nil {
		t.Error("non-numeric jobs must be rejected")
	}
}

func TestNormalize(t *testing.T) {
	got := normalize([]string{"-j4", "-aFILE", "-uv", "--halt", "now,fail=1"})
	want := []string{"-j=4", "-a=FILE", "-u", "-v", "--halt", "now,fail=1"}
	if len(got) != len(want) {
		t.Fatalf("normalize = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("normalize = %v, want %v", got, want)
		}
	}
}

// GNU parallel accepts a bare number of seconds for --timeout; the flag parser
// must accept it too (both the separate and = forms), while leaving unit
// forms and non-numeric values alone.
func TestTimeoutBareSeconds(t *testing.T) {
	tests := []struct {
		argv []string
		want time.Duration
		err  bool
	}{
		{[]string{"--timeout", "30"}, 30 * time.Second, false},
		{[]string{"--timeout=5"}, 5 * time.Second, false},
		{[]string{"--timeout", "200ms"}, 200 * time.Millisecond, false},
		{[]string{"--timeout", "1s"}, time.Second, false},
		{[]string{"--timeout", "0"}, 0, false},
		{[]string{"--timeout", "-5"}, 0, true},
		{[]string{"--timeout", "abc"}, 0, true},
	}
	for _, tc := range tests {
		opts, err := Parse(tc.argv)
		if tc.err {
			if err == nil {
				t.Errorf("Parse(%v): expected error", tc.argv)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%v): %v", tc.argv, err)
			continue
		}
		if opts.Timeout != tc.want {
			t.Errorf("Parse(%v).Timeout = %v, want %v", tc.argv, opts.Timeout, tc.want)
		}
	}
}

// --pty implies streaming (-u) so interactive output is visible while the job
// runs; explicit buffering modes (-k, --line-buffer) are respected instead.
func TestPtyImpliesUngroup(t *testing.T) {
	opts, err := Parse([]string{"--pty"})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.Ungroup {
		t.Error("--pty must imply --ungroup for interactive output")
	}

	opts, err = Parse([]string{"--pty", "-k"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Ungroup || !opts.KeepOrder {
		t.Errorf("-k must win over --pty's implied streaming: %+v", opts)
	}

	opts, err = Parse([]string{"--pty", "--line-buffer"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Ungroup || !opts.LineBuffer {
		t.Errorf("--line-buffer must win over --pty's implied streaming: %+v", opts)
	}
}

func TestParseTermSeq(t *testing.T) {
	steps, err := ParseTermSeq("TERM,200,KILL,25")
	if err != nil || len(steps) != 2 {
		t.Fatalf("ParseTermSeq: %v %v", steps, err)
	}
	if steps[1].Signal != syscall.SIGKILL || steps[1].Wait.Milliseconds() != 25 {
		t.Errorf("unexpected step %+v", steps[1])
	}
	if _, err := ParseTermSeq("TERM"); err == nil {
		t.Error("odd number of fields must fail")
	}
	if _, err := ParseTermSeq("BOGUS,10"); err == nil {
		t.Error("unknown signal must fail")
	}
	// Regression: this value overflowed time.Duration to a negative wait.
	if _, err := ParseTermSeq("ILL,9227000000000"); err == nil {
		t.Error("a duration that overflows time.Duration must fail")
	}
	if _, err := ParseTermSeq("ILL,999999999999999999999"); err == nil {
		t.Error("an unparseable duration must fail")
	}
}
