package cli

import "testing"

// FuzzParseHalt checks that no --halt string can panic the parser and that a
// successful parse upholds the rule's invariants.
func FuzzParseHalt(f *testing.F) {
	seeds := []string{
		"", "never", "0", "1", "2", "-1", "-2", "5%",
		"now,fail=1", "soon,success=2", "now,done=10%", "soon,fail=100%",
		"now", "fail=1", "now,fail", "now,fail=", "now,fail=-3",
		"now,fail=101%", "now,fail=0%", "NOW,FAIL=1", " now , fail = 1 ",
		"never,fail=1", "now,fail=1,extra", "🙂", "\x00now,fail=1",
		"soon,fail=999999999999999999999999", "soon,done=1e3%",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		rule, err := ParseHalt(s)
		if err != nil {
			return
		}
		if rule.Set && rule.When == Never {
			t.Fatalf("ParseHalt(%q): Set with Never", s)
		}
		if rule.Pct != 0 && rule.Count != 0 {
			t.Fatalf("ParseHalt(%q): both Pct and Count set", s)
		}
		if rule.Pct < 0 || rule.Pct > 1 {
			t.Fatalf("ParseHalt(%q): Pct out of range: %v", s, rule.Pct)
		}
		if rule.Count < 0 {
			t.Fatalf("ParseHalt(%q): negative Count", s)
		}
		// The rule must be evaluable and yield a sane exit status.
		if code := rule.ExitCode(1, 1, 1, 0); code < 0 || code > 255 {
			t.Fatalf("ParseHalt(%q): ExitCode = %d", s, code)
		}
		_ = rule.ShouldHalt(1, 1, 1, 1)
	})
}

// FuzzParseTermSeq checks that no --termseq string can panic and that a parse
// yields usable steps.
func FuzzParseTermSeq(f *testing.F) {
	seeds := []string{
		"", "TERM,200,KILL,25", "TERM", "TERM,200,", ",200",
		"TERM,-1", "BOGUS,10", "term,0", "KILL,999999999999999999999",
		"TERM,abc", "TERM,200,TERM,100,TERM,50,KILL,25", "🙂,1",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		steps, err := ParseTermSeq(s)
		if err != nil {
			return
		}
		for _, st := range steps {
			if st.Wait < 0 {
				t.Fatalf("ParseTermSeq(%q): negative wait", s)
			}
			// Steps are only built from resolved signal names, so a zero or
			// negative signal would mean the parser lost track of one.
			if st.Signal <= 0 {
				t.Fatalf("ParseTermSeq(%q): unresolved signal", s)
			}
		}
	})
}
