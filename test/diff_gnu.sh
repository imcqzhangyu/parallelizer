#!/usr/bin/env bash
# Differential test: run the same command file through GNU parallel and
# parallelizer, then compare exit status and (sorted) output.
#
#   GNU_PARALLEL="perl /path/to/src/parallel" ./test/diff_gnu.sh
#   ./test/diff_gnu.sh                     # uses `parallel` from PATH
set -u

GNU=${GNU_PARALLEL:-parallel}
PLZ=${PLZ:-./bin/parallelizer}

if ! command -v ${GNU%% *} >/dev/null 2>&1; then
    echo "skip: GNU parallel not found (set GNU_PARALLEL=...)" >&2
    exit 0
fi

pass=0
fail=0

# check NAME INPUT [ARGS...]: compares exit status and sorted stdout.
check() {
    local name=$1 input=$2
    shift 2
    local g_raw p_raw g_out p_out g_code p_code

    g_raw=$(printf '%s' "$input" | $GNU "$@" 2>/dev/null)
    g_code=$?
    p_raw=$(printf '%s' "$input" | "$PLZ" "$@" 2>/dev/null)
    p_code=$?

    g_out=$(printf '%s\n' "$g_raw" | sort)
    p_out=$(printf '%s\n' "$p_raw" | sort)

    if [ "$g_code" = "$p_code" ] && [ "$g_out" = "$p_out" ]; then
        pass=$((pass + 1))
        printf 'ok   %-32s exit=%s\n' "$name" "$p_code"
    else
        fail=$((fail + 1))
        printf 'FAIL %-32s gnu(exit=%s) plz(exit=%s)\n' "$name" "$g_code" "$p_code"
        diff <(printf '%s\n' "$g_out") <(printf '%s\n' "$p_out") | sed 's/^/     /'
    fi
}

# check_stderr NAME INPUT [ARGS...]: additionally compares sorted stderr.
check_stderr() {
    local name=$1 input=$2
    shift 2
    local g_out p_out g_err p_err g_code p_code

    g_out=$(printf '%s' "$input" | $GNU "$@" 2>/tmp/plz-gnu-err.$$)
    g_code=$?
    g_err=$(sort /tmp/plz-gnu-err.$$)
    p_out=$(printf '%s' "$input" | "$PLZ" "$@" 2>/tmp/plz-plz-err.$$)
    p_code=$?
    p_err=$(sort /tmp/plz-plz-err.$$)
    rm -f /tmp/plz-gnu-err.$$ /tmp/plz-plz-err.$$

    if [ "$g_code" = "$p_code" ] && [ "$(printf '%s\n' "$g_out" | sort)" = "$(printf '%s\n' "$p_out" | sort)" ] \
       && [ "$g_err" = "$p_err" ]; then
        pass=$((pass + 1))
        printf 'ok   %-32s exit=%s (stdout+stderr)\n' "$name" "$p_code"
    else
        fail=$((fail + 1))
        printf 'FAIL %-32s gnu(exit=%s) plz(exit=%s)\n' "$name" "$g_code" "$p_code"
        diff <(printf '%s\n' "$g_err") <(printf '%s\n' "$p_err") | sed 's/^/     stderr /'
        diff <(printf '%s\n' "$g_out" | sort) <(printf '%s\n' "$p_out" | sort) | sed 's/^/     stdout /'
    fi
}

# check_resume NAME INPUT [ARGS...]: seeds an identical joblog (seq 1 already
# done) for each tool, then compares output and exit status under --resume.
check_resume() {
    local name=$1 input=$2
    shift 2
    local dir g_out p_out g_code p_code
    dir=$(mktemp -d)
    local header='Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand\n'
    printf "${header}1\t:\t1.0\t0.1\t0\t0\t0\t0\techo one\n" > "$dir/g.log"
    printf "${header}1\t:\t1.0\t0.1\t0\t0\t0\t0\techo one\n" > "$dir/p.log"

    g_out=$(printf '%s' "$input" | $GNU "$@" --joblog "$dir/g.log" 2>/dev/null)
    g_code=$?
    p_out=$(printf '%s' "$input" | "$PLZ" "$@" --joblog "$dir/p.log" 2>/dev/null)
    p_code=$?
    rm -rf "$dir"

    if [ "$g_code" = "$p_code" ] \
       && [ "$(printf '%s\n' "$g_out" | sort)" = "$(printf '%s\n' "$p_out" | sort)" ]; then
        pass=$((pass + 1))
        printf 'ok   %-32s exit=%s\n' "$name" "$p_code"
    else
        fail=$((fail + 1))
        printf 'FAIL %-32s gnu(exit=%s) plz(exit=%s)\n' "$name" "$g_code" "$p_code"
        diff <(printf '%s\n' "$g_out" | sort) <(printf '%s\n' "$p_out" | sort) | sed 's/^/     /'
    fi
}

echo "GNU: $GNU"
echo "PLZ: $PLZ"
echo

check "basic j1"        $'echo a\necho b\necho c\n' -j1
check "basic j2"        $'echo a\necho b\necho c\necho d\n' -j2
check "empty input"     '' -j2
check "blank lines"     $'\n\necho a\n   \necho b\n' -j1
check "no final newline" 'echo last' -j1
check "halt exit 3"     $'exit 3\n' -j1 --halt now,fail=1
check "halt fail=2"     $'exit 1\nexit 2\nexit 0\n' -j1 --halt now,fail=2
check "halt soon"       $'sleep 0.2; echo late\nexit 1\n' -j2 --halt soon,fail=1
check "no halt, 2 fails" $'exit 1\nexit 2\nexit 0\n' -j1
check "success rule"    $'true\ntrue\n' -j1 --halt now,success=1
check "done rule"       $'true\n' -j1 --halt soon,done=1
check "signal job"      $'kill -9 $$\n' -j1 --halt now,fail=1
check "timeout"         $'sleep 5\n' -j1 --timeout 1s
check "grouped output"  $'echo A1; sleep 0.15; echo A2\necho B1\n' -j2
check "tag"             $'echo hi\n' -j1 --tag
check "keep-order"      $'sleep 0.15; echo first\nsleep 0.02; echo second\n' -j2 -k
check "keep-order k1"   $'sleep 0.1; echo first\necho second\n' -j2 -k
check "line-buffer"     $'echo A; sleep 0.1; echo B\necho C\n' -j2 --line-buffer
check "dry-run"         $'echo a\necho b\n' -j1 --dry-run
check_stderr "stderr passthru" $'echo oops >&2\necho ok\n' -j1
check "halted job output dropped" $'echo kept; sleep 3\nexit 1\n' -j2 --halt now,fail=1
check "many jobs"       "$(for i in $(seq 1 200); do echo "echo $i"; done)" -j8
check "halt many"       "$(for i in $(seq 1 50); do echo "true"; done)"$'\nexit 9\n' -j8 --halt now,fail=1
check_resume "resume skips done" $'echo one\necho two\necho three\n' -j2 -k --resume

# Deliberate differences from GNU parallel, covered by go tests instead:
#   -u --tag        GNU drops the tag; parallelizer keeps it (TestTagWithUngroup)
#   --keep-killed-output   not available in this GNU parallel at all
#   -k + killed job GNU loses all output after the hole; parallelizer prints it
#                   (TestKeepOrderWithKilledJob)

echo
echo "passed: $pass, failed: $fail"
[ "$fail" -eq 0 ]
