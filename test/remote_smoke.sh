#!/usr/bin/env bash
# Remote-platform smoke tests for parallelizer: assertions that test/diff_gnu.sh
# does not cover (signals, SIGPIPE, spool cleanup, joblog details, flag
# validation, output-mode details). Runs anywhere with bash + coreutils:
#
#   PLZ=./parallelizer-linux-amd64 ./remote_smoke.sh
#
# Prints "ok/FAIL" per assertion and exits non-zero if anything failed.
set -u

PLZ=${PLZ:-./parallelizer-linux-amd64}
pass=0
fail=0

ok()  { pass=$((pass + 1)); printf 'ok   %-42s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL %-42s %s\n' "$1" "$2"; }
check() { # NAME WANT GOT
    if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "want=$2 got=$3"; fi
}

if [ ! -x "$PLZ" ]; then
    echo "missing binary: $PLZ" >&2
    exit 2
fi
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

echo "PLZ: $PLZ"
echo

# --- signals -------------------------------------------------------------
signal_exit() { # SIG WANT
    local sig=$1 want=$2 code marker cmd
    # A unique argv[0] makes the leftover check immune to unrelated `sleep`
    # processes owned by other users or concurrent test runs.
    marker="prlz-smoke-$sig-$$"
    cmd="bash -c 'exec -a $marker sleep 30'"
    (printf '%s\n%s\n' "$cmd" "$cmd") | "$PLZ" -j2 2>"$WORK/sig.err" &
    local pid=$!
    sleep 0.4
    kill -"$sig" "$pid"
    wait "$pid"
    code=$?
    check "signal $sig -> exit $want" "$want" "$code"
    sleep 0.5
    check "signal $sig: no job leftovers" "0" "$(pgrep -f "$marker" | wc -l | tr -d '[:space:]')"
}
signal_exit INT 130
signal_exit TERM 143

# SIGUSR1 dumps the running jobs to stderr without disturbing the run
(printf 'sleep 30\n') | "$PLZ" -j1 2>"$WORK/usr1.err" &
pid=$!
sleep 0.4
kill -USR1 "$pid"
sleep 0.4
kill -TERM "$pid"
wait "$pid" || true
if grep -q "1 job(s) running" "$WORK/usr1.err"; then
    ok "SIGUSR1 dumps running jobs"
else
    bad "SIGUSR1 dumps running jobs" "stderr=$(cat "$WORK/usr1.err")"
fi

# SIGPIPE: closed stdout must exit 141, not die from the signal
seq 1 100000 | sed 's/^/echo /' > "$WORK/big"
set -o pipefail
"$PLZ" -j4 < "$WORK/big" 2>/dev/null | head -1 > /dev/null
check "closed stdout -> exit 141" "141" "${PIPESTATUS[0]}"
set +o pipefail

# --- percentage halt spool cleanup ---------------------------------------
mkdir -p "$WORK/tmp"
code=$(printf 'echo a\nexit 2\n' | TMPDIR="$WORK/tmp" "$PLZ" -j1 --halt now,fail=50% >/dev/null 2>&1; echo $?)
check "percent halt exit = percentage" "50" "$code"
code=$(printf 'false\nfalse\nfalse\nfalse\n' | "$PLZ" -j4 --halt 5% >/dev/null 2>&1; echo $?)
check "5% halt, 1 of 4 failed -> 25" "25" "$code"
check "percent halt removes spool file" "0" "$(ls -A "$WORK/tmp" | wc -l | tr -d '[:space:]')"

# --- joblog --------------------------------------------------------------
printf 'kill -9 $$\n' | "$PLZ" -j1 --joblog "$WORK/jl" >/dev/null 2>&1
sig_row=$(awk -F'\t' 'NR==2 {print $7 "/" $8}' "$WORK/jl")
check "joblog signal row Exitval/Signal" "137/9" "$sig_row"

printf 'echo one\n' | "$PLZ" -j1 --joblog "$WORK/jl2" >/dev/null 2>&1
printf 'echo two\n' | "$PLZ" -j1 --joblog "+$WORK/jl2" >/dev/null 2>&1
check "joblog +FILE appends, header once" "1" "$(grep -c $'^Seq\tHost' "$WORK/jl2")"
check "joblog +FILE keeps both rows" "3" "$(wc -l < "$WORK/jl2" | tr -d '[:space:]')"

printf 'Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand\n1\t:\t1.0\t0.1\t0\t0\t0\t0\techo one\n' > "$WORK/jl3"
out=$(printf 'echo one\necho two\necho three\n' | "$PLZ" -j1 -k --joblog "$WORK/jl3" --resume 2>/dev/null)
check "--resume skips seq 1, keeps order" "two
three" "$out"

# --- output modes --------------------------------------------------------
out=$(printf 'echo hi\n' | "$PLZ" -j1 -u --tag 2>/dev/null)
check "--tag works with -u" 'echo hi	hi' "$out"

bytes=$(printf 'echo A\r\n' | "$PLZ" -j1 --keep-cr | wc -c | tr -d '[:space:]')
check "--keep-cr keeps the CR (3 bytes)" "3" "$bytes"
bytes_default=$(printf 'echo A\r\n' | "$PLZ" -j1 | wc -c | tr -d '[:space:]')
check "default strips CR (2 bytes)" "2" "$bytes_default"

printf 'echo out; echo err >&2\n' | "$PLZ" -j1 --line-buffer >"$WORK/lb.out" 2>"$WORK/lb.err"
check "--line-buffer stdout keeps only stdout" "out" "$(cat "$WORK/lb.out")"
if grep -q err "$WORK/lb.err"; then ok "--line-buffer stderr kept separate"; else bad "--line-buffer stderr kept separate" "$(cat "$WORK/lb.err")"; fi

# --- flag validation -----------------------------------------------------
check "-j0 rejected" "255" "$(printf 'true\n' | "$PLZ" -j0 >/dev/null 2>&1; echo $?)"
check "--mem-limit -1 rejected" "255" "$(printf 'true\n' | "$PLZ" --mem-limit -1 >/dev/null 2>&1; echo $?)"
check "bad --tmpdir rejected at startup" "255" "$(printf 'true\n' | "$PLZ" --tmpdir "$WORK/nope" >/dev/null 2>&1; echo $?)"
check "--resume without --joblog rejected" "255" "$(printf 'true\n' | "$PLZ" --resume >/dev/null 2>&1; echo $?)"
check "-a missing file rejected" "255" "$("$PLZ" -a "$WORK/missing" >/dev/null 2>&1; echo $?)"
check "bare --timeout seconds accepted" "1" "$(printf 'sleep 5\n' | "$PLZ" -j1 --timeout 1 >/dev/null 2>&1; echo $?)"
check "fractional --timeout accepted" "1" "$(printf 'sleep 5\n' | "$PLZ" -j1 --timeout 0.2 >/dev/null 2>&1; echo $?)"
check "-r is accepted (GNU no-run-if-empty)" "0" "$(printf 'echo hi\n' | "$PLZ" -r >/dev/null 2>&1; echo $?)"

# --- pty / terminal isolation -------------------------------------------
# Default path: jobs have no controlling terminal and must fail fast.
out=$(printf '(exec 3<>/dev/tty 2>/dev/null) && echo CTTY-OPEN-OK || echo CTTY-OPEN-FAIL\n' | "$PLZ" -j1 2>/dev/null | tr -d '\r')
check "default job cannot open /dev/tty" "CTTY-OPEN-FAIL" "$out"

# --pty: the job gets its own controlling terminal even with no tty around.
out=$(printf 'test -t 0 && echo TTY0; (exec 3<>/dev/tty 2>/dev/null) && echo CTTY-OK || echo CTTY-NO; echo pty-done\n' | "$PLZ" -j1 --pty 2>/dev/null | tr -d '\r')
if printf '%s' "$out" | grep -q '^TTY0$' && printf '%s' "$out" | grep -q '^CTTY-OK$' && printf '%s' "$out" | grep -q '^pty-done$'; then
    ok "--pty job gets a controlling terminal"
else
    bad "--pty job gets a controlling terminal" "out=$(printf '%s' "$out" | tr '\n' '|')"
fi

# --pty output still flows through the buffering pipeline under -k.
out=$(printf 'echo buffered\n' | "$PLZ" -j1 --pty -k 2>/dev/null | tr -d '\r')
check "--pty output through keep-order" "buffered" "$out"

echo
echo "passed: $pass, failed: $fail"
[ "$fail" -eq 0 ]
