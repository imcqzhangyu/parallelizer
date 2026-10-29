# AGENTS.md

Zero-dependency Go reimplementation of GNU parallel's command-per-line mode
(single module `parallelizer`, Unix-only: `syscall.Setpgid`/`WaitStatus`, no
Windows support). Keep go.mod dependency-free.

## Commands

```bash
make build                 # bin/parallelizer (version injected from Makefile VERSION via -X)
make test                  # go test ./...
make race                  # go test -race ./...   <- run before committing
make check                 # gofmt + vet + race + diff (the checklist in one command)
make bench                 # output-path micro-benchmarks
make diff                  # differential test vs GNU parallel (builds first)
gofmt -l .                 # must print nothing; CI fails otherwise
```

- Single test: `go test -run TestName ./test/` (e2e package; it builds the
  binary itself in TestMain — no pre-build needed).
- Remote-platform check: `PLZ=./parallelizer-linux-amd64 ./test/remote_smoke.sh`
  (29 assertions, bash-only, runs on the CentOS box — see SERVER.local.md).
- TUI/pty harness: `python3 test/pty_manual.py ./bin/parallelizer` (needs a
  python REPL subject; gives the tool a real controlling terminal and checks
  keystroke forwarding + termios isolation). The e2e
  `TestPtyForwardsKeystrokes` runs only when the test process has **no** ctty
  (otherwise its key source would be /dev/tty, not the stdin it drives).
- Fuzz seeds only: `go test -run 'Fuzz' ./internal/cli/`.
  Real fuzzing: `go test -fuzz FuzzParseHalt -fuzztime 20s ./internal/cli/`.
  **Never delete `internal/cli/testdata/fuzz/`** — entries there are fuzz-found
  regression cases.
- `make diff` needs GNU parallel: defaults to `parallel` in PATH (brew:
  `/opt/homebrew/bin/parallel`, 20260922). Set `GNU_PARALLEL=...` to override.
  It compares exit codes + sorted output (one scenario also stderr), so
  `PARALLEL="--will-cite"` avoids GNU's citation text polluting stderr.
- Version bump: edit `VERSION` in the Makefile only (single source; injected
  into `cli.Version`, which is a `var`, not a `const`).
- Legacy-platform test target (CentOS 7.6 host, credentials + smoke-test
  steps): see `SERVER.local.md` — a **gitignored local file**; never copy its
  contents into a tracked file.

## Commit checklist

`gofmt -l .` → `go vet ./...` → `go test -race ./...` → `make diff` (all
scenarios must pass; old GNU versions print `skip` for capability-gated
scenarios like `--halt ...done` and the signal-job exit convention) →
`make linux` if syscall code changed (cross-compile check).

## Architecture (the wiring, not the file tree)

```
cmd/parallelizer/main.go   run() only: parse -> tmpdir probe -> fd warning ->
                           signal.Notify (BEFORE openInput) -> openInput ->
                           pool.New -> Run -> Close; main is os.Exit(run())
internal/cli               flags, --halt rules (integer ppm thresholds), termseq
internal/source            line reader + Spool (pre-reads input for % --halt)
internal/job               Exec.Run: one job in its own process group, timeout,
                           --termseq escalation via KillGroup
internal/pool              workers + halt state machine + output ordering
internal/sink              per-job output buffer, spills to disk past --mem-limit
internal/log               GNU-compatible 9-column TSV joblog
```

## Contracts that are easy to break

- **keep-order (`-k`) gaps**: every sequence number must eventually call
  `Output.Emit` or `Output.Skip` — killed, never-started, and --resume-skipped
  jobs included. Missing one and all later output is buffered forever and never
  printed. When a run halts, workers stop pulling and queued commands are
  abandoned, so `Pool.Run` calls `Output.FlushPending()` before returning (it
  prints the finished output stuck behind those holes, ignoring the gaps).
- **Halting must not wait for the reader**: the line reader can be parked in a
  blocking `read(2)` (`tail -f cmds | parallelizer`) that Go cannot cancel.
  Workers exit through `State.Halted()`, the reader reports its error through a
  buffered channel, and `Run` never waits for `close(jobs)` on the halted path.
  Adding a `close(jobs)` there reintroduces the hang (and would race with the
  reader as the only sender).
- **Job settlement is atomic**: `State.JobDone(&res, quiet)` sets `res.Killed`
  and removes the job from the running set in one critical section. Do not
  probe "killed" separately before/after — that reopens the pgid-reuse race.
  `Exec.OnReap` additionally drops the pgid immediately after `cmd.Wait`, so a
  later halt cannot collect a group that is already gone; `KillGroup` also
  re-checks the group with `kill(-pgid, 0)` before every signal. That narrows
  the recycle window to the microseconds between check and kill — it cannot be
  closed portably without pidfds/proc start-time checks.
- **`main` must not `os.Exit` before cleanup**: the spool temp file is removed
  by a `defer` inside `run()`'s inner closure (see `os.Exit(run())` pattern).
  The mid-spool interrupt path instead removes the file and re-raises SIGINT/
  SIGTERM with default disposition (keeps 130/143).
- **`--halt now` vs `soon` on a late pgid**: `State.SetPgid` may run after the
  halt fired. `killOnStart` (set by `now` rules and by `Interrupt`) kills such a
  job; under `soon` it is registered and allowed to finish.
- **`Interrupt` wins over a halt rule**: a signal during shutdown overwrites the
  rule's exit status (Ctrl-C after `--halt` is always 130/143/141).
- **dry-run must not touch the joblog at all** (GNU does not even open it —
  verified in GNU's source, `if($opt::dryrun)` branch).
- **Jobs run with `Setsid` (own session, no controlling terminal)** — never
  revert to inheriting the caller's terminal: a TUI job would SIGTTIN-stop
  forever and could leave the user's terminal in raw mode. `KillGroup` relies
  on a session leader's pgid being its pid, so `kill(-pgid)` keeps working.
- **A job that daemonizes must not block the run**: `cmd.WaitDelay` (500ms)
  bounds the wait for orphaned grandchildren to close the inherited
  stdout/stderr pipes (GNU returns at the shell exit). `exec.ErrWaitDelay` is
  *not* a job failure — `Result.finish` ignores it; output after the timeout is
  dropped.
- **`--pty` contracts**: implies `-u` streaming unless `-k`/`--line-buffer`
  was set explicitly (buffered output would hide the REPL prompt); child uses
  `Setsid+Setctty` with `Ctty: 0` (child fd index of the slave); stderr is
  merged into the stdout writer; the keystroke hub reads `/dev/tty` first,
  falls back to stdin-if-tty, and forwards to all live jobs (pending keys are
  queued up to 64KB between jobs). The pty **master must stay non-blocking**:
  do the ioctls through `SyscallConn().Control`, never `File.Fd()` (Fd flips
  the descriptor to blocking, after which `Close` cannot interrupt the copy
  goroutine). The **slave must be blocking** (`makeBlocking`) or the child
  inherits `O_NONBLOCK` and every read/write returns EAGAIN. `/dev/tty` is
  opened **blocking**: on Darwin a non-blocking tty cannot be registered with
  kqueue and `Read` returns EAGAIN instead of waiting for keys. Darwin's openPty
  needs the full TIOCPTYGNAME → TIOCPTYGRANT → TIOCPTYUNLK sequence (see
  pty_darwin.go).
- **`-r` is GNU's `--no-run-if-empty`**, not `--retries` (retries is long-only).
  Accepting it as a no-op keeps GNU command lines working; our reader never
  runs empty lines anyway.
- **`--timeout` accepts GNU forms**: bare/fractional seconds and `d`, plus Go
  duration units (`200ms`). Do not switch it back to `flag.DurationVar` — that
  rejects `30`, `0.5` and `1d`.

## GNU parallel is the spec

`test/diff_gnu.sh` is the behavioral arbiter (30 scenarios). Three+ divergences
are **intentional** and documented in that script's trailing comment and
README's "Differences from GNU parallel" table — do not "fix" them toward GNU:

- `--tag` is kept with `-u` (GNU drops it); `--keep-killed-output` is an
  extension (GNU 20260922 lacks the flag); `-k` still prints output following
  a job killed by `--halt` (GNU loses it behind the hole); `-k` + `--line-buffer`
  is rejected here but accepted by GNU (semantically equivalent to grouped).

Verified GNU behaviors we deliberately match (regression tests depend on them):

- joblog rows are written in **completion order**, not Seq order;
- signal death logs `Exitval = 128+Signal`, `Signal = S`;
- percentage `--halt` exits with the **percentage of failed jobs, rounded up**
  (25 for 1 of 4, 38 for 3 of 8; verified on GNU 20160222 and 20260922), and it
  does so even when the rule never fires (`--halt 100%` with 1 failure of 4
  exits 25, not 1). `--resume` reduces the denominator to the jobs that still
  have to run.

## Testing quirks

- e2e tests are timing-based on purpose (real processes/groups): keep the
  `sleep` margins — e.g. 300 ms before sending a signal, `sleep 0.4` jobs so a
  halt cannot fire before a job starts. Prefer `signalExit` helper for new
  signal tests.
- Sink/spool failures are hard to trigger e2e now that `--tmpdir` is probed at
  startup — cover such paths as unit tests instead (see
  `internal/sink.TestSinkSpillFailureIsAnError`).
- Joblog assertions must not assume Seq ordering (see above).

## Conventions

- Comments and commit messages in English; README documents user-facing
  behavior — update it in the same change as any behavior change (the
  "Differences" table and "Behaviour notes" sections track real semantics).
- Safety-by-rejection: `-j 0`, negative `--mem-limit`, `--resume` without a
  joblog file, and incompatible flag pairs exit 255 with a message rather than
  being coerced. New validation should follow suit; the fd-limit check is the
  one deliberate warning-only case (estimate is approximate).
- Exit codes are documented in README ("Exit status") and asserted in tests;
  changing one means updating both.
