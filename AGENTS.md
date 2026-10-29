# AGENTS.md

Zero-dependency Go reimplementation of GNU parallel's command-per-line mode
(single module `parallelizer`, Unix-only: `syscall.Setpgid`/`WaitStatus`, no
Windows support). Keep go.mod dependency-free.

## Commands

```bash
make build                 # bin/parallelizer (version injected from Makefile VERSION via -X)
make test                  # go test ./...
make race                  # go test -race ./...   <- run before committing
make bench                 # output-path micro-benchmarks
make diff                  # differential test vs GNU parallel (builds first)
gofmt -l .                 # must print nothing; CI fails otherwise
```

- Single test: `go test -run TestName ./test/` (e2e package; it builds the
  binary itself in TestMain — no pre-build needed).
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

`gofmt -l .` → `go vet ./...` → `go test -race ./...` → `make diff` (24/24
must pass) → `make linux` if syscall code changed (cross-compile check).

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
  printed.
- **Job settlement is atomic**: `State.JobDone(&res, quiet)` sets `res.Killed`
  and removes the job from the running set in one critical section. Do not
  probe "killed" separately before/after — that reopens the pgid-reuse race.
- **`main` must not `os.Exit` before cleanup**: the spool temp file is removed
  by a `defer` inside `run()`'s inner closure (see `os.Exit(run())` pattern).
  The mid-spool interrupt path instead removes the file and re-raises SIGINT/
  SIGTERM with default disposition (keeps 130/143).
- **dry-run must not touch the joblog at all** (GNU does not even open it —
  verified in GNU's source, `if($opt::dryrun)` branch).

## GNU parallel is the spec

`test/diff_gnu.sh` is the behavioral arbiter (24 scenarios). Three+ divergences
are **intentional** and documented in that script's trailing comment and
README's "Differences from GNU parallel" table — do not "fix" them toward GNU:

- `--tag` is kept with `-u` (GNU drops it); `--keep-killed-output` is an
  extension (GNU 20260922 lacks the flag); `-k` still prints output following
  a job killed by `--halt` (GNU loses it behind the hole); `-k` + `--line-buffer`
  is rejected here but accepted by GNU (semantically equivalent to grouped).

Verified GNU behaviors we deliberately match (regression tests depend on them):

- joblog rows are written in **completion order**, not Seq order;
- signal death logs `Exitval = 128+Signal`, `Signal = S`;
- percentage `--halt` exits with the **number of failed jobs**.

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
