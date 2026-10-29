# parallelizer

A small, dependency-free Go reimplementation of the **command-per-line** mode of
GNU parallel:

```bash
parallelizer -j 4 --halt now,fail=1 < command.txt
```

Each line of the input is one complete shell command. `parallelizer` runs up to
`-j` of them at a time, keeps each job's output together, and can stop the whole
run the moment a job fails.

It is **not** a drop-in replacement for GNU parallel: there are no placeholder
strings (`{}`), no `--pipe`, no `--sshlogin`. It is an independent implementation
aimed at batch runs of command lists.

## Install

```bash
make build          # bin/parallelizer
make install        # /usr/local/bin/parallelizer
make linux          # static linux/amd64 binary
```

The binary is pure Go (`CGO_ENABLED=0`), so it runs on CentOS 7.6 and any other
Linux with a 3.x kernel.

## Usage

```
parallelizer [flags]              commands from stdin, one per line
parallelizer [flags] -a FILE      commands from FILE
```

| Flag | Meaning |
|---|---|
| `-j, --jobs N\|N%\|+N` | concurrent jobs (default: number of CPUs) |
| `--halt RULE` | stop early, see below |
| `-a, --arg-file FILE` | read commands from FILE (`-` = stdin) |
| `--joblog FILE` | 9-column TSV log, GNU compatible (`-` = stdout, `+FILE` = append) |
| `--resume` | skip jobs already listed in `--joblog` (requires a file, not `-`) |
| `-r, --retries N` | re-run each failed job up to N more times (same host) |
| `--timeout DUR` | kill a job after DUR (`30s`, `5m`, `200ms`) |
| `--termseq SEQ` | kill escalation (default `TERM,200,TERM,100,TERM,50,KILL,25`) |
| `--tmpdir DIR` | where large outputs are spooled |
| `--mem-limit BYTES` | in-memory output per stream before spilling (default 8 MiB) |
| `--shell PATH` | shell used for `-c` (default `/bin/sh`) |
| `-u, --ungroup` | let jobs write straight to the terminal (interleaved) |
| `--line-buffer` | stream complete lines as they are produced (stdout and stderr stay separate) |
| `-k, --keep-order` | emit output in input order |
| `--tag` | prefix every line with the command and a TAB (also works with `-u`) |
| `--dry-run` | print commands instead of running them |
| `-v, --verbose` | echo each command to stderr (repeat: `-vv` also reports each job's exit status) |
| `-q, --quiet` | do not report failing jobs |
| `--keep-cr` | keep the `\r` of CRLF input |
| `--keep-killed-output` | keep output from jobs killed by `--halt` |

### Environment

| Variable | Meaning |
|---|---|
| `TMPDIR` | fallback for `--tmpdir` (default `/tmp`) |
| `PARALLELIZER_SHELL` | fallback for `--shell` (default `/bin/sh`) |

### `--halt` syntax

```
never
now,fail=1        kill running jobs as soon as 1 job fails
soon,fail=3       start no new jobs after 3 failures, let running ones finish
now,success=1     stop as soon as one job succeeds
soon,done=10%     stop after 10% of the jobs have finished
5%                shorthand for soon,fail=5%
2                 legacy shorthand for now,fail=1
```

`now` kills the whole **process group** of every running job using `--termseq`,
so children and grandchildren die too. `soon` only stops handing out new work.

### Exit status

| Code | Meaning |
|---|---|
| `0` | everything succeeded |
| `1`-`100` | number of failed jobs (`101` means more than 100) |
| job's status | with `--halt ...fail=1` (e.g. `3`) |
| `128+S` | with `--halt ...fail=1` when the job died from signal `S` |
| `130` / `143` | interrupted by SIGINT / SIGTERM |
| `141` | interrupted by SIGPIPE (stdout closed, e.g. `\| head -1`) |
| `255` | parallelizer itself failed (bad flags, unreadable file, ...) |

## Behaviour notes

- A job's stdin is `/dev/null`.
- Empty and whitespace-only lines are skipped.
- A trailing line without a newline is still executed.
- Output is grouped per job by default (the failing job's block is printed, then
  the next job's), which is what makes parallel output look like sequential
  output. Killed jobs' output is discarded unless `--keep-killed-output` is set;
  either way `-k` still prints later jobs' output, because a hole in the
  sequence is treated as "no output here" instead of blocking forever.
- Large outputs spill to `$TMPDIR/prlz-*` after `--mem-limit` bytes per output
  stream (stdout and stderr are counted separately, so the real ceiling is
  roughly `2 x -j x --mem-limit`). If the spool file cannot be created (bad
  `--tmpdir`, full disk) the job fails loudly instead of growing in memory.
- `--line-buffer` keeps stdout and stderr separate and flushes a partial line
  once it exceeds `--mem-limit`, so one huge line cannot exhaust memory.
- `--joblog +FILE` appends to an existing log without repeating the header (the
  GNU syntax). A plain `--joblog FILE` overwrites, as GNU does. A job killed by
  signal `S` is logged as `Exitval = 128+S`, `Signal = S`.
- `--resume` reads the joblog and skips those sequence numbers; their output is
  not replayed (same as GNU), but `-k` still prints later jobs' output and
  percentage `--halt` rules only count the jobs that remain. It requires
  `--joblog FILE` (not `-`).
- `--retries N` re-runs a failed job up to N extra times **on the same host**
  and keeps the last attempt. GNU parallel's `--retries` only fails over to
  other ssh logins, of which this tool has none.
- Closing stdout stops the run with `141` (SIGPIPE, like GNU). Closing stderr
  does **not**: the run continues and only diagnostics are lost, matching GNU.
- A command list that cannot be read to the end (I/O error mid-stream) fails
  the run with `255` instead of silently dropping the remaining commands.
- `--tmpdir` is probed at startup: a bad path fails immediately with `255`
  rather than failing every job that produces large output.
- `--termseq` accepts the standard signal names (HUP, INT, QUIT, ILL, TRAP,
  ABRT, BUS, FPE, KILL, USR1, SEGV, USR2, PIPE, ALRM, TERM, CHLD, CONT, STOP,
  TSTP, TTIN, TTOU, URG, XCPU, XFSZ, VTALRM, PROF, WINCH, SYS, IO), upper or
  lower case.
- A `-j` far above the file-descriptor soft limit prints a startup warning:
  each running job holds two pipe ends.
- `SIGUSR1` prints the jobs that are currently running.
- A job's command line may be arbitrarily long, but it is held in memory: very
  long lines cost RAM proportional to their length.
- `-j 0` is rejected on purpose: unlimited concurrency is a good way to take a
  machine down.

## Differences from GNU parallel

| Topic | GNU parallel | parallelizer |
|---|---|---|
| Input model | args, `:::`, `::::`, stdin | one command per line only |
| Placeholders | `{}`, `{.}`, `{= perl =}`, ... | none |
| `--pipe` | yes | no |
| SSH | yes | no |
| `--tag` | command/argument prefix | command prefix (same for this mode) |
| `--tag` with `-u` | tag is dropped | tag is kept, writes are serialised per line |
| Trailing `\r` | kept | stripped (use `--keep-cr` to keep) |
| `-j 0` | unlimited | rejected |
| Interrupt exit | `255` | `130` / `143` / `141` (SIGINT/SIGTERM/SIGPIPE) |
| Killed job output | discarded | discarded (opt-in with `--keep-killed-output`) |
| `-k` + killed job | later output is lost behind the hole | later output is still printed |
| `-k` + `--line-buffer` | allowed (degrades to grouped output) | rejected as mutually exclusive |
| `--joblog` append | `--joblog +FILE` | same (`+FILE`) |
| `--resume` | skips jobs listed in `--joblog` | same (output of skipped jobs is not replayed) |
| `--retries N` | only with `--sshlogin` (other hosts) | retried on the same host |
| `--halt` percentage | needs total job count, reads all input | same; input is spooled to a temp file first |
| Percentage `--halt` exit | number of failed jobs | same |

## Performance

On 10 000 trivial jobs (`true`, `-j8`) parallelizer takes ~8.7s where GNU
parallel takes ~23.9s (~2.8x). Output handling is pipelined with scheduling:
feeding 256 MB of job output through a ~12.8 MB/s consumer finishes in 25.2s,
i.e. at the consumer's own floor (25.0s), so a slow reader never stalls new job
starts. `make bench` runs the micro-benchmarks for the output hot paths.

## Development

```bash
make test    # unit + end-to-end
make race    # go test -race
make diff    # differential test against GNU parallel
```

`make diff` compares exit status and sorted output (and stderr for one case)
against GNU parallel for 24 scenarios. Point it at a specific GNU binary with:

```bash
GNU_PARALLEL="perl /path/to/gnu-parallel/src/parallel" make diff
```

GitHub Actions runs `gofmt`, `go vet`, `go test -race`, the fuzz seed corpus
and the GNU differential test on every push (`.github/workflows/ci.yml`).

Three behaviours differ from GNU parallel on purpose and are covered by the go
tests instead: `--tag` is kept with `-u` (GNU drops it), `--keep-killed-output`
is an extension (the reference GNU build has no such flag), and `-k` still
prints output that follows a job killed by `--halt` (GNU loses it behind the
hole).

## Layout

```
cmd/parallelizer       entry point, signals, input setup
internal/cli           flag parsing, --halt and --termseq
internal/source        streaming line reader, spooling
internal/job           process-group execution, timeout, result
internal/pool          worker pool, halt state machine, output ordering
internal/sink          per-job output buffer with disk spill
internal/log           GNU-compatible TSV job log
```
