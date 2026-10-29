package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"parallelizer/internal/sink"
)

// Version is the released version string. It is overridable at link time:
//
//	go build -ldflags="-X parallelizer/internal/cli.Version=1.2.3"
var Version = "0.1.0"

// Options holds every setting for a run.
type Options struct {
	JobCount   int
	Halt       HaltRule
	ArgFile    string
	JobLog     string
	Timeout    time.Duration
	TermSeq    []SignalStep
	TmpDir     string
	Shell      string
	MemLimit   int64
	Ungroup    bool
	LineBuffer bool
	KeepOrder  bool
	Tag        bool
	DryRun     bool
	Verbose    int
	Quiet      bool
	KeepCR     bool
	KeepKilled bool

	ShowVersion bool
	ShowHelp    bool
}

// SignalStep is one entry of --termseq.
type SignalStep struct {
	Name string
	Wait time.Duration
}

// ErrVersion and ErrHelp let main distinguish "print and exit 0" from errors.
var (
	ErrVersion = errors.New("version requested")
	ErrHelp    = errors.New("help requested")
)

// Parse turns argv into Options. It accepts:
//
//	-j4 -j 4 --jobs 4 --jobs=4
//	--halt now,fail=1  --halt=now,fail=1
//	-a file -afile
func Parse(argv []string) (*Options, error) {
	argv = normalize(argv)

	var (
		jobsRaw    string
		haltRaw    string
		argFile    string
		jobLog     string
		termSeqRaw string
		tmpDir     string
		shell      string
		memLimit   int64
		timeout    time.Duration
		ungroup    bool
		lineBuffer bool
		keepOrder  bool
		tag        bool
		dryRun     bool
		quiet      bool
		keepCR     bool
		keepKilled bool
		verbose    int
		showVer    bool
		showHelp   bool
	)

	fs := flag.NewFlagSet("parallelizer", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&jobsRaw, "j", "", "")
	fs.StringVar(&jobsRaw, "jobs", "", "")
	fs.StringVar(&haltRaw, "halt", "", "")
	fs.StringVar(&haltRaw, "halt-on-error", "", "")
	fs.StringVar(&argFile, "a", "", "")
	fs.StringVar(&argFile, "arg-file", "", "")
	fs.StringVar(&jobLog, "joblog", "", "")
	fs.DurationVar(&timeout, "timeout", 0, "")
	fs.StringVar(&termSeqRaw, "termseq", "", "")
	fs.StringVar(&tmpDir, "tmpdir", "", "")
	fs.StringVar(&shell, "shell", "", "")
	fs.Int64Var(&memLimit, "mem-limit", 0, "")
	fs.BoolVar(&ungroup, "u", false, "")
	fs.BoolVar(&ungroup, "ungroup", false, "")
	fs.BoolVar(&lineBuffer, "line-buffer", false, "")
	fs.BoolVar(&keepOrder, "k", false, "")
	fs.BoolVar(&keepOrder, "keep-order", false, "")
	fs.BoolVar(&tag, "tag", false, "")
	fs.BoolVar(&dryRun, "dry-run", false, "")
	fs.BoolVar(&quiet, "q", false, "")
	fs.BoolVar(&quiet, "quiet", false, "")
	fs.BoolVar(&keepCR, "keep-cr", false, "")
	fs.BoolVar(&keepKilled, "keep-killed-output", false, "")
	fs.BoolVar(&showVer, "V", false, "")
	fs.BoolVar(&showVer, "version", false, "")
	fs.BoolVar(&showHelp, "h", false, "")
	fs.BoolVar(&showHelp, "help", false, "")
	fs.BoolFunc("v", "", func(string) error { verbose++; return nil })
	fs.BoolFunc("verbose", "", func(string) error { verbose++; return nil })

	// -h also returns flag.ErrHelp; keep the built-in behaviour.
	if err := fs.Parse(argv); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return &Options{ShowHelp: true}, nil
		}
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument %q (commands are read from stdin or -a FILE)", fs.Arg(0))
	}
	if showVer {
		return &Options{ShowVersion: true}, nil
	}
	if showHelp {
		return &Options{ShowHelp: true}, nil
	}

	jobs, err := resolveJobs(jobsRaw)
	if err != nil {
		return nil, err
	}
	rule, err := ParseHalt(haltRaw)
	if err != nil {
		return nil, err
	}
	if termSeqRaw == "" {
		termSeqRaw = "TERM,200,TERM,100,TERM,50,KILL,25"
	}
	steps, err := ParseTermSeq(termSeqRaw)
	if err != nil {
		return nil, err
	}

	opts := &Options{
		JobCount:   jobs,
		Halt:       rule,
		ArgFile:    argFile,
		JobLog:     jobLog,
		Timeout:    timeout,
		TermSeq:    steps,
		TmpDir:     firstNonEmpty(tmpDir, os.Getenv("TMPDIR"), "/tmp"),
		Shell:      firstNonEmpty(shell, os.Getenv("PARALLELIZER_SHELL"), "/bin/sh"),
		MemLimit:   memLimit,
		Ungroup:    ungroup,
		LineBuffer: lineBuffer,
		KeepOrder:  keepOrder,
		Tag:        tag,
		DryRun:     dryRun,
		Verbose:    verbose,
		Quiet:      quiet,
		KeepCR:     keepCR,
		KeepKilled: keepKilled,
	}
	if opts.MemLimit <= 0 {
		opts.MemLimit = sink.DefaultLimit
	}
	if opts.Timeout < 0 {
		return nil, fmt.Errorf("--timeout must not be negative")
	}
	if opts.Ungroup && opts.LineBuffer {
		return nil, fmt.Errorf("--ungroup and --line-buffer are mutually exclusive")
	}
	if opts.KeepOrder && opts.Ungroup {
		return nil, fmt.Errorf("--keep-order and --ungroup are mutually exclusive")
	}
	if opts.KeepOrder && opts.LineBuffer {
		return nil, fmt.Errorf("--keep-order and --line-buffer are mutually exclusive")
	}
	return opts, nil
}

// normalize rewrites the compact forms GNU parallel users type.
func normalize(argv []string) []string {
	out := make([]string, 0, len(argv))
	for _, a := range argv {
		switch {
		case a == "" || a == "-":
			out = append(out, a)
		case strings.HasPrefix(a, "--"):
			out = append(out, a)
		case strings.HasPrefix(a, "-") && len(a) > 2:
			switch {
			case a[1] == 'j':
				out = append(out, "-j="+a[2:])
			case a[1] == 'a':
				out = append(out, "-a="+a[2:])
			case allRunes(a[1:], "uvkq"):
				for _, r := range a[1:] {
					out = append(out, "-"+string(r))
				}
			default:
				out = append(out, a)
			}
		default:
			out = append(out, a)
		}
	}
	return out
}

func allRunes(s, set string) bool {
	for _, r := range s {
		if !strings.ContainsRune(set, r) {
			return false
		}
	}
	return s != ""
}

func resolveJobs(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "auto" {
		return runtime.NumCPU(), nil
	}
	if strings.HasPrefix(s, "+") {
		n, err := strconv.Atoi(s[1:])
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid --jobs %q: want +N", s)
		}
		return runtime.NumCPU() + n, nil
	}
	if strings.HasSuffix(s, "%") {
		p, err := strconv.Atoi(strings.TrimSuffix(s, "%"))
		if err != nil || p < 1 || p > 1000 {
			return 0, fmt.Errorf("invalid --jobs %q: want 1%%..1000%%", s)
		}
		n := int(math.Ceil(float64(runtime.NumCPU()) * float64(p) / 100))
		if n < 1 {
			n = 1
		}
		return n, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid --jobs %q: want N, N%% or +N", s)
	}
	if n < 1 {
		return 0, fmt.Errorf("--jobs %d is not supported: unlimited concurrency risks exhausting the machine", n)
	}
	return n, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// Usage writes the help text.
func Usage(w io.Writer) {
	fmt.Fprintf(w, `parallelizer - run commands from a file/stdin in parallel

Usage:
  parallelizer [flags]            read commands from stdin, one per line
  parallelizer [flags] -a FILE    read commands from FILE

The whole line is passed to the shell (%s -c LINE). The job's stdin is /dev/null.

Flags:
  -j, --jobs N|N%%|+N   concurrent jobs (default: number of CPUs)
      --halt RULE       stop early: never | [now|soon],[fail|success|done]=N|N%%
  -a, --arg-file FILE   read commands from FILE instead of stdin
      --joblog FILE     write a GNU-compatible 9-column TSV log ("-" = stdout,
                        "+FILE" appends instead of overwriting)
      --timeout DUR     kill a job after DUR (e.g. 30s, 5m)
      --termseq SEQ     kill escalation, default TERM,200,TERM,100,TERM,50,KILL,25
      --tmpdir DIR      directory for output spooling (default: $TMPDIR or /tmp)
      --mem-limit BYTES in-memory output per stream before spilling to disk
                        (default: 8MiB)
      --shell PATH      shell used to run commands (default: /bin/sh)
  -u, --ungroup         let jobs write directly to the terminal (interleaved)
      --line-buffer     stream complete lines as they are produced
  -k, --keep-order      emit output in input order
      --tag             prefix every output line with the job's command + TAB
      --dry-run         print commands instead of running them
  -v, --verbose         echo each command to stderr before running
                        (repeat: -vv also reports each job's exit status)
  -q, --quiet           do not report failing jobs
      --keep-cr         keep a trailing CR from CRLF input
      --keep-killed-output  keep output produced by jobs killed by --halt
  -h, --help            show this help
  -V, --version         show version

Exit status:
  0        all jobs succeeded
  1-100    number of failed jobs (101 when more than 100 failed)
  N        with --halt ...fail=1: the failing job's own exit status
  128+S    with --halt ...fail=1: the job died from signal S
  130/143  interrupted by SIGINT/SIGTERM
  255      parallelizer itself failed
`, "/bin/sh")
}
