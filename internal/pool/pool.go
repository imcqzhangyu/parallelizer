package pool

import (
	"fmt"
	"io"
	"sync"
	"time"

	"parallelizer/internal/cli"
	"parallelizer/internal/job"
	"parallelizer/internal/log"
	"parallelizer/internal/sink"
	"parallelizer/internal/source"
)

// Pool runs the command file with a fixed number of workers.
type Pool struct {
	opts   *cli.Options
	st     *State
	out    *Output
	log    *log.TSV
	exec   *job.Exec
	tty    *job.TtyHub  // --pty keystroke forwarding; nil otherwise
	skip   map[int]bool // --resume: jobs already in the joblog
	stdout io.Writer
	stderr io.Writer
}

func New(opts *cli.Options, total int, stdout, stderr io.Writer) (*Pool, error) {
	// cli has already validated the signal names; its steps are job steps.
	steps := opts.TermSeq

	skip := map[int]bool{}
	joblogPath := opts.JobLog
	if opts.Resume {
		var err error
		skip, err = log.CompletedSeqs(opts.JobLog)
		if err != nil {
			return nil, err
		}
		// --resume must add to the existing log instead of overwriting it.
		joblogPath = "+" + opts.JobLog
		// Percentage --halt rules count the jobs that still have to run.
		// Subtract only resumed jobs that also occur in this input (seq <=
		// total): the joblog may come from a longer, earlier run.
		if opts.Halt.Pct > 0 && total > 0 {
			overlap := 0
			for seq := range skip {
				if seq <= total {
					overlap++
				}
			}
			total -= overlap
			if total < 0 {
				total = 0
			}
		}
	}
	if opts.DryRun {
		// GNU parallel does not open the joblog at all in a dry-run: no file,
		// no header, nothing on stdout for --joblog -.
		joblogPath = ""
	}

	jl, err := log.New(joblogPath)
	if err != nil {
		return nil, err
	}

	p := &Pool{
		opts:   opts,
		st:     NewState(opts.Halt, steps, total),
		out:    NewOutput(stdout, stderr, opts.Tag, opts.KeepOrder),
		log:    jl,
		skip:   skip,
		stdout: stdout,
		stderr: stderr,
	}
	var tty *job.TtyHub
	if opts.Pty {
		tty = job.NewTtyHub() // probes our terminal; never fails
	}
	p.exec = &job.Exec{
		Shell:    opts.Shell,
		Timeout:  opts.Timeout,
		TermSeq:  steps,
		DryRun:   opts.DryRun,
		Verbose:  opts.Verbose,
		VerboseW: stderr,
		OnStart:  p.st.SetPgid,
		Pty:      opts.Pty,
		Tty:      tty,
	}
	p.tty = tty
	p.out.onEPIPE = func() { p.st.Interrupt(141) }
	return p, nil
}

// Interrupt is called from the signal handler.
func (p *Pool) Interrupt(code int) { p.st.Interrupt(code) }

// DumpRunning prints the running jobs (SIGUSR1).
func (p *Pool) DumpRunning(w io.Writer) { p.st.DumpRunning(w) }

// Run schedules every command and returns the exit status.
func (p *Pool) Run(src io.Reader) int {
	jobs := make(chan job.Spec, p.opts.JobCount*2)
	var wg sync.WaitGroup

	rd := source.New(src, p.opts.KeepCR)
	var srcErr error
	go func() {
		defer close(jobs)
		seq := 0
		for {
			line, ok := rd.Next()
			if !ok {
				break
			}
			seq++
			select {
			case jobs <- job.Spec{Seq: seq, Command: line}:
			case <-p.st.Halted():
				return
			}
		}
		// A partial read means some commands never ran: the run must fail
		// rather than report success for a truncated command list.
		srcErr = rd.Err()
	}()

	for i := 0; i < p.opts.JobCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for spec := range jobs {
				p.runOne(spec)
			}
		}()
	}

	wg.Wait()

	if srcErr != nil {
		// An input read error outranks the job tally: the command list was
		// truncated, so the run is not trustworthy. 255 = parallelizer itself
		// failed.
		fmt.Fprintf(p.stderr, "parallelizer: cannot read input: %v\n", srcErr)
		return 255
	}
	return p.st.ExitCode()
}

func (p *Pool) runOne(spec job.Spec) {
	if p.skip[spec.Seq] {
		// --resume: this job already ran in an earlier invocation. Its output
		// is not replayed (matching GNU parallel), but the --keep-order cursor
		// still moves over the gap.
		p.out.Skip(spec.Seq)
		return
	}
	if !p.st.StartJob(spec.Seq, spec.Command) {
		// Halting has begun: this job never runs. Tell the output layer so
		// that --keep-order does not wait forever for this sequence number.
		p.out.Skip(spec.Seq)
		return
	}

	var outW, errW io.Writer
	var outSink, errSink *sink.Sink
	var lsOut, lsErr *LineStream

	switch {
	case p.opts.Ungroup:
		outW, errW = p.stdout, p.stderr
		if p.opts.Tag {
			// --tag still prefixes every line; the Tagged wrapper serialises
			// jobs' direct writes so prefixes stay line-atomic.
			outW = p.out.NewTagged(spec.Command, p.stdout)
			errW = p.out.NewTagged(spec.Command, p.stderr)
		}
	case p.opts.LineBuffer:
		lsOut = p.out.NewLineStream(spec.Command, p.stdout, p.opts.MemLimit)
		lsErr = p.out.NewLineStream(spec.Command, p.stderr, p.opts.MemLimit)
		outW, errW = lsOut, lsErr
	default:
		outSink = sink.New(p.opts.TmpDir, p.opts.MemLimit)
		errSink = sink.New(p.opts.TmpDir, p.opts.MemLimit)
		outW, errW = outSink, errSink
	}

	oc := &counter{w: outW}
	ec := &counter{w: errW}

	// --retries N: re-run a failed job up to N more times, on the same host
	// (GNU parallel's --retries only fails over to other ssh logins, and we
	// have none). The final attempt decides the job's accounting; output of
	// every attempt is kept.
	res := p.exec.Run(spec, oc, ec)
	for attempt := 1; attempt <= p.opts.Retries && res.Failed() && !res.DryRun; attempt++ {
		if p.st.IsHalted() {
			break // killed or superseded by a halt: do not restart it
		}
		if p.opts.Verbose > 0 {
			fmt.Fprintf(p.stderr, "parallelizer: #%d retry %d/%d\n", spec.Seq, attempt, p.opts.Retries)
		}
		res = p.exec.Run(spec, oc, ec)
	}
	res.Received = oc.n + ec.n

	// A sink that could not spool its data (bad --tmpdir, full disk) must fail
	// the job: silently continuing would truncate output while exiting 0.
	if res.Err == nil {
		if err := firstSinkErr(outSink, errSink); err != nil {
			res.Err = err
			if res.ExitCode == 0 && res.Signal == 0 {
				res.ExitCode = 1
			}
		}
	}

	// Account for the job (and possibly trigger the halt) before flushing, so
	// that a slow consumer cannot delay killing the remaining jobs. JobDone
	// also settles res.Killed.
	report := p.st.JobDone(&res, p.opts.Quiet)
	if p.log != nil && !res.DryRun {
		p.log.Write(res, spec.Command)
	}

	switch {
	case p.opts.Ungroup:
		// Output went straight to the terminal.
	case p.opts.LineBuffer:
		lsOut.Flush()
		lsErr.Flush()
	default:
		if res.Killed && !p.opts.KeepKilled {
			outSink.Close()
			errSink.Close()
			p.out.Skip(spec.Seq)
		} else {
			tag := spec.Command
			if res.DryRun {
				// The output already is the command; tagging would duplicate it.
				tag = ""
			}
			p.out.Emit(spec.Seq, tag, outSink, errSink)
		}
	}

	if report != "" {
		fmt.Fprint(p.stderr, report)
	}
	if p.opts.Verbose >= 2 {
		fmt.Fprintf(p.stderr, "parallelizer: #%d exit=%d runtime=%s\n",
			spec.Seq, res.Status(), res.Runtime.Round(time.Millisecond))
	}
}

func firstSinkErr(sinks ...*sink.Sink) error {
	for _, s := range sinks {
		if s != nil && s.Err() != nil {
			return s.Err()
		}
	}
	return nil
}

func (p *Pool) Close() error {
	if p.tty != nil {
		p.tty.Close()
	}
	if p.log != nil {
		return p.log.Close()
	}
	return nil
}
