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
	stdout io.Writer
	stderr io.Writer
}

func New(opts *cli.Options, total int, stdout, stderr io.Writer) (*Pool, error) {
	steps := make([]job.SignalStep, 0, len(opts.TermSeq))
	for _, s := range opts.TermSeq {
		sig, err := job.SignalByName(s.Name)
		if err != nil {
			return nil, err
		}
		steps = append(steps, job.SignalStep{Signal: sig, Wait: s.Wait})
	}

	jl, err := log.New(opts.JobLog)
	if err != nil {
		return nil, err
	}

	p := &Pool{
		opts:   opts,
		st:     NewState(opts.Halt, steps, total),
		out:    NewOutput(stdout, stderr, opts.Tag, opts.KeepOrder),
		log:    jl,
		stdout: stdout,
		stderr: stderr,
	}
	p.exec = &job.Exec{
		Shell:    opts.Shell,
		Timeout:  opts.Timeout,
		TermSeq:  steps,
		DryRun:   opts.DryRun,
		Verbose:  opts.Verbose,
		VerboseW: stderr,
		OnStart:  p.st.SetPgid,
	}
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
	go func() {
		defer close(jobs)
		seq := 0
		for {
			line, ok := rd.Next()
			if !ok {
				return
			}
			seq++
			select {
			case jobs <- job.Spec{Seq: seq, Command: line}:
			case <-p.st.Halted():
				return
			}
		}
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
	return p.st.ExitCode()
}

func (p *Pool) runOne(spec job.Spec) {
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

	res := p.exec.Run(spec, oc, ec)
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
	if p.log != nil {
		return p.log.Close()
	}
	return nil
}
