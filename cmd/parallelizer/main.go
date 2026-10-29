package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"parallelizer/internal/cli"
	"parallelizer/internal/pool"
	"parallelizer/internal/source"
)

func main() {
	// Never call os.Exit below this point without going through run(): the
	// spooled-input temp file must be removed before exiting.
	os.Exit(run())
}

func run() int {
	opts, err := cli.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "parallelizer: %v\n", err)
		fmt.Fprintln(os.Stderr, "Try 'parallelizer --help' for more information.")
		return 255
	}
	if opts.ShowVersion {
		fmt.Printf("parallelizer %s (%s, %s/%s)\n", cli.Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
		return 0
	}
	if opts.ShowHelp {
		cli.Usage(os.Stdout)
		return 0
	}

	if err := probeTmpDir(opts.TmpDir); err != nil {
		fmt.Fprintf(os.Stderr, "parallelizer: %v\n", err)
		return 255
	}
	warnFdLimit(opts.JobCount)

	// Register signals before opening the input: while the input is being
	// opened (or spooled) the pool does not exist yet, so the handler parks
	// the code in `pending` and it is applied as soon as the pool is ready.
	// Spooling to a blocking stdin has its own immediate handler inside
	// source.Spool, which removes the temp file and re-raises the signal.
	var (
		sigMu   sync.Mutex
		poolPtr *pool.Pool
		pending int // exit code of a signal that arrived before the pool
		sigStop = make(chan struct{})
	)
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGPIPE)
	go func() {
		for {
			select {
			case sig := <-sigCh:
				code := 0
				switch sig {
				case syscall.SIGINT:
					code = 130
				case syscall.SIGTERM:
					code = 143
				case syscall.SIGPIPE:
					// A closed stdout must exit 141 like GNU parallel instead
					// of dying from the signal; Output reports the EPIPE.
					code = 141
				case syscall.SIGUSR1:
					sigMu.Lock()
					p := poolPtr
					sigMu.Unlock()
					if p != nil {
						p.DumpRunning(os.Stderr)
					}
					continue
				}
				sigMu.Lock()
				p := poolPtr
				if p == nil {
					pending = code
					sigMu.Unlock()
					continue
				}
				sigMu.Unlock()
				p.Interrupt(code)
			case <-sigStop:
				return
			}
		}
	}()
	defer close(sigStop)

	input, total, cleanup, err := openInput(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parallelizer: %v\n", err)
		return 255
	}
	code := func() int {
		defer cleanup()

		p, err := pool.New(opts, total, os.Stdout, os.Stderr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parallelizer: %v\n", err)
			return 255
		}
		sigMu.Lock()
		poolPtr = p
		pend := pending
		pending = 0
		sigMu.Unlock()
		if pend != 0 {
			p.Interrupt(pend) // signal arrived just before the pool existed
		}

		code := p.Run(input)
		if err := p.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "parallelizer: %v\n", err)
			if code == 0 {
				code = 255
			}
		}
		return code
	}()
	return code
}

// warnFdLimit complains when -j is likely to exceed the process file
// descriptor soft limit: every running job holds two pipe ends of its own.
// It only warns — the estimate is approximate and the actual failure (if any)
// is reported per job.
func warnFdLimit(jobs int) {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return
	}
	const infinity = ^uint64(0)
	if rl.Cur == 0 || rl.Cur == infinity {
		return
	}
	need := uint64(2)*uint64(jobs) + 64
	if need > rl.Cur {
		fmt.Fprintf(os.Stderr,
			"parallelizer: warning: -j%d needs roughly %d file descriptors but the soft limit is %d; jobs may fail (raise it with: ulimit -n %d)\n",
			jobs, need, rl.Cur, need)
	}
}

// probeTmpDir verifies --tmpdir is writable before any job runs. Failing fast
// beats failing every large-output job partway through a long run.
func probeTmpDir(dir string) error {
	if dir == "" {
		return nil
	}
	f, err := os.CreateTemp(dir, "prlz-probe-*")
	if err != nil {
		return fmt.Errorf("--tmpdir %s is not usable: %w", dir, err)
	}
	name := f.Name()
	f.Close()
	return os.Remove(name)
}

// openInput resolves -a/stdin and pre-counts jobs when a percentage halt rule
// needs the total.
func openInput(opts *cli.Options) (io.Reader, int, func(), error) {
	var r io.Reader
	var closer io.Closer

	if opts.ArgFile == "" || opts.ArgFile == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(opts.ArgFile)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("cannot open %s: %w", opts.ArgFile, err)
		}
		r, closer = f, f
	}

	cleanup := func() {
		if closer != nil {
			closer.Close()
		}
	}

	if opts.Halt.Pct > 0 {
		spooled, n, err := source.Spool(r, opts.KeepCR, opts.TmpDir)
		cleanup()
		if err != nil {
			return nil, 0, nil, fmt.Errorf("cannot spool input: %w", err)
		}
		return spooled, n, func() {
			name := spooled.Name()
			spooled.Close()
			os.Remove(name)
		}, nil
	}

	return r, 0, cleanup, nil
}
