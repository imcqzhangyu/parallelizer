package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
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

		sigCh := make(chan os.Signal, 4)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGPIPE)
		go func() {
			for sig := range sigCh {
				switch sig {
				case syscall.SIGINT:
					p.Interrupt(130)
				case syscall.SIGTERM:
					p.Interrupt(143)
				case syscall.SIGPIPE:
					// A closed stdout must exit 141 like GNU parallel instead
					// of dying from the signal; Output reports the EPIPE.
					p.Interrupt(141)
				case syscall.SIGUSR1:
					p.DumpRunning(os.Stderr)
				}
			}
		}()

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
