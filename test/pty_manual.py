#!/usr/bin/env python3
"""Manual --pty verification harness (L2/L4 of the TUI test plan).

Runs parallelizer inside a pseudo-terminal so it has a *controlling
terminal* — the environment real users are in — and checks:

L4  default path:  a job trying to open /dev/tty must fail fast
    (no SIGTTIN freeze) and must NOT touch the terminal's termios.
L2  --pty path:    an interactive Python REPL in a job receives typed
    keystrokes through /dev/tty forwarding, prints the result, and the
    USER terminal's termios stays byte-identical (raw-mode isolation).

Usage:  python3 test/pty_manual.py [path-to-parallelizer]
Exit 0 = all checks passed. Requires python3 (the REPL subject).
"""
import os
import pty
import select
import signal
import struct
import subprocess
import sys
import termios
import time

BIN = sys.argv[1] if len(sys.argv) > 1 else "./bin/parallelizer"
failures = []


def ok(name):
    print(f"ok   {name}")


def bad(name, detail):
    failures.append(name)
    print(f"FAIL {name}: {detail}")


def run_in_pty(label, cmd, inject=b"", wait_prompt=None, cap=15.0):
    """Run cmd with a pty as its controlling terminal; optionally type bytes
    when wait_prompt appears in the output. Returns (exit, output, termios_at_start, termios_at_end)."""
    pid, fd = pty.fork()
    if pid == 0:
        os.execvp("bash", ["bash", "-c", cmd])
        os._exit(127)

    # Snapshot the terminal state right after fork (job has not run yet).
    try:
        t_before = termios.tcgetattr(fd)
    except termios.error:
        t_before = None

    out = b""
    injected = False
    start = time.time()
    status = None
    eof = False
    while time.time() - start < cap:
        if (not injected and wait_prompt) and wait_prompt in out:
            os.write(fd, inject)
            injected = True
        r, _, _ = select.select([fd], [], [], 0.2)
        if r and not eof:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                eof = True  # master gives EIO once the session leader is gone
            else:
                if not chunk:
                    eof = True
                else:
                    out += chunk
        p, st = os.waitpid(pid, os.WNOHANG)
        if p:
            status = st
            break
        if eof:
            # Output side is done: the child is dead or about to be — reap it.
            p, st = os.waitpid(pid, 0)
            status = st
            break

    try:
        t_after = termios.tcgetattr(fd)
    except termios.error:
        t_after = None

    if status is None:
        try:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        except OSError:
            pass
        code = "TIMEOUT (killed)"
    else:
        code = os.WEXITSTATUS(status) if os.WIFEXITED(status) else f"SIG{os.WTERMSIG(status)}"
    try:
        os.close(fd)
    except OSError:
        pass
    return code, out.decode(errors="replace"), t_before, t_after, injected


def termios_equal(a, b):
    if a is None or b is None:
        return a is None and b is None
    return a == b  # iflag, oflag, cflag, lflag, ispeed, ospeed, cc


# ---------------------------------------------------------------- L4
print("=== L4: default path — /dev/tty job must fail fast, terminal untouched ===")
cmd = (
    "printf '%s\\n' "
    "'(exec 3<>/dev/tty 2>/dev/null) && echo CTTY-OPEN-OK || echo CTTY-OPEN-FAIL; sleep 0.2' "
    f"| {BIN} -j1; echo TOOL=$?"
)
code, out, before, after, _ = run_in_pty("L4", cmd, cap=8.0)
if code == "TIMEOUT (killed)":
    bad("L4 no freeze", f"tool hung on /dev/tty access: {out!r}")
else:
    ok(f"L4 no freeze (exit={code})")
if "CTTY-OPEN-FAIL" in out:
    ok("L4 job has no controlling terminal")
else:
    bad("L4 no ctty", f"output: {out!r}")
if termios_equal(before, after):
    ok("L4 termios untouched")
else:
    bad("L4 termios changed", "default-path job modified the user's terminal state")

# ---------------------------------------------------------------- L2
print("=== L2: --pty — REPL receives keys via /dev/tty, terminal isolated ===")
cmd = f"printf '%s\\n' 'python3 -q' | {BIN} -j1 --pty; echo TOOL=$?"
code, out, before, after, injected = run_in_pty(
    "L2", cmd, inject=b"print(40 + 2)\nexit()\n", wait_prompt=b">>>", cap=20.0
)
if not injected:
    bad("L2 prompt appeared", f"never saw a REPL prompt: {out!r}")
else:
    ok("L2 prompt appeared (streamed live)")
if "42" in out:
    ok("L2 typed keys reached the job's REPL")
else:
    bad("L2 keystroke forwarding", f"output: {out!r}")
if code == 0:
    ok("L2 clean exit")
else:
    bad("L2 exit", f"exit={code} output={out!r}")
if termios_equal(before, after):
    ok("L2 user terminal termios identical (raw mode stayed in the job's pty)")
else:
    bad("L2 termios polluted", "the REPL's raw mode leaked onto the user's terminal")

print()
if failures:
    print(f"FAILED: {len(failures)} -> {failures}")
    sys.exit(1)
print("all checks passed")
