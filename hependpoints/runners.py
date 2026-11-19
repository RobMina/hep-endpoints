"""Command runners: how an endpoint executes one shell command on its control host.

A runner is ``run(argv, stdin_bytes_or_None, timeout_s) -> subprocess.CompletedProcess`` (stdout/stderr as bytes).
Endpoints always pass ``["bash", "-c", <script>]``. Tests inject fakes with the same signature.
"""
from __future__ import annotations

import shlex
import subprocess
from typing import Callable, Optional, Sequence

Runner = Callable[[Sequence[str], Optional[bytes], float], subprocess.CompletedProcess]


def ssh_runner(host: str, connect_timeout: int = 15) -> Runner:
    """Run each command on `host` through ``ssh -o BatchMode=yes`` (key or Kerberos, never a password prompt).

    A command with no input gets stdin from /dev/null, so ssh never reads the caller's own stdin (a tool server
    speaking over stdio would otherwise lose its client's requests to a helper ssh)."""
    def run(cmd: Sequence[str], stdin: Optional[bytes], timeout: float) -> subprocess.CompletedProcess:
        remote = " ".join(shlex.quote(c) for c in cmd)
        argv = ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={connect_timeout}", host, remote]
        if stdin is None:
            return subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout)
        return subprocess.run(argv, input=stdin, capture_output=True, timeout=timeout)
    return run


def local_runner(env: Optional[dict] = None) -> Runner:
    """Run each command on this host (the control host is the machine the caller runs on)."""
    def run(cmd: Sequence[str], stdin: Optional[bytes], timeout: float) -> subprocess.CompletedProcess:
        return subprocess.run(list(cmd), input=stdin, capture_output=True, timeout=timeout, env=env)
    return run
