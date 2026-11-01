"""Fermilab EAF (Elastic Analysis Facility): an endpoint that runs jobs in the user's JupyterHub server on an A100 MIG
slice, driven from an FNAL interactive host with a hub API token.

EAF has no batch queue and no ssh: one server per user, holding one MIG slice (1g.10gb, 2g.20gb or 4g.40gb). A job is
a script started detached in a server terminal (websocket); it reads its inputs in place from FNAL storage and writes
``<workdir>/<handle>/{job.sh, job.log, job.pid, job.done, out/}`` on storage that EAF and the controlling host both
mount (under ``shared_prefix``), which that host reads directly: no transfer. The adapter

* starts the server when none runs, trying slices in ``slices`` order (largest first by default; a slice that does not
  become ready within ``start_timeout_s`` counts as unavailable and its spawn is cancelled), and reports "no capacity"
  when none starts;
* runs one job at a time (one slice per user) and queues the rest: a submit while a job runs is recorded as queued
  (state ``working``, message "queued") and launched, oldest first, by the next ``status`` call that finds the slice
  free, so the points of a batch run back to back on one slice;
* keeps the server alive while jobs run: every ``status`` call posts user activity to the hub (the idle culler counts
  only activity) and jobs write a heartbeat; ``max_age_s`` (if the hub enforces one) refuses jobs whose wall time
  would outlive the server, and a job that dies with its server is reported ``failed`` with that reason;
* maps states from the files: ``job.done`` holds the exit code; no ``job.done`` and a live server is ``working``;
  no ``job.done`` and no server, or a job past its wall time, is ``failed``;
* cancels by killing the job's process group (``job.pid``), not the server (a queued job is just dropped);
* stops the server when it falls idle: ``after_results`` and ``status`` stop it once no job runs or waits and
  ``linger_s`` (default 120 s) have passed since the last job ended; ``stop_idle`` does the same at once
  (``python -m hependpoints.eaf stop-idle``, or a cron).

Configuration (arguments, else the environment):

  workdir         $HEPEP_EAF_WORKDIR         job directories, under shared_prefix (required)
  shared_prefix   $HEPEP_EAF_SHARED_PREFIX   storage prefix mounted by EAF and by the controlling host (required)
  token_file      $HEPEP_EAF_TOKEN_FILE, else $EAF_TOKEN_FILE   hub API token, mode 600 (needed for hub calls)
  hub             $HEPEP_EAF_HUB             default https://analytics-hub.fnal.gov
  linger_s        $HEPEP_EAF_LINGER_S, else $EAF_LINGER_S    default 120
  max_age_s       $HEPEP_EAF_MAX_AGE_S, else $EAF_MAX_AGE_S  server lifetime cap, if the hub enforces one
  state_dir       $HEPEP_STATE_DIR/<name>    local job records (default ~/.hependpoints/eaf)
  environment     JobEnvironment             variable prefix, preamble, and the ``runtime`` hook (``runtime()``)
  profile, image  the hub spawner profile and the GPU image pattern (FNAL EAF defaults below)

A missing required setting raises hependpoints.site.SiteConfigError naming it. ``hub_client`` is injectable for
tests (see FakeHub in the tests). Subclasses may read other variable names by overriding the *_VARS attributes, and
point the idle watcher at their own module with WATCHER_MODULE.
"""
from __future__ import annotations

import json
import os
import shlex
import ssl
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

from . import EndpointError, JobRecord, JobStatus, Resources, StateDir, site, spec_sha
from .environment import DEFAULT as DEFAULT_ENVIRONMENT, JobEnvironment

PROFILE = "fife-sl7-unpriv-gpu-a100"
IMAGE = "fife-al9-unpriv-htcondor-gpu-a100-{gb}gb"
SLICE_GB = {"4g.40gb": 40, "2g.20gb": 20, "1g.10gb": 10}
HUB = "https://analytics-hub.fnal.gov"


class HubClient:
    """The JupyterHub REST calls the endpoint needs (token auth), plus terminal stdin over a websocket."""

    def __init__(self, hub: str, token_file: str, timeout_s: float = 60.0):
        self.hub, self.token_file, self.timeout_s = hub.rstrip("/"), token_file, timeout_s

    def _token(self) -> str:
        if not self.token_file or not os.path.exists(self.token_file):
            raise EndpointError("eaf: no hub token file (HEPEP_EAF_TOKEN_FILE); make one at <hub>/hub/token")
        if os.stat(self.token_file).st_mode & 0o077:
            raise EndpointError(f"eaf: {self.token_file} is readable by others; chmod 600 it")
        return open(self.token_file).read().strip()

    def call(self, method: str, path: str, body: Optional[dict] = None):
        req = urllib.request.Request(self.hub + path, method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={"Authorization": "token " + self._token(),
                                              "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s, context=ssl.create_default_context()) as r:
                data = r.read()
                return r.status, (json.loads(data) if data else None)
        except urllib.error.HTTPError as e:
            raise EndpointError(f"eaf: {method} {path} -> HTTP {e.code}: {e.read().decode(errors='replace')[:300]}")
        except (urllib.error.URLError, TimeoutError) as e:
            raise EndpointError(f"eaf: {method} {path} failed: {e} (EAF is reachable only from the FNAL network)")

    def user(self) -> dict:
        return self.call("GET", "/hub/api/user")[1]

    def server(self) -> Optional[dict]:
        return (self.user().get("servers") or {}).get("")

    def start(self, options: dict) -> None:
        self.call("POST", f"/hub/api/users/{self.user()['name']}/server", options)

    def stop(self) -> None:
        self.call("DELETE", f"/hub/api/users/{self.user()['name']}/server")

    def user_name(self) -> str:
        return self.user()["name"]

    def send(self, line: str) -> None:
        """Type one line into a new server terminal (needs the optional websocket-client package)."""
        import websocket
        s = self.server()
        if not (s and s.get("ready")):
            raise EndpointError("eaf: no running server")
        _, term = self.call("POST", s["url"] + "api/terminals", {})
        url = self.hub.replace("https://", "wss://") + s["url"] + f"terminals/websocket/{term['name']}"
        ws = websocket.create_connection(url, header=["Authorization: token " + self._token()], timeout=30)
        try:
            ws.send(json.dumps(["stdin", line + "\r"]))
            time.sleep(2)
        finally:
            ws.close()


def slice_of(options: Optional[dict]) -> str:
    img = (options or {}).get("imagegpu", "")
    for name, gb in SLICE_GB.items():
        if img.endswith(f"-{gb}gb"):
            return name
    return ""


class EAFEndpoint:
    WORKDIR_VARS = ("HEPEP_EAF_WORKDIR",)
    SHARED_PREFIX_VARS = ("HEPEP_EAF_SHARED_PREFIX",)
    TOKEN_VARS = ("HEPEP_EAF_TOKEN_FILE", "EAF_TOKEN_FILE")
    LINGER_VARS = ("HEPEP_EAF_LINGER_S", "EAF_LINGER_S")
    MAX_AGE_VARS = ("HEPEP_EAF_MAX_AGE_S", "EAF_MAX_AGE_S")
    CONFIG_HINT = site.DEFAULT_HINT
    WATCHER_MODULE = "hependpoints.eaf"

    def __init__(self, workdir: str | None = None, shared_prefix: str | None = None,
                 state_dir: str | Path | None = None, token_file: Optional[str] = None, hub: str | None = None,
                 slices: Sequence[str] = ("4g.40gb", "2g.20gb", "1g.10gb"),
                 start_timeout_s: float = 420.0, poll_s: float = 10.0, hub_client: Optional[HubClient] = None,
                 linger_s: Optional[float] = None, max_age_s: Optional[float] = None,
                 clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep,
                 name: str = "eaf", environment: Optional[JobEnvironment] = None, profile: str = PROFILE,
                 image: str = IMAGE, **_ignored):
        self.name = name
        self.workdir = (workdir or site.require(*self.WORKDIR_VARS, hint=self.CONFIG_HINT)).rstrip("/")
        self.shared_prefix = shared_prefix or site.require(*self.SHARED_PREFIX_VARS, hint=self.CONFIG_HINT)
        if not self.workdir.startswith(self.shared_prefix):
            raise EndpointError(f"eaf: workdir must be under {self.shared_prefix} (shared by EAF and the host)")
        bad = [s for s in slices if s not in SLICE_GB]
        if bad or not slices:
            raise EndpointError(f"eaf: unknown slices {bad}; choose from {list(SLICE_GB)}")
        self.slices = tuple(slices)
        self.profile, self.image = profile, image
        self.env = environment or DEFAULT_ENVIRONMENT
        self.state = StateDir(Path(state_dir).expanduser() if state_dir is not None else site.state_dir(name))
        self.hub = hub_client or HubClient(hub or site.setting("HEPEP_EAF_HUB", HUB),
                                           token_file or self._first(self.TOKEN_VARS) or "")
        self.start_timeout_s, self.poll_s = start_timeout_s, poll_s
        self.clock, self.sleep = clock, sleep
        self.linger_s = float(self._first(self.LINGER_VARS) or 120 if linger_s is None else linger_s)
        env_age = self._first(self.MAX_AGE_VARS)
        self.max_age_s = float(env_age) if max_age_s is None and env_age else max_age_s

    @staticmethod
    def _first(names) -> Optional[str]:
        for n in names:
            v = site.setting(n)
            if v:
                return v
        return None

    def runtime(self) -> Dict[str, str]:
        """Named shell definitions supplied by the experiment's JobEnvironment.runtime hook."""
        if self.env.runtime is None:
            raise EndpointError(f"{self.name}: no runtime hook configured (JobEnvironment.runtime)")
        return self.env.runtime(self)

    # -- files on shared storage ------------------------------------------------------------------------------------
    def _dir(self, handle: str) -> Path:
        return Path(self.workdir) / handle

    def _read(self, handle: str, name: str) -> Optional[str]:
        p = self._dir(handle) / name
        return p.read_text() if p.exists() else None

    # -- server --------------------------------------------------------------------------------------------------
    def _ready_server(self, resources: Resources) -> dict:
        """The running server, or start one: slices in order."""
        s = self.hub.server()
        if s and s.get("ready"):
            return s
        if s and s.get("pending") == "spawn":         # started by someone else just now: wait for it
            t0 = self.clock()
            while self.clock() - t0 < self.start_timeout_s:
                self.sleep(self.poll_s)
                s = self.hub.server()
                if s and s.get("ready"):
                    return s
                if not (s and s.get("pending")):
                    break
        s = self.hub.server()
        if s and s.get("ready"):
            return s
        if s and s.get("pending"):
            raise EndpointError("eaf: a server is starting or stopping; retry shortly")
        tried = []
        for sl in self.slices:
            self.hub.start({"profile": self.profile, "imagegpu": self.image.format(gb=SLICE_GB[sl])})
            t0 = self.clock()
            while self.clock() - t0 < self.start_timeout_s:
                s = self.hub.server()
                if s and s.get("ready"):
                    return s
                self.sleep(self.poll_s)
            tried.append(sl)
            self.hub.stop()                     # cancel the spawn that did not get a slice
            self.sleep(self.poll_s)
        raise EndpointError(f"eaf: no capacity: no slice of {tried or list(self.slices)} started within "
                            f"{self.start_timeout_s:.0f} s")

    def _records(self) -> List[JobRecord]:
        return [JobRecord(**json.loads(p.read_text())) for p in sorted(self.state.root.glob("*.json"))]

    def _running_job(self) -> Optional[str]:
        """Handle of a launched job that still runs here (one slice per user), or None."""
        for rec in self._records():
            if rec.final is None and rec.resources.get("launched_at") and self._job_state(rec).state == "working":
                return rec.handle
        return None

    def _queue(self) -> List[JobRecord]:
        """Jobs waiting for the slice, oldest first."""
        q = [r for r in self._records() if r.final is None and not r.resources.get("launched_at")]
        return sorted(q, key=lambda r: r.submitted_at)

    def _server_age(self, s: dict) -> Optional[float]:
        st = s.get("started")
        if not st:
            return None
        import datetime as _dt
        t = _dt.datetime.fromisoformat(st.replace("Z", "+00:00")).timestamp()
        return self.clock() - t

    def launch_line(self, d: Path, resources: Resources) -> str:
        """The line typed into a server terminal: one detached process group (setsid), its leader's pid in job.pid
        for cancel, a watchdog for the wall time, and the exit code in job.done (143 on TERM, with a marker telling
        a cancel from a timeout)."""
        q = {k: shlex.quote(str(d / k)) for k in ("job.sh", "job.log", "job.pid", "job.done", "timedout", "job.alive")}
        wall = max(1, int(resources.minutes)) * 60
        inner = (f"trap 'echo exit=143 > {q['job.done']}; exit 143' TERM; echo $$ > {q['job.pid']}; "
                 f"( sleep {wall}; echo 1 > {q['timedout']}; kill -TERM -- -$$ ) & w=$!; "
                 f"( while :; do date +%s > {q['job.alive']}; sleep 60; done ) & hb=$!; "
                 f"bash {q['job.sh']}; rc=$?; kill $w $hb 2>/dev/null; echo exit=$rc > {q['job.done']}")
        return f"setsid nohup bash -c {shlex.quote(inner)} > {q['job.log']} 2>&1 < /dev/null &"

    def header(self, d: Path) -> str:
        return (f"#!/bin/bash\nexport {self.env.job_dir_var}={shlex.quote(str(d))} "
                f"{self.env.out_var}={shlex.quote(str(d / 'out'))}\numask 077\nnvidia-smi -L\n")

    # -- interface -----------------------------------------------------------------------------------------------
    def _lock(self):
        """One submit/launch at a time across threads and processes (callers submit independent steps from threads,
        and two concurrent server starts get HTTP 500 from the hub): an flock on the state dir."""
        import contextlib
        import fcntl

        @contextlib.contextmanager
        def held():
            with open(self.state.root / ".lock", "w") as fh:
                fcntl.flock(fh, fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(fh, fcntl.LOCK_UN)
        return held()

    def admit_resources(self, resources: Resources) -> None:
        """Check deterministic lifetime/device restrictions without starting a server."""
        if resources.gpus > 1:
            raise EndpointError("eaf: one MIG slice per user; gpus must be 0 or 1")
        if self.max_age_s and 60 * resources.minutes > self.max_age_s:
            raise EndpointError(f"eaf: wall time {resources.minutes} min exceeds the server lifetime "
                                f"{self.max_age_s / 60:.0f} min")

    def submit(self, handle: str, script: str, resources: Resources) -> JobRecord:
        with self._lock():
            return self._submit(handle, script, resources)

    def _submit(self, handle: str, script: str, resources: Resources) -> JobRecord:
        sha = spec_sha(script, resources)
        old = self.state.get(handle)
        if old is not None:
            if old.spec_sha != sha:
                raise EndpointError(f"{handle!r} was already submitted with a different script or resources")
            return old
        self.admit_resources(resources)
        d = self._dir(handle)
        (d / "out").mkdir(parents=True, exist_ok=True)
        os.chmod(d, 0o700)
        (d / "job.sh").write_text(self.header(d) + self.env.preamble + script)
        rec = JobRecord(handle, sha, "", str(d), self.clock(), dict(resources.__dict__))
        if self._running_job() or self._queue():
            self.state.put(rec)                     # queued: launched by a later status call
            return rec
        self._launch(rec)                           # may raise "no capacity": nothing recorded then
        return rec

    def _launch(self, rec: JobRecord) -> None:
        res = Resources(**{k: rec.resources[k] for k in Resources.__dataclass_fields__ if k in rec.resources})
        s = self._ready_server(res)
        if self.max_age_s:
            age = self._server_age(s) or 0.0
            if age + 60 * res.minutes > self.max_age_s:
                raise EndpointError(f"eaf: the server ({age / 60:.0f} min old) would reach its lifetime "
                                    f"{self.max_age_s / 60:.0f} min before a {res.minutes} min job ends; retry after it stops")
        self.hub.send(self.launch_line(Path(rec.remote_dir), res))
        rec.resources.update(slice=slice_of(s.get("user_options")), launched_at=self.clock())
        self.state.put(rec)

    def _advance(self) -> None:
        """Launch the oldest queued job if the slice is free (one job at a time)."""
        with self._lock():
            self._advance_locked()

    def _advance_locked(self) -> None:
        if self._running_job():
            return
        q = self._queue()
        if not q:
            return
        try:
            self._launch(q[0])
        except EndpointError as e:              # no capacity now: stays queued, reported in its status message
            q[0].resources["last_launch_error"] = str(e)[:200]
            self.state.put(q[0])

    def _record(self, handle: str) -> JobRecord:
        rec = self.state.get(handle)
        if rec is None:
            raise EndpointError(f"{self.name}: no job {handle!r}")
        return rec

    def _job_state(self, rec: JobRecord) -> JobStatus:
        """State of a launched job from its files and the server (no queue handling, no side effects but caching)."""
        if rec.final:
            return JobStatus(**rec.final)
        slice_, t0 = rec.resources.get("slice", ""), rec.resources.get("launched_at", rec.submitted_at)
        h = rec.handle
        done = self._read(h, "job.done")
        if done is not None:
            code = done.strip().split("=")[-1]
            cancelled, timedout = self._read(h, "cancelled") is not None, self._read(h, "timedout") is not None
            state = "completed" if code == "0" else ("cancelled" if cancelled and not timedout else "failed")
            js = JobStatus(state, "timed out (wall time)" if timedout else f"exit {code}", slice_, rec.job_id)
        elif self.clock() - t0 > 60 * rec.resources.get("minutes", 60) + 120:
            js = JobStatus("failed", "no exit record after the wall time (server culled or stopped?)", slice_, rec.job_id)
        elif self._read(h, "job.pid") is None and self.clock() - t0 > 120:
            js = JobStatus("failed", "the job never started in the server terminal", slice_, rec.job_id)
        else:
            s = self.hub.server()
            if not (s and s.get("ready")):
                alive = self._read(h, "job.alive")
                last = f"; last heartbeat {self.clock() - float(alive):.0f} s ago" if alive and alive.strip().isdigit() else ""
                js = JobStatus("failed", "the EAF server stopped before the job finished (culled, or stopped by hand)"
                               + last, slice_, rec.job_id)
            else:
                return JobStatus("working", "", slice_, rec.job_id)
        rec.final = js.__dict__.copy()
        rec.resources["ended_at"] = self.clock()
        self.state.put(rec)
        return js

    def status(self, handle: str) -> JobStatus:
        rec = self._record(handle)
        if rec.final:
            return JobStatus(**rec.final)
        self._advance()
        rec = self._record(handle)
        if not rec.resources.get("launched_at"):
            ahead = [r.handle for r in self._queue()].index(handle)
            err = rec.resources.get("last_launch_error", "")
            return JobStatus("working", f"queued ({ahead} ahead){'; ' + err if err else ''}", "", rec.job_id)
        js = self._job_state(rec)
        if js.state == "working":
            self.keepalive()
        else:
            self._advance()
            self.stop_idle(linger=True)
        return js

    def keepalive(self) -> None:
        """Tell the hub the server is in use (its idle culler counts activity, not CPU)."""
        try:
            import datetime as _dt
            now = _dt.datetime.fromtimestamp(self.clock(), _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            self.hub.call("POST", f"/hub/api/users/{self.hub.user_name()}/activity", {"last_activity": now})
        except (EndpointError, AttributeError, KeyError):
            pass                                    # best effort: a failed ping must not fail a status call

    def fetch(self, handle: str, dest: Path) -> List[Path]:
        """Copy out/ and job.log into dest/<handle>/ (both are on shared storage already)."""
        import shutil
        rec = self._record(handle)
        src, target = Path(rec.remote_dir), Path(dest) / handle
        if (src / "out").is_dir():
            shutil.copytree(src / "out", target / "out", dirs_exist_ok=True)
        target.mkdir(parents=True, exist_ok=True)
        if (src / "job.log").exists():
            shutil.copy2(src / "job.log", target / "job.log")
        return sorted(p for p in target.rglob("*") if p.is_file())

    def cancel(self, handle: str) -> str:
        rec = self._record(handle)
        if rec.final:
            return rec.final["state"]
        if not rec.resources.get("launched_at"):            # queued: drop it
            rec.final = JobStatus("cancelled", "cancelled while queued", "", rec.job_id).__dict__.copy()
            rec.resources["ended_at"] = self.clock()
            self.state.put(rec)
            return "cancelled"
        st = self._job_state(rec)
        if st.state != "working":
            return st.state
        pid = (self._read(handle, "job.pid") or "").strip()
        if not pid.isdigit():
            raise EndpointError(f"eaf: {handle!r} has no pid yet; retry")
        (Path(rec.remote_dir) / "cancelled").write_text(str(self.clock()))
        self.hub.send(f"kill -TERM -- -{pid} 2>/dev/null || kill -TERM {pid}")
        return "cancelled"

    def after_results(self, handle: str) -> None:
        """Called once a job's results are read: launch the next queued job, or stop an idle server. With a linger
        time and nothing left to run, a detached watcher stops the server once the linger has passed (the caller may
        never call again after its last result)."""
        self._advance()
        if not self.stop_idle(linger=True) and self.linger_s > 0 and not (self._running_job() or self._queue()):
            self._spawn_watcher()

    def _watcher_argv(self) -> List[str]:
        import sys
        return [sys.executable, "-m", self.WATCHER_MODULE, "stop-idle", "--wait",
                "--state-dir", str(self.state.root), "--workdir", self.workdir,
                "--shared-prefix", self.shared_prefix]

    def _spawn_watcher(self) -> None:
        """Start `python -m <WATCHER_MODULE> stop-idle --wait` detached, unless one already waits."""
        import subprocess
        pidf = self.state.root / ".watcher.pid"
        if pidf.exists():
            try:
                os.kill(int(pidf.read_text().strip()), 0)
                return                                  # one is already waiting
            except (ValueError, ProcessLookupError, PermissionError):
                pass
        env = dict(os.environ, HEPEP_EAF_LINGER_S=str(self.linger_s))
        env.setdefault("HEPEP_EAF_TOKEN_FILE", getattr(self.hub, "token_file", "") or "")
        log = open(self.state.root / ".watcher.log", "a")
        pr = subprocess.Popen(self._watcher_argv(), stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=env,
                              start_new_session=True)
        pidf.write_text(str(pr.pid))

    def wait_and_stop(self, max_wait_s: float = 3600.0) -> bool:
        """Sleep until linger_s after the last job ended, then stop the server if still idle (a new job resets it)."""
        t0 = self.clock()
        while self.clock() - t0 < max_wait_s:
            if self._running_job() or self._queue():
                return False                            # work arrived: its own after_results will handle the stop
            ends = [r.resources.get("ended_at", 0.0) for r in self._records()]
            left = self.linger_s - (self.clock() - max(ends)) if ends else 0.0
            if left <= 0:
                return self.stop_idle(linger=True)
            self.sleep(min(left + 1, 60))
        return False

    def stop_idle(self, linger: bool = False) -> bool:
        """Stop the server when no job runs or waits; with linger, only linger_s after the last job ended (so points
        arriving in a burst reuse the slice). True if a stop was sent."""
        if self._running_job() or self._queue():
            return False
        if linger:
            ends = [r.resources.get("ended_at", 0.0) for r in self._records()]
            if ends and self.clock() - max(ends) < self.linger_s:
                return False
        s = self.hub.server()
        if s and (s.get("ready") or s.get("pending")):
            self.hub.stop()
            return True
        return False


def main(argv=None, endpoint_cls=EAFEndpoint, default_state_dir: Optional[str] = None) -> int:
    """python -m hependpoints.eaf {server|jobs|stop-idle} [--state-dir D] (needs HEPEP_EAF_TOKEN_FILE).

    An experiment package reuses this with its own endpoint class and default state dir."""
    import argparse
    ap = argparse.ArgumentParser(description=main.__doc__)
    ap.add_argument("cmd", choices=("server", "jobs", "stop-idle"))
    ap.add_argument("--state-dir", default=default_state_dir or str(site.state_dir("eaf")))
    ap.add_argument("--workdir", default=None, help="job directories (default: the endpoint's)")
    ap.add_argument("--shared-prefix", default=None, help="shared storage prefix (default: the endpoint's)")
    ap.add_argument("--wait", action="store_true", help="stop-idle: wait for the linger time first (the watcher)")
    a = ap.parse_args(argv)
    kw = {k: v for k, v in (("workdir", a.workdir), ("shared_prefix", a.shared_prefix)) if v}
    ep = endpoint_cls(state_dir=a.state_dir, **kw)
    if a.cmd == "server":
        s = ep.hub.server()
        print(json.dumps(s and {k: s.get(k) for k in ("ready", "pending", "started", "user_options")}, indent=1))
    elif a.cmd == "jobs":
        for rec in ep._records():
            print(rec.handle, ep.status(rec.handle).__dict__)
    elif a.wait:
        try:
            print(time.strftime("%F %T"), "watcher:", "stopped" if ep.wait_and_stop() else "not stopped (work arrived)")
        finally:
            (ep.state.root / ".watcher.pid").unlink(missing_ok=True)
    else:
        print("stopped" if ep.stop_idle() else "not stopped (a job runs, or no server)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
