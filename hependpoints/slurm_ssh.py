"""Slurm over SSH: an endpoint for a Slurm site reached by ``ssh <host>`` with a key.

Settings come from arguments or the environment, never from the package:

  host            $HEPEP_SLURM_HOST        ssh host of the cluster's login node (required when first needed)
  account         $HEPEP_SLURM_ACCOUNT     Slurm account / allocation (required when first needed)
  workdir         $HEPEP_SLURM_WORKDIR     job directories on the cluster (required at construction)
  partition       $HEPEP_SLURM_PARTITION   partition for GPU jobs (default "gpu")
  cpu_partition   $HEPEP_SLURM_CPU_PARTITION   partition for CPU-only jobs (default: none, the site default)
  max_su_per_job  $HEPEP_SLURM_MAX_SU_PER_JOB  per-job budget cap in service units (default: no cap)
  su_per_gpu_h, su_per_core_h   $HEPEP_SLURM_SU_PER_GPU_HOUR, $HEPEP_SLURM_SU_PER_CORE_HOUR   the site's charge
                  rates, used by the budget cap (a cap without rates refuses every job)
  state_dir       $HEPEP_STATE_DIR/<name>  local job records (default ~/.hependpoints/slurm_ssh)
  environment     JobEnvironment           variable prefix, job-name prefix, preamble of every job

A missing required setting raises hependpoints.site.SiteConfigError naming the variable. Subclasses may read other
variable names by overriding HOST_VARS / ACCOUNT_VARS / WORKDIR_VARS / CONFIG_HINT.

Every remote action is one ssh command through ``runner`` (injectable for tests). Job layout on the site:
``<workdir>/<handle>/{job.sh, out/, slurm.out}``; the job script writes its results under ``$<PREFIX>_OUT``. The job id
is kept in a local state dir, so a restarted caller finds its jobs. ``status`` asks ``squeue`` first and ``sacct``
once the job has left the queue; a final state is cached locally. Budget guard: a submit whose estimated charge
(:func:`su_estimate`) exceeds ``max_su_per_job`` is refused before anything is sent.
Durable intents and per-handle locks fence each dispatch attempt; an unrecorded intent is reconciled by exact
scheduler comment, never blindly resubmitted. See docs/slurm-recovery.md for storage and single-cluster limits.
"""
from __future__ import annotations

import datetime as dt
import io
import re
import shlex
import subprocess
import tarfile
import time
from pathlib import Path
from typing import List, Optional, Sequence

from . import EndpointError, JobRecord, JobStatus, Resources, StateDir, site, spec_sha
from .environment import DEFAULT as DEFAULT_ENVIRONMENT, JobEnvironment
from .runners import Runner, ssh_runner  # noqa: F401  (ssh_runner re-exported for callers)
from .submission import IntentStore, durable_json, handle_key, read_json, request_digest, submission_lock

WORKING = {"PENDING", "CONFIGURING", "RUNNING", "COMPLETING", "REQUEUED", "RESIZING", "SUSPENDED", "STAGE_OUT",
           "SIGNALING", "REQUEUE_HOLD", "REQUEUE_FED", "PREEMPTED"}
FAILED = {"FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE", "REVOKED", "SPECIAL_EXIT"}


def su_estimate(r: Resources, su_per_gpu_h: float, su_per_core_h: float) -> float:
    """Estimated charge of one job: wall hours x (GPUs x GPU rate + cores x core rate).

    The rates are the site's, supplied as configuration. Example: a site that charges 53 SU per GPU-hour and 0.5 SU
    per core-hour charges ``su_estimate(Resources(cores=8, gpus=1, minutes=20), 53.0, 0.5)`` = 19 SU."""
    return r.minutes / 60.0 * (r.gpus * su_per_gpu_h + r.cores * su_per_core_h)


def map_state(site_state: str, exit_code: str = "0:0") -> str:
    s = site_state.split()[0].rstrip("+") if site_state else ""
    if s in WORKING:
        return "working"
    if s == "COMPLETED":
        return "completed" if exit_code.split(":")[0] == "0" else "failed"
    if s.startswith("CANCELLED"):
        return "cancelled"
    if s in FAILED:
        return "failed"
    raise EndpointError(f"unknown Slurm state {site_state!r}")


class SlurmSSHEndpoint:
    HOST_VARS = ("HEPEP_SLURM_HOST",)
    ACCOUNT_VARS = ("HEPEP_SLURM_ACCOUNT",)
    WORKDIR_VARS = ("HEPEP_SLURM_WORKDIR",)
    CONFIG_HINT = site.DEFAULT_HINT
    UNREACHABLE_HINT = "is the cluster reachable from this host (network, VPN, ssh key)?"

    def __init__(self, host: str | None = None, account: str | None = None, partition: str | None = None,
                 workdir: str | None = None, state_dir: str | Path | None = None,
                 max_su_per_job: float | None = None, runner: Optional[Runner] = None, name: str = "slurm_ssh",
                 su_per_gpu_h: float | None = None, su_per_core_h: float | None = None, timeout_s: float = 120.0,
                 stream_runner=None, cpu_partition: str | None = None,
                 environment: Optional[JobEnvironment] = None):
        self.name, self._host, self._account = name, host, account
        self.partition = partition or site.setting("HEPEP_SLURM_PARTITION", "gpu")
        self.cpu_partition = cpu_partition or site.setting("HEPEP_SLURM_CPU_PARTITION")
        self.workdir = (workdir or site.require(*self.WORKDIR_VARS, hint=self.CONFIG_HINT)).rstrip("/")
        self.state = StateDir(Path(state_dir).expanduser() if state_dir is not None else site.state_dir(name))
        self.max_su_per_job = (max_su_per_job if max_su_per_job is not None
                               else site.number("HEPEP_SLURM_MAX_SU_PER_JOB"))
        gpu_rate = su_per_gpu_h if su_per_gpu_h is not None else site.number("HEPEP_SLURM_SU_PER_GPU_HOUR")
        core_rate = su_per_core_h if su_per_core_h is not None else site.number("HEPEP_SLURM_SU_PER_CORE_HOUR")
        self.su_rates = None if gpu_rate is None and core_rate is None else (gpu_rate or 0.0, core_rate or 0.0)
        self.env = environment or DEFAULT_ENVIRONMENT
        self.run = runner or (lambda cmd, stdin, timeout: ssh_runner(self.host)(cmd, stdin, timeout))
        self.timeout_s = timeout_s
        self.stream_runner = stream_runner   # tests: (source_argv, host, remote_cmd, timeout) -> node md5 output

    @property
    def host(self) -> str:
        return self._host or site.require(*self.HOST_VARS, hint=self.CONFIG_HINT)

    @host.setter
    def host(self, value: str) -> None:     # explicit retarget: overrides the environment for this endpoint
        self._host = value

    @property
    def account(self) -> str:
        return self._account or site.require(*self.ACCOUNT_VARS, hint=self.CONFIG_HINT)

    @account.setter
    def account(self, value: str) -> None:  # overrides the environment for this endpoint
        self._account = value

    # -- helpers -------------------------------------------------------------------------------------------------
    def _sh(self, script: str, stdin: Optional[bytes] = None, timeout: Optional[float] = None) -> str:
        return self._raw(script, stdin, timeout).decode(errors="replace")

    def _raw(self, script: str, stdin: Optional[bytes] = None, timeout: Optional[float] = None) -> bytes:
        try:
            p = self.run(["bash", "-c", script], stdin, timeout or self.timeout_s)
        except subprocess.TimeoutExpired as e:
            raise EndpointError(f"{self.name}: ssh timed out after {e.timeout} s ({self.UNREACHABLE_HINT})")
        if p.returncode != 0:
            err = (p.stderr or b"").decode(errors="replace").strip()[-400:]
            raise EndpointError(f"{self.name}: remote command failed (rc {p.returncode}): {err}")
        return p.stdout or b""

    def _dir(self, handle: str) -> str:
        return f"{self.workdir}/{handle}"

    def _partition_for(self, r: Resources) -> str:
        return self.partition if r.gpus else (self.cpu_partition or "")

    def header(self, handle: str, r: Resources) -> str:
        gres = f"gpu:{r.gpu_type}:{r.gpus}" if r.gpu_type else f"gpu:{r.gpus}"
        part = self._partition_for(r)
        lines = ["#!/bin/bash", f"#SBATCH -J {self.env.job_prefix}_{handle}"[:60], f"#SBATCH -A {self.account}"]
        if part:
            lines.append(f"#SBATCH -p {part}")
        if r.gpus:
            lines.append(f"#SBATCH --gres={gres}")
        d = self._dir(handle)
        lines += [f"#SBATCH -c {r.cores}", f"#SBATCH --mem={r.mem_gb}G", f"#SBATCH -t {r.minutes}",
                  f"#SBATCH -o {d}/slurm.out"]
        return "\n".join(lines) + f"\nexport {self.env.job_dir_var}={d} {self.env.out_var}={d}/out\n"

    def body(self, handle: str, script: str, r: Resources) -> str:
        """The complete job file: header, output directory, the environment preamble, the caller's script."""
        return (self.header(handle, r) + f"umask 077\nmkdir -p \"${self.env.out_var}\"\n" + self.env.preamble
                + script)

    def admit_resources(self, resources: Resources) -> None:
        """Apply the configured per-job budget without contacting Slurm."""
        if self.max_su_per_job is None:
            return
        if self.su_rates is None:
            raise EndpointError(f"{self.name}: max_su_per_job is set but no SU rates are configured "
                                "(su_per_gpu_h / su_per_core_h)")
        su = su_estimate(resources, *self.su_rates)
        if su > self.max_su_per_job:
            raise EndpointError(f"estimated {su:.0f} SU exceeds max_su_per_job {self.max_su_per_job:.0f}")

    def _target(self, resources):
        return {"host": self.host, "account": self.account, "workdir": self.workdir,
                "partition": self._partition_for(resources)}

    def _intents(self) -> IntentStore:
        return IntentStore(self.state.root, self.env.job_prefix)

    def _load_record(self, handle):
        handle_key(handle)
        path = self.state._path(handle)
        data = read_json(path)
        if data is None and not path.exists():
            return None
        try:
            rec = JobRecord(**data)
            if (rec.handle != handle or not re.fullmatch(r"[1-9][0-9]*", rec.job_id)
                    or not re.fullmatch(r"[0-9a-f]{16}", rec.spec_sha) or not isinstance(rec.remote_dir, str)
                    or not isinstance(rec.resources, dict)):
                raise ValueError("invalid job record identity")
            intent = self._intents().get(handle)
            if intent is not None:
                resources = Resources(**rec.resources)
                if (intent["target"] != self._target(resources) or intent["spec_sha"] != rec.spec_sha
                        or intent["resources"] != rec.resources or intent["remote_dir"] != rec.remote_dir
                        or intent["created_at"] != rec.submitted_at or intent.get("unsupported_ack")):
                    raise ValueError("record/intent identity or endpoint target mismatch")
            return rec
        except (ValueError, TypeError, KeyError) as exc:
            raise EndpointError(f"{handle!r}: invalid job record or target: {exc}") from exc

    def _save_record(self, rec):
        durable_json(self.state._path(rec.handle), rec.__dict__)

    def _adopt(self, intent, job_id):
        rec = JobRecord(intent["handle"], intent["spec_sha"], job_id, intent["remote_dir"],
                        intent["created_at"], intent["resources"])
        self._save_record(rec)
        return rec

    @staticmethod
    def _recovery_ids(text, token):
        ids = set()
        for line in text.splitlines():
            if not line.strip():
                continue
            fields = line.strip().split("|", 1)
            if len(fields) != 2:
                raise EndpointError("uninterpretable scheduler recovery row")
            job_id, comment = (field.strip() for field in fields)
            if comment == token:
                if not re.fullmatch(r"[1-9][0-9]*", job_id):
                    raise EndpointError(f"unsupported matching allocation ID {job_id!r}")
                ids.add(job_id)
        return ids

    def _reconcile(self, intent):
        if intent.get("unsupported_ack"):
            raise EndpointError(f"{intent['handle']!r}: nonlocal or compound sbatch acknowledgement; "
                                "operator investigation required; intent retained, no resubmission")
        token = intent["token"]
        start = dt.datetime.fromtimestamp(max(0, intent["created_at"] - 86400), dt.timezone.utc)
        since = start.strftime("%Y-%m-%dT%H:%M:%S")
        q = self._sh("unset SLURM_CLUSTERS; squeue --local -h --me -o '%i|%.128k'")
        a = self._sh(f"unset SLURM_CLUSTERS; TZ=UTC sacct --local -n -X -P --duplicates --user=\"$(id -un)\" "
                     f"--starttime={since} -o JobIDRaw,Comment%128")
        ids = self._recovery_ids(q, token) | self._recovery_ids(a, token)
        if len(ids) != 1:
            detail = "no visible matching allocation" if not ids else f"multiple matching allocations {sorted(ids)}"
            raise EndpointError(f"{intent['handle']!r}: unresolved submission ({detail}); retry the same request "
                                "to reconcile only; do not delete the intent or submit a new handle without investigation")
        return self._adopt(intent, ids.pop())

    # -- interface -----------------------------------------------------------------------------------------------
    def submit(self, handle: str, script: str, resources: Resources) -> JobRecord:
        with submission_lock(self.state.root, handle):
            return self._submit_locked(handle, script, resources)

    def _submit_locked(self, handle, script, resources):
        sha = spec_sha(script, resources)
        old = self._load_record(handle)
        if old is not None:
            if old.spec_sha != sha:
                raise EndpointError(f"{handle!r} was already submitted with a different script or resources")
            return old
        target = self._target(resources)
        d = self._dir(handle)
        body = self.body(handle, script, resources)
        request = request_digest(body, resources.__dict__, target)
        intents = self._intents()
        intent = intents.get(handle)
        if intent is not None:
            if (intent["request_sha"] != request or intent["spec_sha"] != sha or intent["target"] != target
                    or intent["remote_dir"] != d or intent["resources"] != resources.__dict__):
                raise EndpointError(f"{handle!r}: pending submission has a different request or target")
            return self._reconcile(intent)
        self.admit_resources(resources)
        intent = intents.create(handle, sha, request, target, resources.__dict__.copy(), d, time.time())
        try:
            out = self._sh(f"set -e; unset SLURM_CLUSTERS SBATCH_CLUSTERS; mkdir -p {shlex.quote(d)}/out; "
                           f"cat > {shlex.quote(d)}/job.sh; cd {shlex.quote(d)} && "
                           f"sbatch --parsable --comment={shlex.quote(intent['token'])} job.sh", stdin=body.encode())
            job_id = out.strip()
            if re.fullmatch(r"[1-9][0-9]*(?:;[^\n]+|[+_][^\n]+)", job_id):
                intent["unsupported_ack"] = job_id
                durable_json(intents._path(handle), intent)
            if not re.fullmatch(r"[1-9][0-9]*", job_id):
                raise EndpointError(f"unsupported or missing local sbatch job ID: {out!r}")
            return self._adopt(intent, job_id)
        except (EndpointError, OSError) as exc:
            raise EndpointError(f"{handle!r}: submission outcome uncertain; intent retained; retry the same request "
                                f"to reconcile only (no automatic resubmission): {exc}") from exc

    def _record(self, handle: str) -> JobRecord:
        rec = self._load_record(handle)
        if rec is None:
            raise EndpointError(f"{self.name}: no job {handle!r}")
        return rec

    def status(self, handle: str) -> JobStatus:
        with submission_lock(self.state.root, handle):
            return self._status_locked(handle)

    def _status_locked(self, handle):
        rec = self._record(handle)
        if rec.final:
            return JobStatus(**rec.final)
        q = self._sh(f"squeue -h -j {rec.job_id} -o '%T|%r' 2>/dev/null || true").strip()
        if q:
            st, reason = (q.splitlines()[0].split("|") + [""])[:2]
            return JobStatus("working", reason if reason not in ("None", "") else "", st, rec.job_id)
        a = self._sh(f"sacct -n -X -P -j {rec.job_id} -o State,ExitCode").strip()
        if not a:
            return JobStatus("working", "not yet in squeue or sacct", "", rec.job_id)
        st, code = (a.splitlines()[0].split("|") + ["0:0"])[:2]
        js = JobStatus(map_state(st, code), f"slurm {st} exit {code}", st, rec.job_id)
        rec.final = js.__dict__.copy()
        self._save_record(rec)
        return js

    def fetch(self, handle: str, dest: Path) -> List[Path]:
        """Copy <remote_dir>/out and slurm.out into dest/<handle>/ (tar over ssh); returns the local files."""
        rec = self._record(handle)
        d = shlex.quote(rec.remote_dir)
        raw = self._raw(f"cd {d} && if [ -f slurm.out ]; then tar -cf - out slurm.out; else tar -cf - out; fi",
                        timeout=600)
        target = Path(dest) / handle
        target.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(raw)) as t:
            t.extractall(target, filter="data")
        return sorted(p for p in target.rglob("*") if p.is_file())

    # -- extras for tasks that stream inputs into a running job ----------------------------------------------------
    def read_file(self, handle: str, rel: str) -> str:
        """Contents of <remote_dir>/<rel>, or "" if it does not exist yet."""
        rec = self._record(handle)
        return self._sh(f"cat {shlex.quote(rec.remote_dir + '/' + rel)} 2>/dev/null || true")

    def _srun(self, rec: JobRecord, node: str) -> str:
        return f"srun --jobid={rec.job_id} --overlap -N1 -n1 --mem=0 -w {shlex.quote(node)}"

    def run_on_node(self, handle: str, node: str, cmd: str, timeout: float = 120) -> str:
        """Run `cmd` as a step of the running job on its node (compute nodes take no ssh)."""
        rec = self._record(handle)
        return self._sh(f"{self._srun(rec, node)} sh -c {shlex.quote(cmd)}", timeout=timeout)

    def stream_in(self, handle: str, node: str, dest: str, source_argv: Sequence[str], timeout: float = 3600) -> str:
        """Pipe the stdout of `source_argv` (run on this host, e.g. `ssh <source host> cat <file>`) into `dest` on the
        job's node, without touching this host's disk; returns the md5 the node computed."""
        rec = self._record(handle)
        remote = f"{self._srun(rec, node)} sh -c {shlex.quote(f'cat > {shlex.quote(dest)} && md5sum {shlex.quote(dest)}')}"
        if self.stream_runner is not None:
            out = self.stream_runner(list(source_argv), self.host, remote, timeout)
        else:
            src = subprocess.Popen(list(source_argv), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            try:
                p = subprocess.run(["ssh", "-o", "BatchMode=yes", self.host, remote], stdin=src.stdout,
                                   capture_output=True, timeout=timeout)
            finally:
                src.stdout.close()
                src.wait(timeout=60)
            if p.returncode or src.returncode:
                raise EndpointError(f"{self.name}: streaming into {dest} failed (rc {p.returncode}/{src.returncode}): "
                                    f"{p.stderr.decode(errors='replace')[-300:]}")
            out = p.stdout.decode()
        return out.split()[0] if out.split() else ""

    def times(self, handle: str) -> dict:
        """Slurm start/end (epoch s) and elapsed seconds of a finished job."""
        rec = self._record(handle)
        a = self._sh(f"sacct -n -X -P -j {rec.job_id} -o Start,End,ElapsedRaw").strip().splitlines()
        s, e, el = (a[0].split("|") + ["", "", "0"])[:3] if a else ("", "", "0")
        ts = lambda x: dt.datetime.fromisoformat(x).timestamp() if x and x[0].isdigit() else time.time()  # noqa: E731
        return {"start": ts(s), "end": ts(e), "elapsed_s": float(el or 0), "job_id": rec.job_id}

    def cancel(self, handle: str) -> str:
        rec = self._record(handle)
        st = self.status(handle)
        if st.state != "working":
            return st.state
        self._sh(f"scancel {rec.job_id}")
        return "cancelled"
