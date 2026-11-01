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
  gpu_table       GpuTable                 the site's GPU types (gres name, partition, rates, per-job limits) and the
                  "auto" rule's candidates; default none: every job is submitted as requested
  gpu_type        "auto", a type name, or None   how GPU jobs get their card (default: the table's ``default``)

A missing required setting raises hependpoints.site.SiteConfigError naming the variable. Subclasses may read other
variable names by overriding HOST_VARS / ACCOUNT_VARS / WORKDIR_VARS / CONFIG_HINT.

Every remote action is one ssh command through ``runner`` (injectable for tests). Job layout on the site:
``<workdir>/<handle>/{job.sh, out/, slurm.out}``; the job script writes its results under ``$<PREFIX>_OUT``. The job id
is kept in a local state dir, so a restarted caller finds its jobs. ``status`` asks ``squeue`` first and ``sacct``
once the job has left the queue; a final state is cached locally. Budget guard: a submit whose estimated charge
(:func:`su_estimate`) exceeds ``max_su_per_job`` is refused before anything is sent.
GPU types (docs/gpu-types.md): with a ``gpu_table``, ``gpu_type="auto"`` moves a GPU job to the first ``auto``
candidate whose GPU and memory limits the request fits (cores capped at the candidate's limit) and leaves any other
request on the type it asked for; an explicit type applies to every GPU job (cores and memory capped at its limits);
None submits every request as asked. The choice, the original request and the reason go into the job script header
and the job record (``gpu_choice``). The table's rates also price the budget estimate when no flat rates are set.
Durable intents and per-handle locks fence each dispatch attempt; an unrecorded intent is reconciled by exact
scheduler comment, never blindly resubmitted. See docs/slurm-recovery.md for storage and single-cluster limits.
"""
from __future__ import annotations

import datetime as dt
import io
import json
import re
import shlex
import subprocess
import tarfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from . import EndpointError, JobRecord, JobStatus, Resources, StateDir, site, spec_sha
from .environment import DEFAULT as DEFAULT_ENVIRONMENT, JobEnvironment
from .runners import Runner, ssh_runner  # noqa: F401  (ssh_runner re-exported for callers)
from .submission import IntentStore, durable_json, handle_key, read_json, request_digest, submission_lock

WORKING = {"PENDING", "CONFIGURING", "RUNNING", "COMPLETING", "REQUEUED", "RESIZING", "SUSPENDED", "STAGE_OUT",
           "SIGNALING", "REQUEUE_HOLD", "REQUEUE_FED", "PREEMPTED"}
FAILED = {"FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE", "REVOKED", "SPECIAL_EXIT"}


AUTO = "auto"
_TABLE_DEFAULT = object()   # gpu_type not given: the table's default


@dataclass(frozen=True)
class GpuType:
    """One GPU type of a site, keyed in :class:`GpuTable` by its gres type name (``--gres=gpu:<name>:<n>``).

    partition: where jobs of this type go (None: the endpoint's GPU partition). su_per_gpu_h / su_per_core_h /
    su_per_gb_h: the charge per GPU-, core- and GB-hour of a job of this type (su_per_gpu_h None: not priced).
    max_gpus / max_cores / max_mem_gb: the most one job of this type may ask for (None: no limit)."""
    partition: Optional[str] = None
    su_per_gpu_h: Optional[float] = None
    su_per_core_h: float = 0.0
    su_per_gb_h: float = 0.0
    max_gpus: Optional[int] = None
    max_cores: Optional[int] = None
    max_mem_gb: Optional[int] = None


@dataclass(frozen=True)
class GpuTable:
    """A site's GPU types and the rule that picks one for a GPU job (supplied by the experiment or site config).

    types: {gres type name: GpuType}. auto: the candidates of ``gpu_type="auto"``, most preferred first; a request
    moves to the first candidate whose max_gpus and max_mem_gb it fits (cores are capped at its max_cores), else it
    keeps its own type. default: the endpoint's gpu_type when none is given ("auto", a type name, or None).
    cpu_su_per_core_h / cpu_su_per_gb_h: charge of CPU-only jobs (cpu_su_per_core_h None: not priced). price_unknown_as:
    the type whose rates price a GPU request of a type not in the table (None: such a request is not priced).
    fit_reason / nofit_reason: format strings of the recorded reason; fields gpus, mem_gb, cores (the request), type,
    max_gpus, max_mem_gb, max_cores (the chosen candidate, or for nofit_reason the most preferred one), candidates."""
    types: Mapping[str, GpuType] = field(default_factory=dict)
    auto: Tuple[str, ...] = ()
    default: Optional[str] = AUTO
    cpu_su_per_core_h: Optional[float] = None
    cpu_su_per_gb_h: float = 0.0
    price_unknown_as: Optional[str] = None
    fit_reason: str = "{gpus} GPU(s), {mem_gb} GB fit {type} (<= {max_mem_gb} GB, cores capped at {max_cores})"
    nofit_reason: str = "{gpus} GPU(s), {mem_gb} GB fit none of {candidates}: the requested type"

    def __post_init__(self):
        object.__setattr__(self, "types", dict(self.types))
        object.__setattr__(self, "auto", tuple(self.auto))
        bad = [t for t in self.auto if t not in self.types]
        if bad:
            raise ValueError(f"auto candidates {bad} are not in the table's types {sorted(self.types)}")
        for name in (self.default, self.price_unknown_as):
            if name not in (None, AUTO) and name not in self.types:
                raise ValueError(f"{name!r} is not in the table's types {sorted(self.types)}")
        if self.price_unknown_as == AUTO:
            raise ValueError("price_unknown_as names a type, not 'auto'")

    def fields(self, r: Resources, name: Optional[str]) -> dict:
        t = self.types.get(name) or GpuType()
        return {"gpus": r.gpus, "mem_gb": r.mem_gb, "cores": r.cores, "type": name, "max_gpus": t.max_gpus,
                "max_mem_gb": t.max_mem_gb, "max_cores": t.max_cores, "candidates": ", ".join(self.auto) or "(none)"}


def _fits(r: Resources, t: GpuType) -> bool:
    return (t.max_gpus is None or r.gpus <= t.max_gpus) and (t.max_mem_gb is None or r.mem_gb <= t.max_mem_gb)


def _cap(value: int, limit: Optional[int]) -> int:
    return value if limit is None else min(value, limit)


def su_estimate(r: Resources, su_per_gpu_h: Optional[float] = None, su_per_core_h: Optional[float] = None,
                gpu_table: Optional[GpuTable] = None) -> float:
    """Estimated charge of one job.

    With flat rates (either given): wall hours x (GPUs x GPU rate + cores x core rate). Example: a site that charges
    53 SU per GPU-hour and 0.5 SU per core-hour charges ``su_estimate(Resources(cores=8, gpus=1, minutes=20), 53.0,
    0.5)`` = 19 SU. Without flat rates, ``gpu_table``'s rates for r's type: wall hours x (GPUs x GPU rate + cores x
    core rate + GB x GB rate), CPU-only jobs at the table's CPU rates. EndpointError when nothing prices r."""
    if su_per_gpu_h is not None or su_per_core_h is not None:
        return r.minutes / 60.0 * (r.gpus * (su_per_gpu_h or 0.0) + r.cores * (su_per_core_h or 0.0))
    if gpu_table is None:
        raise EndpointError("su_estimate needs flat rates or a gpu_table")
    if not r.gpus:
        if gpu_table.cpu_su_per_core_h is None:
            raise EndpointError("the gpu_table has no CPU rates (cpu_su_per_core_h)")
        return r.minutes / 60.0 * (r.cores * gpu_table.cpu_su_per_core_h + r.mem_gb * gpu_table.cpu_su_per_gb_h)
    t = gpu_table.types.get(r.gpu_type) or gpu_table.types.get(gpu_table.price_unknown_as)
    if t is None or t.su_per_gpu_h is None:
        raise EndpointError(f"the gpu_table has no SU rates for gpu_type {r.gpu_type!r}")
    return r.minutes / 60.0 * (r.gpus * t.su_per_gpu_h + r.cores * t.su_per_core_h + r.mem_gb * t.su_per_gb_h)


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
                 environment: Optional[JobEnvironment] = None, gpu_table: Optional[GpuTable] = None,
                 gpu_type=_TABLE_DEFAULT):
        self.name, self._host, self._account = name, host, account
        self._partition_set = bool(partition or site.setting("HEPEP_SLURM_PARTITION"))
        self.partition = partition or site.setting("HEPEP_SLURM_PARTITION", "gpu")
        if gpu_type is _TABLE_DEFAULT:
            gpu_type = gpu_table.default if gpu_table is not None else None
        if gpu_type is not None and gpu_table is None:
            raise EndpointError(f"gpu_type {gpu_type!r} needs a gpu_table")
        if gpu_type not in (None, AUTO) and gpu_type not in gpu_table.types:
            raise EndpointError(f"unknown gpu_type {gpu_type!r}; known: {AUTO!r}, None, {sorted(gpu_table.types)}")
        self.gpu_table, self.gpu_type = gpu_table, gpu_type
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
        if not r.gpus:
            return self.cpu_partition or ""
        t = self.gpu_table.types.get(r.gpu_type) if self.gpu_table is not None else None
        return t.partition if t is not None and t.partition and not self._partition_set else self.partition

    def gpu_choice(self, r: Resources) -> Tuple[Resources, Optional[Dict]]:
        """(the job actually submitted, the choice record) for request r; the record is None when r passes unchanged
        because there is no table, gpu_type is None, or r asks for no GPU."""
        if not r.gpus or self.gpu_type is None or self.gpu_table is None:
            return r, None
        table, requested = self.gpu_table, dict(r.__dict__)
        if self.gpu_type == AUTO:
            name = next((c for c in table.auto if _fits(r, table.types[c])), None)
            if name is not None:
                t = table.types[name]
                out = type(r)(cores=_cap(r.cores, t.max_cores), mem_gb=r.mem_gb, gpus=r.gpus, gpu_type=name,
                              minutes=r.minutes)
                why = table.fit_reason.format(**table.fields(r, name))
            else:
                out = r
                why = table.nofit_reason.format(**table.fields(r, table.auto[0] if table.auto else None))
            return out, {"policy": AUTO, "chosen": out.gpu_type, "requested": requested, "why": why}
        t = table.types[self.gpu_type]
        out = type(r)(cores=_cap(r.cores, t.max_cores), mem_gb=_cap(r.mem_gb, t.max_mem_gb), gpus=r.gpus,
                      gpu_type=self.gpu_type, minutes=r.minutes)
        return out, {"policy": "explicit", "chosen": self.gpu_type, "requested": requested,
                     "why": f"gpu_type={self.gpu_type} set on the endpoint"}

    def shape(self, r: Resources) -> Resources:
        """The job actually submitted for request r (gpu_choice without the record)."""
        return self.gpu_choice(r)[0]

    def header(self, handle: str, r: Resources, choice: Optional[Dict] = None) -> str:
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
        if choice:
            lines.append(f"# {self.env.job_prefix} gpu_choice " + json.dumps(choice, sort_keys=True))
        return "\n".join(lines) + f"\nexport {self.env.job_dir_var}={d} {self.env.out_var}={d}/out\n"

    def body(self, handle: str, script: str, r: Resources, choice: Optional[Dict] = None) -> str:
        """The complete job file: header, output directory, the environment preamble, the caller's script."""
        return (self.header(handle, r, choice) + f"umask 077\nmkdir -p \"${self.env.out_var}\"\n" + self.env.preamble
                + script)

    def admit_resources(self, resources: Resources) -> None:
        """Apply the configured per-job budget without contacting Slurm."""
        if self.max_su_per_job is None:
            return
        if self.su_rates is None and self.gpu_table is None:
            raise EndpointError(f"{self.name}: max_su_per_job is set but no SU rates are configured "
                                "(su_per_gpu_h / su_per_core_h, or a gpu_table)")
        su = su_estimate(self.shape(resources), *(self.su_rates or (None, None)), gpu_table=self.gpu_table)
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

    def _adopt(self, intent, job_id, choice=None):
        rec = JobRecord(intent["handle"], intent["spec_sha"], job_id, intent["remote_dir"],
                        intent["created_at"], intent["resources"], gpu_choice=choice)
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

    def _reconcile(self, intent, choice=None):
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
        return self._adopt(intent, ids.pop(), choice)

    # -- interface -----------------------------------------------------------------------------------------------
    def submit(self, handle: str, script: str, resources: Resources) -> JobRecord:
        with submission_lock(self.state.root, handle):
            if self.gpu_table is not None and self.gpu_type is not None and resources.gpus:
                old, intent = self._load_record(handle), self._intents().get(handle)
                if ((old is not None and old.spec_sha == spec_sha(script, resources))
                        or (intent is not None and intent["resources"] == resources.__dict__
                            and intent["request_sha"] == self._request(handle, script, resources)[2])):
                    return self._submit_locked(handle, script, resources)   # recorded as requested, unshaped
            return self._submit_locked(handle, script, *self.gpu_choice(resources))

    def _request(self, handle, script, resources, choice=None):
        """(job file, target, request digest) of one submission."""
        target = self._target(resources)
        body = self.body(handle, script, resources, choice)
        return body, target, request_digest(body, resources.__dict__, target)

    def _submit_locked(self, handle, script, resources, choice=None):
        sha = spec_sha(script, resources)
        old = self._load_record(handle)
        if old is not None:
            if old.spec_sha != sha:
                raise EndpointError(f"{handle!r} was already submitted with a different script or resources")
            return old
        d = self._dir(handle)
        body, target, request = self._request(handle, script, resources, choice)
        intents = self._intents()
        intent = intents.get(handle)
        if intent is not None:
            if (intent["request_sha"] != request or intent["spec_sha"] != sha or intent["target"] != target
                    or intent["remote_dir"] != d or intent["resources"] != resources.__dict__):
                raise EndpointError(f"{handle!r}: pending submission has a different request or target")
            return self._reconcile(intent, choice)
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
            return self._adopt(intent, job_id, choice)
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
