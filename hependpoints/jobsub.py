"""FNAL grid through jobsub_lite: submit / status / fetch / cancel with ``jobsub_submit``, ``jobsub_q``,
``jobsub_fetchlog`` and ``jobsub_rm``, run on a host where those commands work (an FNAL interactive node, either this
machine or one reached by ``ssh``) with a valid grid token.

Settings come from arguments or the environment, never from the package:

  group           $HEPEP_JOBSUB_GROUP       experiment / VO group, ``-G`` (required)
  workdir         $HEPEP_JOBSUB_WORKDIR     directory on the submit host for job scripts and the submit CWD
                                            (required; a data or scratch area, not a synced code tree)
  output_dir      $HEPEP_JOBSUB_OUTPUT_DIR  dCache scratch directory the jobs copy their results to (required)
  host            $HEPEP_JOBSUB_HOST        ssh host of the submit node; unset = run the commands on this host
  role            $HEPEP_JOBSUB_ROLE        ``--role`` (e.g. Analysis); unset = jobsub's default
  storage_modify  $HEPEP_JOBSUB_STORAGE_MODIFY   ``--need-storage-modify`` path (default: output_dir; jobsub_lite
                                            rewrites a dCache mount path to the logical token path)
  image           $HEPEP_JOBSUB_IMAGE       Apptainer/Singularity image for the job (``image_option``, default
                                            ``--singularity-image``); unset = none
  tarballs        $HEPEP_JOBSUB_TARBALL     ``--tar-file-name`` values (``dropbox://<file>`` or a published RCDS
                                            directory); unset = none
  dropbox         $HEPEP_JOBSUB_DROPBOX     ``cvmfs`` (``--use-cvmfs-dropbox``) or ``pnfs`` (``--use-pnfs-dropbox``)
  disk            $HEPEP_JOBSUB_DISK        ``--disk`` (e.g. 10GB); unset = jobsub's default
  usage_model     $HEPEP_JOBSUB_USAGE_MODEL ``--resource-provides=usage_model=`` (default DEDICATED,OPPORTUNISTIC)
  max_minutes     $HEPEP_JOBSUB_MAX_MINUTES admission cap on the requested lifetime; unset = no cap
  copy_command, mkdir_command, remove_command   dCache file commands, on the worker and the submit host
                                            (default ``ifdh cp`` / ``ifdh mkdir_p`` / ``ifdh rm``)
  worker_setup    shell text run first on the worker (e.g. making ifdh available); default none
  gpu_types       {gpu_type: condor requirement} for Resources.gpu_type; any other nonempty gpu_type is refused
  extra_args      further jobsub_submit arguments (part of the endpoint target identity)
  state_dir       $HEPEP_STATE_DIR/<name>   local job records (default ~/.hependpoints/jobsub)
  environment     JobEnvironment            variable prefix, job prefix, preamble of every job

Job layout. The job file goes to ``<workdir>/<handle>/job.sh`` on the submit host and is submitted from that
directory as ``file://.../job.sh``. On the worker it runs in ``$_CONDOR_SCRATCH_DIR`` with ``$<PREFIX>_OUT`` as its
output directory, and copies to ``<output_dir>/<handle>/<token>/``: ``jobid`` when it starts (its
``$JOBSUBJOBID``), then ``result.tar`` (``out/`` and ``job.log``) and last ``job.done`` (``exit=<rc>``) when it ends,
on any exit including SIGTERM. The token is the submission intent's random token, passed with ``-e``, so a result
can never be confused with another attempt's. Files on dCache are read by known path with ``copy_command`` (never a
listing or a stat of the dCache mount).

State handling mirrors slurm_ssh: a durable intent and a per-handle lock fence each ``jobsub_submit``; an attempt
whose acknowledgement was lost is reconciled from the ``jobid`` file the job writes when it starts, never
resubmitted. ``status`` asks ``jobsub_q`` first; once the job has left the queue, ``job.done`` gives the exit code;
a job gone from the queue with no ``job.done`` after ``absent_grace_s`` is ``failed``. A final state is cached.
See docs/jobsub.md, including what has not been exercised against a real schedd.
"""
from __future__ import annotations

import io
import re
import shlex
import subprocess
import tarfile
import time
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional, Sequence

from . import EndpointError, JobRecord, JobStatus, Resources, StateDir, site, spec_sha
from .environment import DEFAULT as DEFAULT_ENVIRONMENT, JobEnvironment
from .runners import Runner, local_runner, ssh_runner
from .submission import (IntentStore, durable_json, handle_key, new_token, read_json, request_digest,
                         submission_lock)

JOB_ID = re.compile(r"[1-9][0-9]*\.[0-9]+@[A-Za-z0-9][A-Za-z0-9.-]*")
ACK = re.compile(r"Use job id (\S+) to retrieve output")
# condor job states as jobsub_q prints them (ST column)
QUEUE_WORKING = {"I": "idle", "R": "running", "S": "suspended", "<": "transferring input",
                 ">": "transferring output"}
ABSENT = "__HEPEP_ABSENT__"
PRESENT = "__HEPEP_PRESENT__"


def parse_submit(text: str) -> List[str]:
    """Job ids in jobsub_submit's output ("Use job id <id> to retrieve output")."""
    return ACK.findall(text)


def parse_queue(text: str, job_id: str) -> Optional[str]:
    """The condor state letter (ST column) of `job_id` in jobsub_q's table, or None when it is not listed.

    jobsub_q prints ``JOBSUBJOBID OWNER SUBMITTED RUNTIME ST PRIO SIZE COMMAND``; the row is found by its first
    column and the state is its first single-character field after it."""
    for line in text.splitlines():
        fields = line.split()
        if fields and fields[0] == job_id:
            for f in fields[1:]:
                if len(f) == 1 and f in "IRHCXS<>":
                    return f
            raise EndpointError(f"uninterpretable jobsub_q row for {job_id}: {line.strip()[:200]!r}")
    return None


def _split(value: Optional[str]) -> List[str]:
    return [v for v in (value or "").split(",") if v]


class JobsubEndpoint:
    GROUP_VARS = ("HEPEP_JOBSUB_GROUP",)
    WORKDIR_VARS = ("HEPEP_JOBSUB_WORKDIR",)
    OUTPUT_VARS = ("HEPEP_JOBSUB_OUTPUT_DIR",)
    CONFIG_HINT = site.DEFAULT_HINT

    def __init__(self, group: str | None = None, workdir: str | None = None, output_dir: str | None = None,
                 host: str | None = None, role: str | None = None, storage_modify: str | None = None,
                 image: str | None = None, image_option: str = "--singularity-image",
                 tarballs: Sequence[str] | None = None, dropbox: str | None = None, disk: str | None = None,
                 usage_model: str | None = None, max_minutes: float | None = None,
                 copy_command: str = "ifdh cp", mkdir_command: str = "ifdh mkdir_p", remove_command: str = "ifdh rm",
                 worker_setup: str = "", gpu_types: Mapping[str, str] | None = None,
                 extra_args: Sequence[str] = (), state_dir: str | Path | None = None, runner: Optional[Runner] = None,
                 name: str = "jobsub", environment: Optional[JobEnvironment] = None, timeout_s: float = 300.0,
                 absent_grace_s: float = 900.0, held_is_failed: bool = False, fetch_logs: bool = True,
                 clock: Callable[[], float] = time.time):
        self.name = name
        self.group = group or site.require(*self.GROUP_VARS, hint=self.CONFIG_HINT)
        self.workdir = (workdir or site.require(*self.WORKDIR_VARS, hint=self.CONFIG_HINT)).rstrip("/")
        self.output_dir = (output_dir or site.require(*self.OUTPUT_VARS, hint=self.CONFIG_HINT)).rstrip("/")
        for label, path in (("workdir", self.workdir), ("output_dir", self.output_dir)):
            if not path.startswith("/"):
                raise EndpointError(f"{name}: {label} must be an absolute path")
        self.host = host if host is not None else (site.setting("HEPEP_JOBSUB_HOST") or "")
        self.role = role or site.setting("HEPEP_JOBSUB_ROLE") or ""
        self.storage_modify = storage_modify or site.setting("HEPEP_JOBSUB_STORAGE_MODIFY") or self.output_dir
        self.image = image or site.setting("HEPEP_JOBSUB_IMAGE") or ""
        self.image_option = image_option
        self.tarballs = list(tarballs) if tarballs is not None else _split(site.setting("HEPEP_JOBSUB_TARBALL"))
        self.dropbox = dropbox or site.setting("HEPEP_JOBSUB_DROPBOX") or ""
        if self.dropbox not in ("", "cvmfs", "pnfs"):
            raise EndpointError(f"{name}: dropbox must be cvmfs or pnfs, not {self.dropbox!r}")
        self.disk = disk or site.setting("HEPEP_JOBSUB_DISK") or ""
        self.usage_model = usage_model or site.setting("HEPEP_JOBSUB_USAGE_MODEL", "DEDICATED,OPPORTUNISTIC")
        self.max_minutes = max_minutes if max_minutes is not None else site.number("HEPEP_JOBSUB_MAX_MINUTES")
        self.copy_command, self.mkdir_command, self.remove_command = copy_command, mkdir_command, remove_command
        self.worker_setup = worker_setup if not worker_setup or worker_setup.endswith("\n") else worker_setup + "\n"
        self.gpu_types = dict(gpu_types or {})
        self.extra_args = [str(a) for a in extra_args]
        self.env = environment or DEFAULT_ENVIRONMENT
        self.state = StateDir(Path(state_dir).expanduser() if state_dir is not None else site.state_dir(name))
        self.run = runner or (ssh_runner(self.host) if self.host else local_runner())
        self.timeout_s, self.absent_grace_s, self.held_is_failed = timeout_s, absent_grace_s, held_is_failed
        self.fetch_logs, self.clock = fetch_logs, clock

    # -- helpers -------------------------------------------------------------------------------------------------
    def _sh(self, script: str, stdin: Optional[bytes] = None, timeout: Optional[float] = None) -> str:
        return self._raw(script, stdin, timeout).decode(errors="replace")

    def _raw(self, script: str, stdin: Optional[bytes] = None, timeout: Optional[float] = None) -> bytes:
        where = f"on {self.host}" if self.host else "locally"
        try:
            p = self.run(["bash", "-c", script], stdin, timeout or self.timeout_s)
        except subprocess.TimeoutExpired as e:
            raise EndpointError(f"{self.name}: command {where} timed out after {e.timeout} s")
        if p.returncode != 0:
            err = (p.stderr or b"").decode(errors="replace").strip()[-400:]
            raise EndpointError(f"{self.name}: command {where} failed (rc {p.returncode}): {err}")
        return p.stdout or b""

    def _group_args(self) -> List[str]:
        return ["-G", self.group] + ([f"--role={self.role}"] if self.role else [])

    def _jobdir(self, handle: str) -> str:
        return f"{self.workdir}/{handle}"

    def _dest(self, handle: str, token: str) -> str:
        return f"{self.output_dir}/{handle}/{token}"

    def _target(self, resources: Resources) -> Dict:
        return {"host": self.host, "group": self.group, "role": self.role, "workdir": self.workdir,
                "output_dir": self.output_dir, "storage_modify": self.storage_modify, "image": self.image,
                "image_option": self.image_option, "tarballs": list(self.tarballs), "dropbox": self.dropbox,
                "disk": self.disk, "usage_model": self.usage_model, "extra_args": list(self.extra_args),
                "gpu_requirement": self.gpu_types.get(resources.gpu_type, "") if resources.gpu_type else ""}

    def _intents(self) -> IntentStore:
        return IntentStore(self.state.root, self.env.job_prefix)

    def body(self, handle: str, script: str, r: Resources) -> str:
        """The job file the worker runs: setup, start marker, the caller's script, and copy-out on any exit."""
        P = self.env.var_prefix
        out_base = shlex.quote(f"{self.output_dir}/{handle}")
        cp, mk, rm = self.copy_command, self.mkdir_command, self.remove_command
        return (
            "#!/bin/bash\n"
            f"# hep-endpoints jobsub job {handle} (group {self.group})\n"
            f": \"${{{self.env.token_var}:?no submission token in the job environment}}\"\n"
            f"export {P}_JOB_DIR=\"${{_CONDOR_SCRATCH_DIR:-$PWD}}/hepep_job\"\n"
            f"export {P}_OUT=\"${P}_JOB_DIR/out\"\n"
            f"hepep_dest={out_base}/\"${self.env.token_var}\"\n"
            "umask 077\n"
            f"mkdir -p \"${P}_OUT\" || exit 90\n"
            + self.worker_setup +
            f"hepep_put() {{ {rm} \"$2\" >/dev/null 2>&1; {cp} \"$1\" \"$2\"; }}\n"
            f"{mk} \"$hepep_dest\" || exit 91\n"
            f"printf '%s\\n' \"${{JOBSUBJOBID:-${{CLUSTER:-0}}.${{PROCESS:-0}}}}\" > \"${P}_JOB_DIR/jobid\"\n"
            f"hepep_put \"${P}_JOB_DIR/jobid\" \"$hepep_dest/jobid\" || exit 92\n"
            "hepep_finish() {\n"
            "  rc=$?\n"
            "  trap - EXIT TERM\n"
            "  exec 1>&3 2>&4\n"
            f"  tail -n 200 \"${P}_JOB_DIR/job.log\"\n"
            f"  tar -C \"${P}_JOB_DIR\" -cf \"${P}_JOB_DIR/result.tar\" out job.log || {{ [ \"$rc\" = 0 ] && rc=93; }}\n"
            f"  hepep_put \"${P}_JOB_DIR/result.tar\" \"$hepep_dest/result.tar\" || exit 94\n"
            f"  echo \"exit=$rc\" > \"${P}_JOB_DIR/job.done\"\n"
            f"  hepep_put \"${P}_JOB_DIR/job.done\" \"$hepep_dest/job.done\" || exit 95\n"
            "  exit \"$rc\"\n"
            "}\n"
            f"exec 3>&1 4>&2 > \"${P}_JOB_DIR/job.log\" 2>&1\n"
            "trap hepep_finish EXIT\n"
            "trap 'exit 143' TERM\n"
            + self.env.preamble + script + ("" if script.endswith("\n") else "\n"))

    def submit_argv(self, handle: str, token: str, r: Resources) -> List[str]:
        """The jobsub_submit command line for one job (run in <workdir>/<handle>)."""
        argv = ["jobsub_submit", *self._group_args(), f"--resource-provides=usage_model={self.usage_model}",
                f"--cpu={r.cores}", f"--memory={r.mem_gb}GB", f"--expected-lifetime={r.minutes}m"]
        if self.disk:
            argv.append(f"--disk={self.disk}")
        if r.gpus:
            argv.append(f"--gpu={r.gpus}")
            if r.gpu_type:
                argv.append(f"--append_condor_requirements={self.gpu_types[r.gpu_type]}")
        if self.image:
            argv += [self.image_option, self.image]
        if self.dropbox:
            argv.append(f"--use-{self.dropbox}-dropbox")
        argv += [f"--tar-file-name={t}" for t in self.tarballs]
        argv += [f"--need-storage-modify={self.storage_modify}", f"--lines=+HepepToken=\"{token}\"",
                 "-e", f"{self.env.token_var}={token}", "--mail_never", *self.extra_args,
                 f"file://{self._jobdir(handle)}/job.sh"]
        return argv

    def admit_resources(self, r: Resources) -> None:
        """Deterministic checks before anything is sent."""
        if r.cores < 1 or r.mem_gb < 1 or r.minutes < 1 or r.gpus < 0:
            raise EndpointError(f"{self.name}: cores, mem_gb and minutes must be positive, gpus nonnegative")
        if r.gpu_type and r.gpus and r.gpu_type not in self.gpu_types:
            raise EndpointError(f"{self.name}: gpu_type {r.gpu_type!r} has no condor requirement configured "
                                f"(gpu_types); known: {sorted(self.gpu_types)}")
        if self.max_minutes is not None and r.minutes > self.max_minutes:
            raise EndpointError(f"{self.name}: lifetime {r.minutes} min exceeds max_minutes {self.max_minutes:.0f}")

    def _read_output(self, path: str) -> Optional[bytes]:
        """A file on dCache copied to a temporary file on the submit host and printed; None when the copy fails
        (absent, or not readable now)."""
        script = (f"t=$(mktemp -d) || exit 1; if {self.copy_command} {shlex.quote(path)} \"$t/f\" >/dev/null 2>&1; "
                  f"then printf '{PRESENT}\\n'; cat \"$t/f\"; else printf '{ABSENT}\\n'; fi; rm -rf \"$t\"")
        raw = self._raw(script, timeout=600)
        head, _, rest = raw.partition(b"\n")
        if head == PRESENT.encode():
            return rest
        if head == ABSENT.encode():
            return None
        raise EndpointError(f"{self.name}: unexpected reply reading {path}")

    # -- records -------------------------------------------------------------------------------------------------
    def _load_record(self, handle):
        handle_key(handle)
        path = self.state._path(handle)
        data = read_json(path)
        if data is None and not path.exists():
            return None
        try:
            rec = JobRecord(**data)
            if (rec.handle != handle or not JOB_ID.fullmatch(rec.job_id)
                    or not re.fullmatch(r"[0-9a-f]{16}", rec.spec_sha) or not isinstance(rec.remote_dir, str)
                    or not isinstance(rec.resources, dict)):
                raise ValueError("invalid job record identity")
            intent = self._intents().get(handle)
            if intent is None:
                raise ValueError("job record without its submission intent")
            resources = Resources(**rec.resources)
            if (intent["target"] != self._target(resources) or intent["spec_sha"] != rec.spec_sha
                    or intent["resources"] != rec.resources or intent["remote_dir"] != rec.remote_dir
                    or rec.remote_dir != self._dest(handle, intent["token"])
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

    def _sidecar(self, kind: str, handle: str) -> Path:
        d = self.state.root / f".{kind}"
        d.mkdir(mode=0o700, exist_ok=True)
        return d / f"{handle_key(handle)}.json"

    def _reconcile(self, intent):
        if intent.get("unsupported_ack"):
            raise EndpointError(f"{intent['handle']!r}: unsupported jobsub_submit acknowledgement "
                                f"{intent['unsupported_ack']!r}; operator investigation required; intent retained, "
                                "no resubmission")
        raw = self._read_output(intent["remote_dir"] + "/jobid")
        if raw is None:
            raise EndpointError(f"{intent['handle']!r}: unresolved submission (no job has reported token "
                                f"{intent['token']} yet: it may be idle, held, or never submitted); retry the same "
                                "request to reconcile only; check jobsub_q for the HepepToken classad before deciding "
                                "anything else; do not delete the intent")
        ids = [line.strip() for line in raw.decode(errors="replace").splitlines() if line.strip()]
        if len(ids) != 1 or not JOB_ID.fullmatch(ids[0]):
            raise EndpointError(f"{intent['handle']!r}: unsupported job id report {ids!r} for token "
                                f"{intent['token']}; operator investigation required; intent retained")
        return self._adopt(intent, ids[0])

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
        body = self.body(handle, script, resources)
        request = request_digest(body, resources.__dict__, target)
        intents = self._intents()
        intent = intents.get(handle)
        if intent is not None:
            if (intent["request_sha"] != request or intent["spec_sha"] != sha or intent["target"] != target
                    or intent["remote_dir"] != self._dest(handle, intent["token"])
                    or intent["resources"] != resources.__dict__):
                raise EndpointError(f"{handle!r}: pending submission has a different request or target")
            return self._reconcile(intent)
        self.admit_resources(resources)
        token = new_token(self.env.job_prefix)
        dest = self._dest(handle, token)
        intent = intents.create(handle, sha, request, target, resources.__dict__.copy(), dest, time.time(),
                                token=token)
        jd = shlex.quote(self._jobdir(handle))
        argv = " ".join(shlex.quote(a) for a in self.submit_argv(handle, token, resources))
        try:
            out = self._sh(f"set -e; mkdir -p {jd}; cat > {jd}/job.sh; chmod 0700 {jd}/job.sh; cd {jd}; {argv}",
                           stdin=body.encode())
            ids = parse_submit(out)
            if len(ids) != 1 or not JOB_ID.fullmatch(ids[0]):
                if ids:
                    intent["unsupported_ack"] = ",".join(ids)[:200]
                    durable_json(intents._path(handle), intent)
                raise EndpointError(f"no single job id in the jobsub_submit output: {out[-300:]!r}")
            return self._adopt(intent, ids[0])
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
            return self._status_locked(handle)[0]

    def _finish(self, rec: JobRecord, js: JobStatus) -> JobStatus:
        rec.final = js.__dict__.copy()
        self._save_record(rec)
        return js

    def _status_locked(self, handle):
        """(status, still in the queue)."""
        rec = self._record(handle)
        if rec.final:
            return JobStatus(**rec.final), False
        q = self._sh(" ".join(shlex.quote(a) for a in ["jobsub_q", *self._group_args(), "--jobid", rec.job_id]))
        st = parse_queue(q, rec.job_id)
        cancelled = read_json(self._sidecar("cancelled", handle)) is not None
        if st in QUEUE_WORKING:
            return JobStatus("working", QUEUE_WORKING[st], st, rec.job_id), True
        if st == "H":
            if self.held_is_failed:
                return self._finish(rec, JobStatus("failed", "held (still in the queue; jobsub_rm to remove)",
                                                   st, rec.job_id)), True
            return JobStatus("working", "held (a release policy may re-queue it; cancel to give up)", st,
                             rec.job_id), True
        done = self._read_output(rec.remote_dir + "/job.done")
        if done is not None:
            code = done.decode(errors="replace").strip().split("=")[-1]
            state = "completed" if code == "0" else ("cancelled" if cancelled else "failed")
            return self._finish(rec, JobStatus(state, f"exit {code}", st or "", rec.job_id)), False
        if cancelled:
            return self._finish(rec, JobStatus("cancelled", "cancelled (no exit record)", st or "",
                                               rec.job_id)), False
        if st == "X":
            return self._finish(rec, JobStatus("failed", "removed from the queue with no exit record", st,
                                               rec.job_id)), False
        path = self._sidecar("absent", handle)
        seen = read_json(path)
        if seen is None:
            seen = {"since": self.clock()}
            durable_json(path, seen)
        waited = self.clock() - seen["since"]
        if waited > self.absent_grace_s:
            return self._finish(rec, JobStatus("failed", f"left the queue with no exit record ({waited:.0f} s)",
                                               st or "", rec.job_id)), False
        return JobStatus("working", "left the queue; waiting for its exit record", st or "", rec.job_id), False

    def fetch(self, handle: str, dest: Path) -> List[Path]:
        """Unpack <output_dir>/<handle>/<token>/result.tar (out/ and job.log) into dest/<handle>/, and the jobsub
        logs (jobsub_fetchlog) into dest/<handle>/logs/ (best effort); returns the local files."""
        rec = self._record(handle)
        target = Path(dest) / handle
        target.mkdir(parents=True, exist_ok=True)
        if self.fetch_logs:
            self._fetch_logs(rec, target / "logs")
        raw = self._read_output(rec.remote_dir + "/result.tar")
        if raw is None:
            raise EndpointError(f"{self.name}: {handle!r} has no result.tar in {rec.remote_dir} yet")
        with tarfile.open(fileobj=io.BytesIO(raw)) as t:
            t.extractall(target, filter="data")
        return sorted(p for p in target.rglob("*") if p.is_file())

    def _fetch_logs(self, rec: JobRecord, logs: Path) -> None:
        cmd = " ".join(shlex.quote(a) for a in ["jobsub_fetchlog", *self._group_args(), "--jobid", rec.job_id])
        try:
            raw = self._raw(f"t=$(mktemp -d) || exit 1; {cmd} --destdir \"$t\" >&2 && tar -C \"$t\" -cf - .; "
                            "rc=$?; rm -rf \"$t\"; exit $rc", timeout=600)
            logs.mkdir(parents=True, exist_ok=True)
            with tarfile.open(fileobj=io.BytesIO(raw)) as t:
                t.extractall(logs, filter="data")
        except (EndpointError, tarfile.TarError) as exc:
            logs.mkdir(parents=True, exist_ok=True)
            (logs / "FETCHLOG_FAILED.txt").write_text(str(exc)[:2000] + "\n")

    def cancel(self, handle: str) -> str:
        with submission_lock(self.state.root, handle):
            rec = self._record(handle)
            st, queued = self._status_locked(handle)
            if st.state != "working" and st.site_state == "H":     # reported failed while held: still queued?
                queued = parse_queue(self._sh(" ".join(shlex.quote(a) for a in [
                    "jobsub_q", *self._group_args(), "--jobid", rec.job_id])), rec.job_id) is not None
            if st.state == "working":
                durable_json(self._sidecar("cancelled", handle), {"at": self.clock()})
            elif not (queued and st.site_state == "H"):      # finished: nothing to remove
                return st.state
            if queued:                                       # a held job reported failed is still removed
                self._sh(" ".join(shlex.quote(a) for a in ["jobsub_rm", *self._group_args(), "--jobid", rec.job_id]))
            return "cancelled" if st.state == "working" else st.state
