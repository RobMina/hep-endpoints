"""jobsub endpoint against a fake grid: the jobsub_* and ifdh commands are stand-ins on PATH (tests/fakegrid.py), the
endpoint's commands and the rendered job run in a real local bash. No schedd, token, ssh or dCache is contacted."""
import json
import os
import signal
import subprocess
import sys
import tarfile
import threading
import time
from pathlib import Path

import pytest

from hependpoints import EndpointError, JobRecord, Resources, site
from hependpoints.environment import JobEnvironment
from hependpoints.jobsub import JobsubEndpoint, parse_queue, parse_submit
from hependpoints.runners import local_runner
from hependpoints.submission import IntentStore

FAKE = Path(__file__).with_name("fakegrid.py")
R = Resources(cores=2, mem_gb=4, gpus=0, minutes=30)


class Grid:
    """The fake grid's directories, its command environment, and the worker side (running a submitted job)."""

    def __init__(self, root: Path):
        self.root, self.bin, self.dcache = root / "grid", root / "bin", root / "dcache"
        for d in (self.root, self.bin, self.dcache):
            d.mkdir(parents=True)
        for name in ("jobsub_submit", "jobsub_q", "jobsub_rm", "jobsub_fetchlog", "ifdh"):
            p = self.bin / name
            p.write_text(f"#!/bin/sh\nexec {sys.executable} {FAKE} {name} \"$@\"\n")
            p.chmod(0o755)
        self.env = dict(os.environ, PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}", FAKEGRID_DIR=str(self.root),
                        FAKEGRID_DCACHE=str(self.dcache))
        self.calls = []
        self.failure = None
        self._run = local_runner(self.env)

    def runner(self, cmd, stdin, timeout):
        self.calls.append(cmd[-1])
        if self.failure == "before_acceptance" and "jobsub_submit" in cmd[-1]:
            raise subprocess.TimeoutExpired(cmd, timeout)
        p = self._run(cmd, stdin, timeout)
        if "jobsub_submit" in cmd[-1]:
            if self.failure == "lost_reply":
                raise subprocess.TimeoutExpired(cmd, timeout)
            if self.failure == "error_reply":
                return subprocess.CompletedProcess(cmd, 255, b"", b"connection lost")
            if self.failure == "garbage_reply":
                return subprocess.CompletedProcess(cmd, 0, b"Submitting job(s).\n", b"")
            if self.failure == "double_reply":
                return subprocess.CompletedProcess(cmd, 0, p.stdout + p.stdout.replace(b"1001.0", b"1002.0"), b"")
        return p

    def state(self):
        path = self.root / "state.json"
        return json.loads(path.read_text()) if path.exists() else {"jobs": {}}

    @property
    def submits(self):
        return len(self.state()["jobs"])

    def job(self, jid):
        return self.state()["jobs"][jid]

    def set(self, jid, st):
        data = self.state()
        data["jobs"][jid]["state"] = st
        (self.root / "state.json").write_text(json.dumps(data))

    def worker_env(self, jid, scratch):
        job = self.job(jid)
        cluster, proc = jid.split("@")[0].split(".")
        return dict(self.env, **job["env"], JOBSUBJOBID=jid, CLUSTER=cluster, PROCESS=proc,
                    _CONDOR_SCRATCH_DIR=str(scratch))

    def run(self, jid, tmp_path):
        """Run the job as a worker would, then drop it from the queue."""
        scratch = tmp_path / f"worker-{jid.split('.')[0]}"
        scratch.mkdir()
        self.set(jid, "R")
        p = subprocess.run(["bash", self.job(jid)["script"]], env=self.worker_env(jid, scratch), cwd=scratch,
                           capture_output=True, text=True)
        self.set(jid, "done")
        return p


@pytest.fixture
def grid(tmp_path):
    return Grid(tmp_path)


def make(tmp_path, grid, **kw):
    cfg = dict(group="testexp", workdir=str(tmp_path / "submit"), output_dir=str(grid.dcache / "out"),
               state_dir=tmp_path / "state", runner=grid.runner)
    cfg.update(kw)
    return JobsubEndpoint(**cfg)


SCRIPT = 'echo working; echo \'{"x": 2.5}\' > "$HEPEP_OUT/metrics.json"\n'


def test_parsers():
    assert parse_submit("x\nUse job id 12.0@s.example to retrieve output\n") == ["12.0@s.example"]
    table = ("JOBSUBJOBID OWNER SUBMITTED RUNTIME ST PRIO SIZE COMMAND\n"
             "12.0@s.example u 10/31 12:00 0+00:00:01 R 0 0.0 job.sh\n")
    assert parse_queue(table, "12.0@s.example") == "R"
    assert parse_queue(table, "13.0@s.example") is None
    with pytest.raises(EndpointError, match="uninterpretable"):
        parse_queue("12.0@s.example garbled row\n", "12.0@s.example")


def test_submit_run_status_fetch(grid, tmp_path):
    ep = make(tmp_path, grid, image="/cvmfs/images.example/el9:latest", tarballs=["dropbox:///path/to/code.tar"],
              dropbox="cvmfs", disk="10GB", role="Analysis")
    rec = ep.submit("p1", SCRIPT, Resources(cores=4, mem_gb=8, gpus=1, minutes=45))
    job = grid.job(rec.job_id)
    argv, token = job["argv"], job["env"]["HEPEP_TOKEN"]
    for flag in ("-G", "testexp", "--role=Analysis", "--cpu=4", "--memory=8GB", "--expected-lifetime=45m", "--gpu=1",
                 "--disk=10GB", "--singularity-image", "/cvmfs/images.example/el9:latest", "--use-cvmfs-dropbox",
                 "--tar-file-name=dropbox:///path/to/code.tar", f"--need-storage-modify={grid.dcache}/out",
                 "--resource-provides=usage_model=DEDICATED,OPPORTUNISTIC", "--mail_never",
                 f'--lines=+HepepToken="{token}"', f"file://{tmp_path}/submit/p1/job.sh"):
        assert flag in argv, flag
    assert job["cwd"] == str(tmp_path / "submit" / "p1")                # submitted from the job directory
    assert rec.remote_dir == f"{grid.dcache}/out/p1/{token}" and token.startswith("hepep-v1-")
    assert IntentStore(ep.state.root).get("p1")["token"] == token
    st = ep.status("p1")
    assert (st.state, st.message, st.site_state) == ("working", "idle", "I")
    p = grid.run(rec.job_id, tmp_path)
    assert p.returncode == 0, p.stderr
    assert "working" in p.stdout                                        # job.log tail also reaches the jobsub log
    dest = Path(rec.remote_dir)
    assert (dest / "jobid").read_text().strip() == rec.job_id and (dest / "job.done").read_text() == "exit=0\n"
    assert ep.status("p1").state == "completed"
    n = len(grid.calls)
    assert ep.status("p1").state == "completed" and len(grid.calls) == n   # cached
    files = ep.fetch("p1", tmp_path / "fetched")
    names = {f.relative_to(tmp_path / "fetched" / "p1").as_posix() for f in files}
    assert {"out/metrics.json", "job.log"} <= names and any(n.startswith("logs/") for n in names)
    assert json.loads((tmp_path / "fetched/p1/out/metrics.json").read_text()) == {"x": 2.5}


def test_failing_job_and_fetchlog_failure_keep_results(grid, tmp_path, monkeypatch):
    ep = make(tmp_path, grid)
    rec = ep.submit("bad", 'echo partial > "$HEPEP_OUT/partial.txt"; exit 3\n', R)
    assert grid.run(rec.job_id, tmp_path).returncode == 3
    st = ep.status("bad")
    assert st.state == "failed" and st.message == "exit 3"
    grid.env["FAKEGRID_FETCHLOG_FAIL"] = "1"
    files = ep.fetch("bad", tmp_path / "f")
    assert (tmp_path / "f/bad/out/partial.txt").read_text() == "partial\n"
    assert "transfer failed" in (tmp_path / "f/bad/logs/FETCHLOG_FAILED.txt").read_text()
    assert any(f.name == "partial.txt" for f in files)


def test_terminated_job_still_copies_out(grid, tmp_path):
    ep = make(tmp_path, grid)
    rec = ep.submit("slow", "sleep 30\n", R)
    scratch = tmp_path / "w"
    scratch.mkdir()
    proc = subprocess.Popen(["bash", grid.job(rec.job_id)["script"]], env=grid.worker_env(rec.job_id, scratch),
                            cwd=scratch, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    jobid = Path(rec.remote_dir) / "jobid"
    t0 = time.time()
    while not jobid.exists() and time.time() - t0 < 15:
        time.sleep(0.05)
    time.sleep(0.3)
    proc.send_signal(signal.SIGTERM)
    proc.wait(30)
    grid.set(rec.job_id, "done")
    assert (Path(rec.remote_dir) / "job.done").read_text() == "exit=143\n"
    assert ep.status("slow").state == "failed" and ep.fetch("slow", tmp_path / "f")


def test_idempotent_and_refusals(grid, tmp_path):
    ep = make(tmp_path, grid, max_minutes=120, gpu_types={"a100": '(TARGET.GPUType =?= "A100")'})
    a = ep.submit("p", SCRIPT, R)
    assert ep.submit("p", SCRIPT, R) == a and grid.submits == 1
    with pytest.raises(EndpointError, match="different script"):
        ep.submit("p", "echo other\n", R)
    n = len(grid.calls)
    with pytest.raises(EndpointError, match="no condor requirement"):
        ep.submit("g", SCRIPT, Resources(gpus=1, gpu_type="h100"))
    with pytest.raises(EndpointError, match="exceeds max_minutes"):
        ep.submit("long", SCRIPT, Resources(minutes=600))
    with pytest.raises(EndpointError, match="bad handle"):
        ep.submit("../evil", SCRIPT, R)
    assert len(grid.calls) == n and grid.submits == 1
    rec = ep.submit("g", SCRIPT, Resources(gpus=1, gpu_type="a100"))
    assert '--append_condor_requirements=(TARGET.GPUType =?= "A100")' in grid.job(rec.job_id)["argv"]


@pytest.mark.parametrize("failure", ["lost_reply", "error_reply", "garbage_reply"])
def test_accepted_submission_with_lost_reply_is_reconciled_not_resubmitted(grid, tmp_path, failure):
    ep = make(tmp_path, grid)
    grid.failure = failure
    with pytest.raises(EndpointError, match="uncertain.*intent retained"):
        ep.submit("p", SCRIPT, R)
    grid.failure = None
    assert grid.submits == 1 and ep.state.get("p") is None
    jid = next(iter(grid.state()["jobs"]))
    for _ in range(2):                               # not started yet: no jobid report, never a second submit
        with pytest.raises(EndpointError, match="unresolved submission"):
            make(tmp_path, grid).submit("p", SCRIPT, R)
    assert grid.submits == 1
    grid.run(jid, tmp_path)                          # the job starts (and here finishes), reporting its id
    rec = make(tmp_path, grid).submit("p", SCRIPT, R)
    assert rec.job_id == jid and grid.submits == 1
    assert make(tmp_path, grid).status("p").state == "completed"


def test_never_accepted_submission_stays_unresolved(grid, tmp_path):
    ep = make(tmp_path, grid)
    grid.failure = "before_acceptance"
    with pytest.raises(EndpointError, match="uncertain"):
        ep.submit("p", SCRIPT, R)
    grid.failure = None
    for _ in range(3):
        with pytest.raises(EndpointError, match="unresolved"):
            ep.submit("p", SCRIPT, R)
    assert grid.submits == 0 and IntentStore(ep.state.root).get("p")


def test_multiple_ids_are_quarantined(grid, tmp_path):
    ep = make(tmp_path, grid)
    grid.failure = "double_reply"
    with pytest.raises(EndpointError, match="uncertain"):
        ep.submit("p", SCRIPT, R)
    grid.failure = None
    assert "," in IntentStore(ep.state.root).get("p")["unsupported_ack"]
    n = len(grid.calls)
    with pytest.raises(EndpointError, match="operator investigation"):
        ep.submit("p", SCRIPT, R)
    assert len(grid.calls) == n and grid.submits == 1


@pytest.mark.parametrize("change", ["script", "resources", "group", "output_dir", "image", "host"])
def test_pending_request_or_target_change_is_refused_without_remote_calls(grid, tmp_path, change):
    grid.failure = "lost_reply"
    with pytest.raises(EndpointError):
        make(tmp_path, grid).submit("p", SCRIPT, R)
    grid.failure = None
    n = len(grid.calls)
    kw = {"group": dict(group="other"), "output_dir": dict(output_dir=str(grid.dcache / "elsewhere")),
          "image": dict(image="/cvmfs/other.example/img"), "host": dict(host="other-host.example")}.get(change, {})
    with pytest.raises(EndpointError, match="different request or target"):
        make(tmp_path, grid, **kw).submit("p", "echo x\n" if change == "script" else SCRIPT,
                                          Resources(minutes=31) if change == "resources" else R)
    assert len(grid.calls) == n


def test_lifecycle_calls_refuse_a_changed_target(grid, tmp_path):
    make(tmp_path, grid).submit("p", SCRIPT, R)
    other = make(tmp_path, grid, group="other")
    n = len(grid.calls)
    for call in (lambda: other.status("p"), lambda: other.cancel("p"), lambda: other.fetch("p", tmp_path / "o")):
        with pytest.raises(EndpointError, match="target mismatch"):
            call()
    assert len(grid.calls) == n


def test_corrupt_record_and_intent_fail_closed(grid, tmp_path):
    ep = make(tmp_path, grid)
    rec = ep.submit("p", SCRIPT, R)
    path = ep.state._path("p")
    data = json.loads(path.read_text())
    data["job_id"] = rec.job_id + "; touch SENTINEL"
    path.write_text(json.dumps(data))
    n = len(grid.calls)
    with pytest.raises(EndpointError, match="invalid job record"):
        ep.cancel("p")
    assert len(grid.calls) == n
    ep.state.put(JobRecord("q", rec.spec_sha, rec.job_id, rec.remote_dir, rec.submitted_at, rec.resources))
    with pytest.raises(EndpointError, match="without its submission intent"):
        ep.status("q")
    grid.failure = "lost_reply"
    with pytest.raises(EndpointError):
        ep.submit("r", SCRIPT, R)
    IntentStore(ep.state.root)._path("r").write_text('{"version": 1}')
    with pytest.raises(EndpointError, match="corrupt submission intent"):
        ep.submit("r", SCRIPT, R)


def test_cancel_queued_job_and_held_jobs(grid, tmp_path):
    ep = make(tmp_path, grid)
    rec = ep.submit("c", SCRIPT, R)
    assert ep.cancel("c") == "cancelled" and grid.job(rec.job_id)["removed"]
    assert ep.status("c").state == "cancelled" and ep.cancel("c") == "cancelled"
    h = ep.submit("h", SCRIPT, R)
    grid.set(h.job_id, "H")
    st = ep.status("h")
    assert st.state == "working" and st.message.startswith("held")
    strict = make(tmp_path / "strict", grid, held_is_failed=True)
    s = strict.submit("s", SCRIPT, R)
    grid.set(s.job_id, "H")
    assert strict.status("s").state == "failed"
    assert strict.cancel("s") == "failed" and grid.job(s.job_id)["removed"]   # still removed from the queue


def test_removed_without_record_and_absent_grace(grid, tmp_path):
    clock = [1000.0]
    ep = make(tmp_path, grid, clock=lambda: clock[0], absent_grace_s=600)
    x = ep.submit("x", SCRIPT, R)
    grid.set(x.job_id, "X")
    assert ep.status("x").state == "failed" and "removed" in ep.status("x").message
    v = ep.submit("v", SCRIPT, R)
    grid.set(v.job_id, "done")                     # left the queue, no exit record (yet)
    st = ep.status("v")
    assert st.state == "working" and "waiting for its exit record" in st.message
    clock[0] += 601
    st = ep.status("v")
    assert st.state == "failed" and "no exit record" in st.message


def test_query_failure_is_not_absence(grid, tmp_path):
    ep = make(tmp_path, grid)
    ep.submit("p", SCRIPT, R)
    (grid.bin / "jobsub_q").write_text("#!/bin/sh\necho 'schedd unreachable' >&2\nexit 1\n")
    with pytest.raises(EndpointError, match="schedd unreachable"):
        ep.status("p")
    assert ep.state.get("p").final is None


def test_concurrent_submits_of_one_handle_dispatch_once(grid, tmp_path):
    results, errors = [], []

    def go(script):
        try:
            results.append(make(tmp_path, grid).submit("p", script, R).job_id)
        except EndpointError as e:
            errors.append(e)
    threads = [threading.Thread(target=go, args=(SCRIPT,)) for _ in range(3)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert grid.submits == 1 and not errors and len(set(results)) == 1


def test_environment_hooks_and_valid_bash(grid, tmp_path):
    env = JobEnvironment(var_prefix="MYEXP", job_prefix="myexp", preamble="export MYEXP_SETUP=1")
    ep = make(tmp_path, grid, environment=env, worker_setup="export FROM_SETUP=1")
    body = ep.body("p", 'echo "$MYEXP_SETUP $FROM_SETUP" > "$MYEXP_OUT/env.txt"\n', R)
    assert subprocess.run(["bash", "-n"], input=body.encode()).returncode == 0
    assert "HEPEP" not in body and body.index("FROM_SETUP=1") < body.index("MYEXP_SETUP=1")
    rec = ep.submit("p", 'echo "$MYEXP_SETUP $FROM_SETUP" > "$MYEXP_OUT/env.txt"\n', R)
    assert grid.job(rec.job_id)["env"]["MYEXP_TOKEN"].startswith("myexp-v1-")
    grid.run(rec.job_id, tmp_path)
    ep.fetch("p", tmp_path / "f")
    assert (tmp_path / "f/p/out/env.txt").read_text() == "1 1\n"
    with tarfile.open(Path(rec.remote_dir) / "result.tar") as t:
        assert {"out", "job.log"} <= {m.name.split("/")[0] for m in t.getmembers()}


def test_job_without_token_refuses_to_run(grid, tmp_path):
    ep = make(tmp_path, grid)
    rec = ep.submit("p", SCRIPT, R)
    env = grid.worker_env(rec.job_id, tmp_path)
    env.pop("HEPEP_TOKEN")
    p = subprocess.run(["bash", grid.job(rec.job_id)["script"]], env=env, capture_output=True, text=True)
    assert p.returncode != 0 and "no submission token" in p.stderr


def test_settings_from_environment(tmp_path, monkeypatch):
    with pytest.raises(site.SiteConfigError, match="HEPEP_JOBSUB_GROUP"):
        JobsubEndpoint(state_dir=tmp_path)
    for name, value in (("HEPEP_JOBSUB_GROUP", "testexp"), ("HEPEP_JOBSUB_WORKDIR", "/path/to/submit/"),
                        ("HEPEP_JOBSUB_OUTPUT_DIR", "/path/to/dcache/out"), ("HEPEP_JOBSUB_HOST", "gpvm.example"),
                        ("HEPEP_JOBSUB_ROLE", "Analysis"), ("HEPEP_JOBSUB_IMAGE", "/cvmfs/img.example/el9"),
                        ("HEPEP_JOBSUB_TARBALL", "dropbox:///path/to/a.tar,/cvmfs/rcds.example/sw/abc"),
                        ("HEPEP_JOBSUB_DROPBOX", "pnfs"), ("HEPEP_JOBSUB_DISK", "5GB"),
                        ("HEPEP_JOBSUB_MAX_MINUTES", "600"), ("HEPEP_JOBSUB_STORAGE_MODIFY", "/testexp/scratch/x")):
        monkeypatch.setenv(name, value)
    ep = JobsubEndpoint()
    assert (ep.group, ep.workdir, ep.output_dir, ep.host, ep.role) == (
        "testexp", "/path/to/submit", "/path/to/dcache/out", "gpvm.example", "Analysis")
    assert ep.tarballs == ["dropbox:///path/to/a.tar", "/cvmfs/rcds.example/sw/abc"] and ep.max_minutes == 600
    argv = ep.submit_argv("h", "hepep-v1-" + "0" * 32, R)
    assert "--use-pnfs-dropbox" in argv and "--disk=5GB" in argv and "--need-storage-modify=/testexp/scratch/x" in argv
    assert ep.state.root == site.state_dir("jobsub")
    monkeypatch.setenv("HEPEP_JOBSUB_DROPBOX", "ftp")
    with pytest.raises(EndpointError, match="dropbox"):
        JobsubEndpoint()
