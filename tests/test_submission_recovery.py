"""Fault injection for Slurm's one-attempt fence; no scheduler or SSH is contacted."""
import json
import multiprocessing
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict

import pytest

from hependpoints import EndpointError, JobRecord, Resources
from hependpoints.slurm_ssh import SlurmSSHEndpoint
from hependpoints.submission import IntentStore, durable_json, submission_lock

R = Resources(cores=8, mem_gb=48, gpus=1, gpu_type="a100", minutes=30)
WORKDIR = "/path/to/scratch/jobs"
SITE = dict(host="slurm-host.example", account="test_account", workdir=WORKDIR, max_su_per_job=500,
            su_per_gpu_h=53.0, su_per_core_h=0.5)


class Scheduler:
    def __init__(self):
        self.calls, self.jobs, self.submits = [], {}, 0
        self.failure = None
        self.queue_text = self.accounting_text = None
        self.queue_error = self.accounting_error = False
        self.state_root = None

    def __call__(self, cmd, stdin, timeout):
        shell = cmd[-1]
        self.calls.append(shell)
        if "sbatch --parsable" in shell:
            self.submits += 1
            token = re.search(r"--comment=(hepep-v1-[0-9a-f]{32})", shell)[1]
            if self.state_root is not None:
                assert IntentStore(self.state_root).get("point")["token"] == token
            if self.failure != "before_acceptance":
                self.jobs[str(1000 + self.submits)] = token
            if self.failure in ("lost_reply", "before_acceptance"):
                raise subprocess.TimeoutExpired(cmd, timeout)
            if self.failure == "error_reply":
                return subprocess.CompletedProcess(cmd, 255, b"", b"connection lost")
            if self.failure == "empty_reply":
                output = ""
            elif self.failure == "cluster_reply":
                output = "1001;other_cluster\n"
            elif self.failure == "extra_reply":
                output = "noise\n1001\n"
            else:
                output = str(1000 + self.submits) + "\n"
        elif "squeue --local" in shell:
            assert shell.startswith("unset SLURM_CLUSTERS;")
            if self.queue_error:
                return subprocess.CompletedProcess(cmd, 1, b"", b"query failed")
            output = self.queue_text if self.queue_text is not None else self.rows()
        elif "sacct --local" in shell:
            assert shell.startswith("unset SLURM_CLUSTERS;")
            if self.accounting_error:
                return subprocess.CompletedProcess(cmd, 1, b"", b"accounting unavailable")
            assert "--duplicates" in shell and "--starttime=" in shell and "Comment%128" in shell
            assert "--user=" in shell and "|| true" not in shell
            output = self.accounting_text if self.accounting_text is not None else self.rows()
        else:
            raise AssertionError(f"unexpected command {shell}")
        return subprocess.CompletedProcess(cmd, 0, output.encode(), b"")

    def rows(self):
        return "".join(f"{jid}|{token}\n" for jid, token in self.jobs.items())


def endpoint(tmp_path, site, **kwargs):
    return SlurmSSHEndpoint(state_dir=tmp_path, runner=site, **{**SITE, **kwargs})


def uncertain(tmp_path, failure="lost_reply"):
    site = Scheduler()
    site.failure = failure
    ep = endpoint(tmp_path, site)
    site.state_root = ep.state.root
    with pytest.raises(EndpointError, match="uncertain.*intent retained"):
        ep.submit("point", "echo safe\n", R)
    assert site.submits == 1
    assert ep.state.get("point") is None
    assert IntentStore(ep.state.root).get("point")
    site.failure = None
    return site, ep


@pytest.mark.parametrize("failure", ["lost_reply", "error_reply", "empty_reply", "extra_reply"])
def test_acceptance_then_lost_or_bad_reply_is_adopted(tmp_path, failure):
    site, ep = uncertain(tmp_path, failure)
    rec = endpoint(tmp_path, site).submit("point", "echo safe\n", R)
    assert rec.job_id == "1001" and site.submits == 1
    assert ep.state.get("point") == rec
    count = len(site.calls)
    assert endpoint(tmp_path, site).submit("point", "echo safe\n", R) == rec
    assert len(site.calls) == count


def test_intent_precedes_dispatch_and_is_retained_after_success(tmp_path):
    site = Scheduler()
    ep = endpoint(tmp_path, site)
    site.state_root = tmp_path
    rec = ep.submit("point", "echo safe\n", R)
    intent = IntentStore(tmp_path).get("point")
    assert intent["remote_dir"] == rec.remote_dir
    assert intent["created_at"] == rec.submitted_at
    assert len(intent["request_sha"]) == 64
    assert intent["token"] in site.calls[0]


def test_record_write_failure_recovers_without_dispatch(tmp_path, monkeypatch):
    site = Scheduler()
    ep = endpoint(tmp_path, site)
    monkeypatch.setattr(ep, "_save_record", lambda rec: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(EndpointError, match="uncertain"):
        ep.submit("point", "echo safe\n", R)
    assert endpoint(tmp_path, site).submit("point", "echo safe\n", R).job_id == "1001"
    assert site.submits == 1


def test_intent_write_failure_never_dispatches(tmp_path, monkeypatch):
    import hependpoints.submission as module
    site = Scheduler()
    ep = endpoint(tmp_path, site)
    monkeypatch.setattr(module, "durable_json", lambda *args: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        ep.submit("point", "echo safe\n", R)
    assert site.calls == []


@pytest.mark.parametrize("failure", ["before_acceptance", "lost_reply"])
def test_empty_queries_are_not_permission_to_resubmit(tmp_path, failure):
    site, ep = uncertain(tmp_path, failure)
    site.queue_text = site.accounting_text = ""
    for _ in range(2):
        with pytest.raises(EndpointError, match="no visible matching allocation"):
            endpoint(tmp_path, site).submit("point", "echo safe\n", R)
    assert site.submits == 1
    assert IntentStore(tmp_path).get("point")


def test_accounting_visibility_delay_then_terminal_job(tmp_path):
    site, ep = uncertain(tmp_path)
    site.queue_text = site.accounting_text = ""
    with pytest.raises(EndpointError, match="unresolved"):
        ep.submit("point", "echo safe\n", R)
    site.accounting_text = None
    rec = ep.submit("point", "echo safe\n", R)
    assert rec.job_id == "1001" and site.submits == 1


@pytest.mark.parametrize("field", ["queue_error", "accounting_error"])
def test_recovery_query_failure_never_resubmits(tmp_path, field):
    site, ep = uncertain(tmp_path)
    setattr(site, field, True)
    with pytest.raises(EndpointError, match="remote command failed"):
        ep.submit("point", "echo safe\n", R)
    assert site.submits == 1 and ep.state.get("point") is None


@pytest.mark.parametrize("output", ["unparseable", "warning\n1001|token"])
def test_malformed_query_is_not_absence(tmp_path, output):
    site, ep = uncertain(tmp_path)
    site.accounting_text = output
    with pytest.raises(EndpointError, match="uninterpretable"):
        ep.submit("point", "echo safe\n", R)
    assert site.submits == 1


def test_exact_tokens_deduplication_and_ambiguous_matches(tmp_path):
    site, ep = uncertain(tmp_path)
    token = site.jobs["1001"]
    site.queue_text = f"42|unrelated|comment\n999|{token}suffix\n1001|   {token}   \n"
    site.accounting_text = f"1001|{token}\n1002|{token}\n"
    with pytest.raises(EndpointError, match="multiple matching allocations"):
        ep.submit("point", "echo safe\n", R)
    site.accounting_text = f"1001|{token}\n"
    assert ep.submit("point", "echo safe\n", R).job_id == "1001"
    assert site.submits == 1


@pytest.mark.parametrize("job_id", ["1001.batch", "1001_2", "1001+0", "0", "-1"])
def test_unsupported_matching_identity_is_refused(tmp_path, job_id):
    site, ep = uncertain(tmp_path)
    site.queue_text = f"{job_id}|{site.jobs['1001']}\n"
    with pytest.raises(EndpointError, match="unsupported matching allocation"):
        ep.submit("point", "echo safe\n", R)
    assert site.submits == 1


@pytest.mark.parametrize("change", ["script", "resources", "host", "account", "partition", "workdir"])
def test_pending_spec_and_target_changes_fail_without_remote_calls(tmp_path, change):
    site, ep = uncertain(tmp_path)
    count = len(site.calls)
    kwargs = {change: "changed"} if change not in ("script", "resources") else {}
    retry = endpoint(tmp_path, site, **kwargs)
    script = "different" if change == "script" else "echo safe\n"
    resources = Resources(cores=8, mem_gb=48, gpus=1, gpu_type="a100", minutes=31) if change == "resources" else R
    with pytest.raises(EndpointError, match="different request or target"):
        retry.submit("point", script, resources)
    assert len(site.calls) == count and site.submits == 1


def test_recorded_legacy_jobs_need_no_remote_queries_or_new_budget(tmp_path):
    from hependpoints import spec_sha
    site = Scheduler()
    ep = endpoint(tmp_path, site)
    rec = JobRecord("point", spec_sha("echo safe\n", R), "999", "/legacy/dir", 1, asdict(R))
    ep.state.put(rec)
    assert endpoint(tmp_path, site, max_su_per_job=0).submit("point", "echo safe\n", R) == rec
    assert site.calls == []


def test_recorded_jobs_with_intents_reject_changed_target_on_lifecycle_calls(tmp_path):
    site = Scheduler()
    ep = endpoint(tmp_path, site)
    ep.submit("point", "echo safe\n", R)
    count = len(site.calls)
    other = endpoint(tmp_path, site, host="another-cluster")
    for call in (lambda: other.submit("point", "echo safe\n", R), lambda: other.status("point"),
                 lambda: other.cancel("point"), lambda: other.fetch("point", tmp_path / "out")):
        with pytest.raises(EndpointError, match="target mismatch"):
            call()
    assert len(site.calls) == count


def test_corrupt_intent_fails_closed(tmp_path):
    site, ep = uncertain(tmp_path)
    path = IntentStore(tmp_path)._path("point")
    path.write_text('{"version":1}')
    count = len(site.calls)
    with pytest.raises(EndpointError, match="corrupt submission intent"):
        ep.submit("point", "echo safe\n", R)
    assert len(site.calls) == count


@pytest.mark.parametrize("conflict", [False, True])
def test_concurrent_instances_serialize_same_handle(tmp_path, conflict):
    site = Scheduler()
    def submit(script):
        try:
            return endpoint(tmp_path, site).submit("point", script, R).job_id
        except EndpointError:
            return "refused"
    with ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(submit, script) for script in ("echo safe\n", "different" if conflict else "echo safe\n")]
        results = [f.result() for f in futures]
    assert site.submits == 1
    assert sorted(results) == (["1001", "refused"] if conflict else ["1001", "1001"])


def _process_submit(root, counter, barrier, outputs):
    def run(cmd, stdin, timeout):
        with counter.get_lock():
            counter.value += 1
        time.sleep(0.05)
        return subprocess.CompletedProcess(cmd, 0, b"1001\n", b"")
    barrier.wait()
    rec = SlurmSSHEndpoint(state_dir=root, runner=run, **SITE).submit("point", "echo safe\n", R)
    outputs.put(rec.job_id)


def test_processes_share_one_submission(tmp_path):
    context = multiprocessing.get_context("spawn")
    counter, barrier, outputs = context.Value("i", 0), context.Barrier(2), context.Queue()
    processes = [context.Process(target=_process_submit, args=(tmp_path, counter, barrier, outputs)) for _ in range(2)]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(15)
            assert process.exitcode == 0
        assert counter.value == 1
        assert [outputs.get(timeout=2), outputs.get(timeout=2)] == ["1001", "1001"]
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(5)


def test_durable_json_syncs_file_before_replace_and_directory_after(tmp_path, monkeypatch):
    import hependpoints.submission as module
    events = []
    sync, replace = module.os.fsync, module.os.replace
    def record_sync(fd):
        events.append("sync")
        return sync(fd)
    def record_replace(src, dest):
        events.append("replace")
        return replace(src, dest)
    monkeypatch.setattr(module.os, "fsync", record_sync)
    monkeypatch.setattr(module.os, "replace", record_replace)
    durable_json(tmp_path / "state.json", {"ok": True})
    assert events == ["sync", "replace", "sync"]
    assert json.loads((tmp_path / "state.json").read_text()) == {"ok": True}
    assert list(tmp_path.glob(".state.json.*")) == []


def test_lock_namespaces_do_not_collide_with_handles(tmp_path):
    site = Scheduler()
    ep = endpoint(tmp_path, site)
    for handle in ("point", "point.lock", "point.intent"):
        ep.submit(handle, "echo safe\n", R)
    assert site.submits == 3
    assert len(list((tmp_path / ".locks").glob("*.lock"))) == 3
    with submission_lock(tmp_path, "point"):
        pass
    assert len(list((tmp_path / ".locks").glob("*.lock"))) == 3


def test_nonlocal_acknowledgement_is_quarantined(tmp_path):
    site, ep = uncertain(tmp_path, "cluster_reply")
    assert IntentStore(tmp_path).get("point")["unsupported_ack"] == "1001;other_cluster"
    count = len(site.calls)
    with pytest.raises(EndpointError, match="operator investigation"):
        ep.submit("point", "echo safe\n", R)
    assert len(site.calls) == count and site.submits == 1


def test_crash_after_intent_before_runner_never_dispatches_on_retry(tmp_path):
    class Crash(BaseException):
        pass
    ep = SlurmSSHEndpoint(state_dir=tmp_path, runner=lambda *args: (_ for _ in ()).throw(Crash()), **SITE)
    with pytest.raises(Crash):
        ep.submit("point", "echo safe\n", R)
    assert IntentStore(tmp_path).get("point")
    site = Scheduler()
    with pytest.raises(EndpointError, match="unresolved"):
        endpoint(tmp_path, site).submit("point", "echo safe\n", R)
    assert site.submits == 0


def _hold_lock(root, ready):
    with submission_lock(root, "point"):
        ready.set()
        time.sleep(60)


def test_process_death_releases_lock_without_losing_intent(tmp_path):
    site, ep = uncertain(tmp_path)
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    process = context.Process(target=_hold_lock, args=(tmp_path, ready))
    process.start()
    try:
        assert ready.wait(10)
    finally:
        process.terminate()
        process.join(10)
    assert not process.is_alive()
    assert endpoint(tmp_path, site).submit("point", "echo safe\n", R).job_id == "1001"
    assert site.submits == 1


@pytest.mark.parametrize("sidecar", ["record", "intent"])
def test_dangling_state_symlink_fails_closed(tmp_path, sidecar):
    site = Scheduler()
    ep = endpoint(tmp_path, site)
    path = ep.state._path("point") if sidecar == "record" else IntentStore(tmp_path)._path("point")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(tmp_path / "absent")
    with pytest.raises(EndpointError, match="expected a regular file"):
        ep.submit("point", "echo safe\n", R)
    assert site.calls == []


def test_corrupt_record_id_cannot_enter_scheduler_commands(tmp_path):
    site = Scheduler()
    ep = endpoint(tmp_path, site)
    rec = ep.submit("point", "echo safe\n", R)
    data = asdict(rec)
    data["job_id"] = "1001; sentinel"
    ep.state._path("point").write_text(json.dumps(data))
    count = len(site.calls)
    with pytest.raises(EndpointError, match="invalid job record"):
        ep.cancel("point")
    assert len(site.calls) == count


def test_nested_state_root_ancestors_synced_before_dispatch(tmp_path, monkeypatch):
    import hependpoints.submission as module
    synced = []
    sync = module._sync_dir
    def record(path):
        synced.append(path)
        sync(path)
    monkeypatch.setattr(module, "_sync_dir", record)
    root = tmp_path / "new" / "parent" / "state"
    site = Scheduler()
    def runner(*args):
        assert root.parent in synced and root.parent.parent in synced and tmp_path in synced
        return site(*args)
    SlurmSSHEndpoint(state_dir=root, runner=runner, **SITE).submit("point", "echo safe\n", R)
    assert site.submits == 1
