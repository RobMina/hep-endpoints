"""Slurm-over-SSH adapter against a fake Slurm site (no ssh, no cluster)."""
import io
import json
import shutil
import subprocess
import tarfile

import pytest

from hependpoints import EndpointError, Resources, site
from hependpoints.environment import JobEnvironment
from hependpoints.slurm_ssh import SlurmSSHEndpoint, map_state, su_estimate

GPU = Resources(cores=8, mem_gb=48, gpus=1, gpu_type="a100", minutes=30)
SITE = dict(host="slurm-host.example", account="test_account", workdir="/path/to/scratch/jobs",
            su_per_gpu_h=53.0, su_per_core_h=0.5)


class FakeSite:
    """Answers the shell snippets SlurmSSHEndpoint sends, from an in-memory queue."""

    def __init__(self):
        self.next_id, self.queue, self.acct, self.scripts, self.calls = 1000, {}, {}, {}, []

    def __call__(self, cmd, stdin, timeout):
        s = cmd[-1]
        self.calls.append(s)
        out = b""
        if "sbatch --parsable" in s:
            self.next_id += 1
            jid = str(self.next_id)
            self.queue[jid] = "PENDING"
            self.scripts[jid] = stdin.decode()
            out = f"{jid}\n".encode()
        elif s.startswith("squeue"):
            jid = s.split("-j ")[1].split()[0]
            out = f"{self.queue[jid]}|None\n".encode() if jid in self.queue else b""
        elif s.startswith("sacct"):
            jid = s.split("-j ")[1].split()[0]
            out = (self.acct[jid] + "\n").encode() if jid in self.acct else b""
        elif s.startswith("scancel"):
            jid = s.split()[1]
            self.queue.pop(jid, None)
            self.acct[jid] = "CANCELLED by 1|0:0"
        elif "tar -cf" in s:
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as t:
                for name, data in (("out/metrics.json", json.dumps({"events_per_s": 20020.0}).encode()),
                                   ("slurm.out", b"done\n")):
                    ti = tarfile.TarInfo(name)
                    ti.size = len(data)
                    t.addfile(ti, io.BytesIO(data))
            out = buf.getvalue()
        return subprocess.CompletedProcess(cmd, 0, out, b"")

    def finish(self, jid, state="COMPLETED", code="0:0"):
        self.queue.pop(jid, None)
        self.acct[jid] = f"{state}|{code}"


@pytest.fixture
def site_ep(tmp_path):
    fake = FakeSite()
    ep = SlurmSSHEndpoint(runner=fake, state_dir=tmp_path / "jobs", max_su_per_job=100, cpu_partition="standard",
                          **SITE)
    return fake, ep


def test_map_state():
    assert map_state("PENDING") == "working"
    assert map_state("COMPLETED", "0:0") == "completed"
    assert map_state("COMPLETED", "3:0") == "failed"
    assert map_state("CANCELLED by 123") == "cancelled"
    assert map_state("TIMEOUT") == "failed"
    with pytest.raises(EndpointError):
        map_state("WEIRD")


def test_su_estimate_with_site_rates():
    # Measured on a site charging ~53 SU per GPU-hour and 0.5 SU per core-hour: 19 SU and 25 SU jobs.
    assert su_estimate(Resources(cores=8, gpus=1, minutes=20), 53.0, 0.5) == pytest.approx(19, abs=2)
    assert su_estimate(Resources(cores=16, gpus=1, minutes=24), 53.0, 0.5) == pytest.approx(25, abs=2)
    assert su_estimate(Resources(cores=4, gpus=0, minutes=60), 0.0, 1.0) == 4.0


def test_submit_status_fetch(site_ep, tmp_path):
    fake, ep = site_ep
    rec = ep.submit("p1.bench", "echo hi\n", GPU)
    script = fake.scripts[rec.job_id]
    assert "#SBATCH -A test_account" in script and "#SBATCH --gres=gpu:a100:1" in script and script.endswith("echo hi\n")
    assert "#SBATCH -J hepep_p1.bench" in script and "#SBATCH -p gpu" in script
    assert "export HEPEP_JOB_DIR=/path/to/scratch/jobs/p1.bench HEPEP_OUT=/path/to/scratch/jobs/p1.bench/out" in script
    assert "--comment=hepep-v1-" in fake.calls[0]
    assert ep.status("p1.bench").state == "working"
    fake.finish(rec.job_id)
    st = ep.status("p1.bench")
    assert st.state == "completed" and st.job_id == rec.job_id
    n = len(fake.calls)
    assert ep.status("p1.bench").state == "completed" and len(fake.calls) == n   # final state cached, no ssh
    files = ep.fetch("p1.bench", tmp_path / "f")
    assert {p.name for p in files} == {"metrics.json", "slurm.out"}


def test_submit_idempotent_and_refusals(site_ep):
    fake, ep = site_ep
    a = ep.submit("p2.bench", "x\n", GPU)
    assert ep.submit("p2.bench", "x\n", GPU).job_id == a.job_id and len(fake.scripts) == 1
    with pytest.raises(EndpointError, match="different"):
        ep.submit("p2.bench", "y\n", GPU)
    with pytest.raises(EndpointError, match="exceeds max_su_per_job"):
        ep.submit("big", "x\n", Resources(gpus=8, minutes=600))
    with pytest.raises(EndpointError, match="bad handle"):
        ep.submit("../evil", "x\n", Resources(minutes=5))


def test_cancel_and_failure(site_ep):
    fake, ep = site_ep
    ep.submit("p3", "x\n", Resources(minutes=10))
    assert ep.cancel("p3") == "cancelled" and ep.status("p3").state == "cancelled"
    b = ep.submit("p4", "x\n", Resources(minutes=10))
    fake.finish(b.job_id, "OUT_OF_MEMORY", "0:125")
    assert ep.status("p4").state == "failed"


def test_cpu_jobs_use_the_cpu_partition_or_the_site_default(site_ep, tmp_path):
    fake, ep = site_ep
    rec = ep.submit("cpu", "x\n", Resources(cores=2, minutes=10))
    script = fake.scripts[rec.job_id]
    assert "#SBATCH -p standard" in script and "--gres" not in script
    plain = SlurmSSHEndpoint(runner=fake, state_dir=tmp_path / "plain", **SITE)
    rec = plain.submit("cpu", "x\n", Resources(cores=2, minutes=10))
    assert "#SBATCH -p" not in fake.scripts[rec.job_id]


def test_environment_prefixes_and_preamble(tmp_path):
    fake = FakeSite()
    env = JobEnvironment(var_prefix="MYEXP", job_prefix="myexp", preamble="source /path/to/setup.sh")
    ep = SlurmSSHEndpoint(runner=fake, state_dir=tmp_path, environment=env, **SITE)
    rec = ep.submit("p", "echo run\n", GPU)
    script = fake.scripts[rec.job_id]
    assert "#SBATCH -J myexp_p" in script and "export MYEXP_JOB_DIR=" in script and 'mkdir -p "$MYEXP_OUT"' in script
    assert script.index("source /path/to/setup.sh\n") < script.index("echo run") and "HEPEP" not in script
    assert "--comment=myexp-v1-" in fake.calls[0]


def test_budget_needs_rates_and_is_optional(tmp_path):
    fake = FakeSite()
    no_cap = SlurmSSHEndpoint(runner=fake, state_dir=tmp_path / "a", host="h", account="a", workdir="/path/to/w")
    no_cap.admit_resources(Resources(gpus=8, minutes=6000))                # no cap configured: admitted
    cap_only = SlurmSSHEndpoint(runner=fake, state_dir=tmp_path / "b", host="h", account="a", workdir="/path/to/w",
                                max_su_per_job=10)
    with pytest.raises(EndpointError, match="no SU rates"):
        cap_only.submit("p", "x\n", Resources(minutes=1))
    assert fake.calls == []


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
def test_rendered_body_is_valid_bash(site_ep):
    _, ep = site_ep
    r = subprocess.run(["bash", "-n"], input=ep.body("t", "echo ok\n", GPU).encode(), capture_output=True)
    assert r.returncode == 0, r.stderr.decode()


def test_settings_come_from_the_environment_and_fail_closed(tmp_path, monkeypatch):
    with pytest.raises(site.SiteConfigError, match="HEPEP_SLURM_WORKDIR"):
        SlurmSSHEndpoint(state_dir=tmp_path)
    ep = SlurmSSHEndpoint(state_dir=tmp_path, workdir="/path/to/w")
    for attr, name in (("host", "HEPEP_SLURM_HOST"), ("account", "HEPEP_SLURM_ACCOUNT")):
        with pytest.raises(site.SiteConfigError, match=name):
            getattr(ep, attr)
    for name, value in (("HEPEP_SLURM_HOST", "site-host"), ("HEPEP_SLURM_ACCOUNT", "site-account"),
                        ("HEPEP_SLURM_WORKDIR", "/path/to/scratch/w/"), ("HEPEP_SLURM_PARTITION", "gpu-a"),
                        ("HEPEP_SLURM_CPU_PARTITION", "cpu-a"), ("HEPEP_SLURM_MAX_SU_PER_JOB", "200"),
                        ("HEPEP_SLURM_SU_PER_GPU_HOUR", "50"), ("HEPEP_SLURM_SU_PER_CORE_HOUR", "1")):
        monkeypatch.setenv(name, value)
    ep = SlurmSSHEndpoint()
    assert (ep.host, ep.account, ep.workdir) == ("site-host", "site-account", "/path/to/scratch/w")
    assert (ep.partition, ep.cpu_partition, ep.max_su_per_job, ep.su_rates) == ("gpu-a", "cpu-a", 200.0, (50.0, 1.0))
    assert ep.state.root == site.state_dir("slurm_ssh")
    ep.host, ep.account = "other-host", "other-account"
    assert (ep.host, ep.account) == ("other-host", "other-account")
    assert "#SBATCH -A other-account" in ep.header("h", Resources())
    monkeypatch.setenv("HEPEP_SLURM_MAX_SU_PER_JOB", "lots")
    with pytest.raises(site.SiteConfigError, match="not a number"):
        SlurmSSHEndpoint()


def test_subclass_reads_its_own_variables(tmp_path, monkeypatch):
    class Mine(SlurmSSHEndpoint):
        HOST_VARS = ("MYEXP_SLURM_HOST", "HEPEP_SLURM_HOST")
        CONFIG_HINT = "see the myexp site.env.example"
    ep = Mine(state_dir=tmp_path, workdir="/path/to/w")
    with pytest.raises(site.SiteConfigError, match="MYEXP_SLURM_HOST or HEPEP_SLURM_HOST .see the myexp"):
        ep.host
    monkeypatch.setenv("MYEXP_SLURM_HOST", "mine")
    assert ep.host == "mine"
