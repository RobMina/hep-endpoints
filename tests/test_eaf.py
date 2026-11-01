"""EAF endpoint against a fake JupyterHub; the launch line runs for real in a local bash (no EAF, no network)."""
import os
import shutil
import subprocess
import sys
import time

import pytest

from hependpoints import EndpointError, JobRecord, Resources, site
from hependpoints.eaf import EAFEndpoint, main, slice_of
from hependpoints.environment import JobEnvironment


class FakeHub:
    """Server life cycle in memory; `send` runs the typed line in a local bash, as the EAF terminal would.
    available: slices that start; others never become ready."""

    def __init__(self, available=("2g.20gb",), run=True):
        self.available, self.run = set(available), run
        self.srv, self.starts, self.stops, self.sent, self.pings = None, [], 0, [], 0

    def server(self):
        return self.srv

    def start(self, options):
        self.starts.append(slice_of(options))
        ok = slice_of(options) in self.available
        self.srv = {"ready": ok, "pending": None if ok else "spawn", "url": "/user/u/", "user_options": options}

    def stop(self):
        self.stops += 1
        self.srv = None

    def user_name(self):
        return "u"

    def call(self, method, path, body=None):
        if path.endswith("/activity"):
            self.pings += 1
        return 200, None

    def send(self, line):
        self.sent.append(line)
        if self.run:
            subprocess.run(["bash", "-c", line], check=True)


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def make(tmp_path, hub, clock=None, **kw):
    clock = clock or Clock()
    return EAFEndpoint(workdir=str(tmp_path / "work"), state_dir=tmp_path / "jobs", hub_client=hub, clock=clock,
                       sleep=clock.sleep, start_timeout_s=60, poll_s=10, shared_prefix=str(tmp_path), **kw), clock


@pytest.fixture
def setsid_on_path(tmp_path_factory, monkeypatch):
    """The launch line needs util-linux setsid; where it is missing (macOS) put a small stand-in on PATH that starts
    a new session and execs its arguments, which is all the launch line relies on."""
    if shutil.which("setsid") is None:
        d = tmp_path_factory.mktemp("bin")
        shim = d / "setsid"
        shim.write_text(f"#!{sys.executable}\nimport os, sys\nos.setsid()\nos.execvp(sys.argv[1], sys.argv[1:])\n")
        shim.chmod(0o755)
        monkeypatch.setenv("PATH", f"{d}{os.pathsep}{os.environ['PATH']}")


needs_setsid = pytest.mark.usefixtures("setsid_on_path")


def wait_pid(ep, h, timeout=10):
    t0 = time.time()
    while not (ep._dir(h) / "job.pid").exists() and time.time() - t0 < timeout:
        time.sleep(0.1)


def wait_done(ep, h, timeout=20):
    t0 = time.time()
    while (ep._dir(h) / "job.done").exists() is False and time.time() - t0 < timeout:
        time.sleep(0.1)


def test_slice_fallback_and_no_capacity(tmp_path):
    hub = FakeHub(available=("1g.10gb",), run=False)
    ep, _ = make(tmp_path, hub)
    ep.submit("p1", "true\n", Resources(minutes=5))
    assert hub.starts == ["4g.40gb", "2g.20gb", "1g.10gb"] and hub.stops == 2      # the two stuck spawns cancelled
    assert ep.state.get("p1").resources["slice"] == "1g.10gb"
    hub2 = FakeHub(available=(), run=False)
    ep2, _ = make(tmp_path / "b", hub2)
    with pytest.raises(EndpointError, match="no capacity"):
        ep2.submit("p2", "true\n", Resources(minutes=5))


@needs_setsid
def test_run_complete_fetch_and_idempotent(tmp_path):
    hub = FakeHub()
    ep, clock = make(tmp_path, hub)
    script = 'echo hello; echo \'{"x": 1.5}\' > "$HEPEP_OUT/metrics.json"\n'
    rec = ep.submit("p1", script, Resources(minutes=5))
    wait_done(ep, "p1")
    assert ep.status("p1").state == "completed"
    files = ep.fetch("p1", tmp_path / "fetched")
    assert any(p.name == "metrics.json" for p in files) and "hello" in (tmp_path / "fetched/p1/job.log").read_text()
    assert ep.submit("p1", script, Resources(minutes=5)).spec_sha == rec.spec_sha                     # same job back
    with pytest.raises(EndpointError, match="different"):
        ep.submit("p1", "echo other\n", Resources(minutes=5))


@needs_setsid
def test_failure_one_job_at_a_time_and_cancel(tmp_path):
    hub = FakeHub()
    ep, clock = make(tmp_path, hub)
    ep.submit("bad", "exit 3\n", Resources(minutes=5))
    wait_done(ep, "bad")
    assert ep.status("bad").state == "failed" and "exit 3" in ep.status("bad").message
    ep.submit("long", "sleep 30\n", Resources(minutes=5))
    wait_pid(ep, "long")
    ep.submit("other", "true\n", Resources(minutes=5))                 # slice busy: queued, not refused
    st = ep.status("other")
    assert st.state == "working" and st.message.startswith("queued")
    assert ep.cancel("long") == "cancelled"
    wait_done(ep, "long")
    assert ep.status("long").state == "cancelled"                      # this call also launches "other"
    wait_done(ep, "other")
    assert ep.status("other").state == "completed"
    assert ep.stop_idle() and hub.srv is None                          # nothing runs or waits: the server is stopped
    assert not ep.stop_idle()                                          # and not stopped twice


@needs_setsid
def test_wall_time_watchdog(tmp_path):
    hub = FakeHub()
    ep, _ = make(tmp_path, hub)
    line = ep.launch_line(ep._dir("x"), Resources(minutes=1))
    assert "sleep 60" in line and "kill -TERM -- -$$" in line and "timedout" in line
    # the watchdog for real, with a 2 s wall time: the job is killed, marked timed out and reported failed
    d = ep._dir("slow")
    (d / "out").mkdir(parents=True)
    (d / "job.sh").write_text("sleep 30\n")
    subprocess.run(["bash", "-c", line.replace("sleep 60", "sleep 2").replace(str(ep._dir("x")), str(d))], check=True)
    ep.state.put(JobRecord("slow", "sha", "", str(d), ep.clock(), {"minutes": 1, "slice": "2g.20gb"}))
    wait_done(ep, "slow", timeout=15)
    st = ep.status("slow")
    assert st.state == "failed" and "timed out" in st.message


def test_status_without_exit_record(tmp_path):
    hub = FakeHub(run=False)                       # the typed line never runs: no pid, no done
    ep, clock = make(tmp_path, hub)
    ep.submit("lost", "true\n", Resources(minutes=5))
    assert ep.status("lost").state == "working"
    clock.t += 200
    assert ep.status("lost").state == "failed" and "never started" in ep.status("lost").message


def test_workdir_must_be_shared(tmp_path):
    with pytest.raises(EndpointError, match="shared"):
        EAFEndpoint(workdir="/elsewhere/x", shared_prefix=str(tmp_path), state_dir=tmp_path, hub_client=FakeHub())


def test_queue_order_keepalive_and_linger_stop(tmp_path):
    hub = FakeHub(available=("4g.40gb",), run=False)   # first slice starts; jobs never run: files written by hand
    ep, clock = make(tmp_path, hub, linger_s=120)
    for h in ("a", "b", "c"):
        ep.submit(h, f"echo {h}\n", Resources(minutes=5))
        clock.t += 1
    assert [r.handle for r in ep._queue()] == ["b", "c"]
    assert ep.status("c").message == "queued (1 ahead)"
    (ep._dir("a") / "job.pid").write_text("1")
    assert ep.status("a").state == "working" and hub.pings == 1       # keepalive posted while working
    (ep._dir("a") / "job.done").write_text("exit=0\n")
    assert ep.status("a").state == "completed"                        # and "b" is launched in the same call
    assert ep.state.get("b").resources.get("launched_at") and not ep.state.get("c").resources.get("launched_at")
    assert ep.cancel("c") == "cancelled" and ep.status("c").state == "cancelled"
    (ep._dir("b") / "job.pid").write_text("2"); (ep._dir("b") / "job.done").write_text("exit=0\n")
    assert ep.status("b").state == "completed"
    assert hub.srv is not None                                        # within linger: the server stays
    clock.t += 121
    ep.after_results("b")
    assert hub.srv is None and hub.stops == 1                         # past linger, nothing queued: stopped


def test_server_lifetime_cap_and_cull_report(tmp_path):
    hub = FakeHub(run=False)
    ep, clock = make(tmp_path, hub, max_age_s=3600)
    with pytest.raises(EndpointError, match="lifetime"):
        ep.submit("long", "true\n", Resources(minutes=90))
    ep.submit("j", "true\n", Resources(minutes=5))
    (ep._dir("j") / "job.pid").write_text("1"); (ep._dir("j") / "job.alive").write_text(str(int(clock.t)))
    clock.t += 300
    hub.srv = None                                                    # the hub culled the server
    st = ep.status("j")
    assert st.state == "failed" and "server stopped" in st.message and "last heartbeat 300 s ago" in st.message


def test_concurrent_submits_start_one_server(tmp_path):
    """Callers submit independent steps from threads: one server start, the second job queued."""
    import threading
    hub = FakeHub(available=("4g.40gb",), run=False)
    lock = threading.Lock()
    orig = hub.start

    def start(options):                        # a second start while one exists is the hub's HTTP 500
        with lock:
            assert hub.srv is None, "second server start"
            orig(options)
    hub.start = start
    ep, _ = make(tmp_path, hub)
    errs = []

    def go(h):
        try:
            ep.submit(h, f"echo {h}\n", Resources(minutes=5))
        except Exception as e:                 # noqa: BLE001
            errs.append(e)
    th = [threading.Thread(target=go, args=(h,)) for h in ("s1", "s2")]
    [t.start() for t in th]
    [t.join() for t in th]
    assert not errs and len(hub.starts) == 1
    launched = [h for h in ("s1", "s2") if ep.state.get(h).resources.get("launched_at")]
    assert len(launched) == 1 and len(ep._queue()) == 1


def test_watcher_stops_after_linger(tmp_path):
    hub = FakeHub(available=("4g.40gb",), run=False)
    ep, clock = make(tmp_path, hub, linger_s=120)
    ep.submit("w", "true\n", Resources(minutes=5))
    (ep._dir("w") / "job.pid").write_text("1"); (ep._dir("w") / "job.done").write_text("exit=0\n")
    assert ep.status("w").state == "completed" and hub.srv is not None          # inside the linger: kept
    spawned = []
    ep._spawn_watcher = lambda: spawned.append(1)
    ep.after_results("w")
    assert spawned == [1]                                                       # nobody may call again: a watcher
    assert ep.wait_and_stop() and hub.srv is None                               # sleeps out the linger, then stops
    assert clock.t >= ep.state.get("w").resources["ended_at"] + 120


def test_environment_prefix_preamble_and_runtime_hook(tmp_path):
    hub = FakeHub(run=False)
    env = JobEnvironment(var_prefix="MYEXP", preamble="source /path/to/setup.sh",
                         runtime=lambda ep: {"a": f"A() {{ ( cd {ep.workdir} && \"$@\" ); }}\n"})
    ep, _ = make(tmp_path, hub, environment=env)
    ep.submit("j", "echo hi\n", Resources(minutes=5))
    text = (ep._dir("j") / "job.sh").read_text()
    assert "export MYEXP_JOB_DIR=" in text and "MYEXP_OUT=" in text and "HEPEP_" not in text
    assert text.index("source /path/to/setup.sh\n") < text.index("echo hi")
    assert ep.runtime()["a"].startswith("A() {") and str(tmp_path / "work") in ep.runtime()["a"]
    bare, _ = make(tmp_path / "b", FakeHub(run=False))
    with pytest.raises(EndpointError, match="runtime hook"):
        bare.runtime()


def test_settings_from_environment_fail_closed(tmp_path, monkeypatch):
    with pytest.raises(site.SiteConfigError, match="HEPEP_EAF_WORKDIR"):
        EAFEndpoint(state_dir=tmp_path, hub_client=FakeHub())
    monkeypatch.setenv("HEPEP_EAF_WORKDIR", str(tmp_path / "shared/work"))
    with pytest.raises(site.SiteConfigError, match="HEPEP_EAF_SHARED_PREFIX"):
        EAFEndpoint(state_dir=tmp_path, hub_client=FakeHub())
    monkeypatch.setenv("HEPEP_EAF_SHARED_PREFIX", str(tmp_path / "shared"))
    monkeypatch.setenv("EAF_LINGER_S", "30")
    monkeypatch.setenv("HEPEP_EAF_MAX_AGE_S", "7200")
    ep = EAFEndpoint(hub_client=FakeHub())
    assert ep.linger_s == 30 and ep.max_age_s == 7200
    assert ep.state.root == site.state_dir("eaf")
    monkeypatch.setenv("HEPEP_EAF_TOKEN_FILE", str(tmp_path / "token"))
    assert EAFEndpoint(state_dir=tmp_path / "s").hub.token_file == str(tmp_path / "token")


def test_watcher_command_names_the_module_and_settings(tmp_path):
    ep, _ = make(tmp_path, FakeHub(run=False))
    argv = ep._watcher_argv()
    assert argv[1:4] == ["-m", "hependpoints.eaf", "stop-idle"]
    assert argv[argv.index("--shared-prefix") + 1] == str(tmp_path)
    assert argv[argv.index("--workdir") + 1] == ep.workdir


def test_cli_stop_idle_with_endpoint_factory(tmp_path, capsys):
    hub = FakeHub(available=("4g.40gb",), run=False)
    hub.start({"imagegpu": "x-40gb"})

    def factory(**kw):
        return EAFEndpoint(hub_client=hub, **kw)
    assert main(["stop-idle", "--state-dir", str(tmp_path / "s"), "--workdir", str(tmp_path / "w"),
                 "--shared-prefix", str(tmp_path)], endpoint_cls=factory) == 0
    assert "stopped" in capsys.readouterr().out and hub.srv is None
