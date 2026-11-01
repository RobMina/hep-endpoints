"""A fake FNAL grid for the jobsub endpoint tests: stand-ins for jobsub_submit, jobsub_q, jobsub_rm, jobsub_fetchlog and
ifdh, run as real commands on PATH (``fakegrid.py <command> args...``). State lives in $FAKEGRID_DIR/state.json; the
"dCache" is the local file system. Only the behaviour the endpoint relies on is modelled, after the jobsub_lite
documentation; this is not a test of jobsub_lite itself."""
import fcntl
import json
import os
import shutil
import sys
from contextlib import contextmanager
from pathlib import Path

SCHEDD = "fake-schedd.example"


@contextmanager
def state():
    root = Path(os.environ["FAKEGRID_DIR"])
    with open(root / "state.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = root / "state.json"
        data = json.loads(path.read_text()) if path.exists() else {"next": 1000, "jobs": {}}
        yield root, data
        path.write_text(json.dumps(data, indent=1))


def option(args, name):
    for i, a in enumerate(args):
        if a == name:
            return args[i + 1]
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return None


def jobsub_submit(args):
    if not option(args, "-G"):
        sys.exit("jobsub_submit: -G is required")
    scripts = [a for a in args if a.startswith("file://")]
    if len(scripts) != 1:
        sys.exit("jobsub_submit: one file:// executable expected")
    env = dict(args[i + 1].split("=", 1) for i, a in enumerate(args) if a == "-e")
    with state() as (root, data):
        data["next"] += 1
        jid = f"{data['next']}.0@{SCHEDD}"
        copy = root / f"{data['next']}.sh"
        shutil.copy(scripts[0][len("file://"):], copy)
        data["jobs"][jid] = {"state": "I", "script": str(copy), "env": env, "argv": args, "cwd": os.getcwd()}
    print(f"Submitting job(s).\n1 job(s) submitted to cluster {jid.split('.')[0]}.\n")
    print(f"Use job id {jid} to retrieve output")


def jobsub_q(args):
    jid = option(args, "--jobid")
    print("JOBSUBJOBID                           OWNER    SUBMITTED     RUNTIME   ST PRIO   SIZE  COMMAND")
    with state() as (_, data):
        job = data["jobs"].get(jid)
        if job and job["state"] in "IRHXS<>":
            print(f"{jid}  someone  10/31 12:00  0+00:00:10 {job['state']}   0   0.0 job.sh")


def jobsub_rm(args):
    jid = option(args, "--jobid")
    with state() as (_, data):
        job = data["jobs"].get(jid)
        if not job or job["state"] not in "IRHS<>":
            sys.exit(f"jobsub_rm: no such job {jid}")
        job["state"] = "X"
        job["removed"] = True
    print(f"Job {jid} marked for removal")


def jobsub_fetchlog(args):
    jid, dest = option(args, "--jobid"), option(args, "--destdir")
    if os.environ.get("FAKEGRID_FETCHLOG_FAIL"):
        sys.exit("jobsub_fetchlog: transfer failed")
    Path(dest, f"{jid.split('@')[0]}.out").write_text(f"stdout of {jid}\n")


def ifdh(args):
    cmd, rest = args[0], args[1:]
    if cmd == "cp":
        src, dst = rest
        if not os.path.isfile(src):
            sys.exit(f"ifdh cp: {src}: no such file")
        if os.path.exists(dst) and os.path.dirname(os.path.abspath(dst)).startswith(os.environ["FAKEGRID_DCACHE"]):
            sys.exit(f"ifdh cp: {dst} exists (dCache is write-once)")
        shutil.copyfile(src, dst)
    elif cmd == "mkdir_p":
        os.makedirs(rest[0], exist_ok=True)
    elif cmd == "rm":
        try:
            os.unlink(rest[0])
        except FileNotFoundError:
            sys.exit(f"ifdh rm: {rest[0]}: no such file")
    else:
        sys.exit(f"ifdh: unsupported {cmd}")


if __name__ == "__main__":
    name, argv = sys.argv[1], sys.argv[2:]
    {"jobsub_submit": jobsub_submit, "jobsub_q": jobsub_q, "jobsub_rm": jobsub_rm,
     "jobsub_fetchlog": jobsub_fetchlog, "ifdh": ifdh}[name](argv)
