# The jobsub endpoint

`hependpoints.jobsub.JobsubEndpoint` runs jobs on the FNAL grid with jobsub_lite. Every command runs on a submit
host where `jobsub_submit`, `jobsub_q`, `jobsub_rm`, `jobsub_fetchlog` and `ifdh` work with a valid grid token:
this machine (`host` unset) or one reached with `ssh -o BatchMode=yes <host>` (`host` / `HEPEP_JOBSUB_HOST`). The
endpoint never obtains or refreshes credentials.

## Submit

Under the per-handle lock the endpoint renders the job file, writes a durable intent with a fresh token
`<job_prefix>-v1-<hex>`, then in one remote command writes `<workdir>/<handle>/job.sh`, changes to that directory
(jobsub's dropbox handling writes temporary copies into the CWD) and runs

```text
jobsub_submit -G <group> [--role=<role>] --resource-provides=usage_model=<usage_model>
  --cpu=<cores> --memory=<mem_gb>GB --expected-lifetime=<minutes>m [--disk=<disk>]
  [--gpu=<gpus> [--append_condor_requirements=<gpu_types[gpu_type]>]]
  [<image_option> <image>] [--use-cvmfs-dropbox | --use-pnfs-dropbox] [--tar-file-name=<tarball> ...]
  --need-storage-modify=<storage_modify> --lines=+HepepToken="<token>" -e <PREFIX>_TOKEN=<token>
  --mail_never [extra_args ...] file://<workdir>/<handle>/job.sh
```

The job id is taken from the line `Use job id <cluster>.<proc>@<schedd> to retrieve output`; exactly one is
accepted. Several ids are persisted as an unsupported acknowledgement and quarantined (operator investigation).

## The job on the worker

The job file checks that `$<PREFIX>_TOKEN` is set, works in `$_CONDOR_SCRATCH_DIR/hepep_job`, exports
`$<PREFIX>_JOB_DIR` and `$<PREFIX>_OUT`, runs `worker_setup`, creates `<output_dir>/<handle>/<token>/` with
`mkdir_command` and copies `jobid` (`$JOBSUBJOBID`) there. It then redirects its output to `job.log`, installs an
EXIT trap (and `TERM` -> exit 143) and runs the environment preamble and the caller's script. On any exit the trap
prints the tail of `job.log` to the original stdout (so `jobsub_fetchlog` has it), packs `out/` and `job.log` into
`result.tar`, copies it, and copies `job.done` (`exit=<rc>`) last. Each copy removes an existing destination first
(dCache files are write-once; a restarted job overwrites its own token directory). The output directory is bound
to the intent's token, so results of different attempts can never be confused.

## Status, fetch, cancel

- `status`: `jobsub_q -G <group> --jobid <id>`. A listed job in state I, R, S, `<`, `>` is `working`; H (held) is
  `working` with a "held" message, or `failed` with `held_is_failed=True`. A failing `jobsub_q` is an error, never
  "absent". A job not listed (or C/X) is decided by `job.done`: exit 0 `completed`, otherwise `failed` (or
  `cancelled` after a cancel). No `job.done`: removed (X) is `failed`, a cancelled job `cancelled`, otherwise
  `working` until `absent_grace_s` (default 900 s) after it was first seen missing, then `failed`. Final states are
  cached.
- `fetch`: `jobsub_fetchlog -G <group> --jobid <id> --destdir <tmp>` into `dest/<handle>/logs/` (best effort; a
  failure leaves `logs/FETCHLOG_FAILED.txt`), then `result.tar` unpacked into `dest/<handle>/` (`out/`, `job.log`).
- `cancel`: records a local cancel marker and runs `jobsub_rm -G <group> --jobid <id>` while the job is listed.

Files on dCache are read only by known path with `copy_command` into a temporary file on the submit host, never by
listing or stat-ing the dCache mount.

## Token scope

jobsub_lite strips every `storage.modify` scope from the job token and adds back only the `--need-storage-modify`
paths, as logical storage paths (the dCache mount prefix dropped). The default `storage_modify` is therefore
`token_path(output_dir)`: `<mount_prefix>vo/scratch/x/out` becomes `/vo/scratch/x/out`, so the job may write its
results and nothing else. A job that writes elsewhere needs a wider `storage_modify`; a path outside the submitting
user's own token scope fails at submit.

## Staging job inputs

A job that needs large inputs (an image, a code tarball, tables), more than the dropbox's per-file limit or more than
should be re-uploaded per submit, can take them from dCache scratch. `stage_file(src, kind, name, sha256, size)`
runs on the submit host:

1. a destination already recorded as verified (`<state>/staged/`) is returned without a remote call;
2. otherwise `src` is rehashed and its size checked against the pin (a changed source is refused);
3. the destination `<stage_dir>/<kind>/<sha256[:16]>/<name>` is removed, its directory made, and `src` copied with
   `copy_command`;
4. the copy is read back into a temporary file on the submit host and rehashed; only a match is recorded.

The job copies the staged paths into `$<PREFIX>_JOB_DIR` with `copy_command` and should check each digest (for
example `sha256sum -c`) before use. Staged files sit on scratch (LRU-purged): a purged file fails the job's copy, and
a record can be dropped (delete its JSON under `<state>/staged/`) to stage again.

## Shipping an image: the sandbox fallback

A job that runs its own SIF image with the worker's Apptainer (nested in the job's container) needs FUSE to mount
the SIF, and many grid workers have no /dev/fuse. `hependpoints.apptainer.sandbox_fallback(...)` renders job-script
lines that try the SIF with a check command and, when that fails, unpack it with `apptainer build --sandbox` in the
job directory, delete the SIF and check again; the image and mode (`sif` / `sandbox`) variables tell the rest of the
script what to run. Size `--disk` for the SIF plus a sandbox of about 3-4 times its size.

## Recovery

As with Slurm, an intent is written before `jobsub_submit` runs, and any failure after that (timeout, error reply,
missing or garbled acknowledgement, a crash) leaves it in place. A retry of the same request (same body, resources
and target) never submits again: it reads `<output_dir>/<handle>/<token>/jobid`, which the job writes when it
starts, and adopts that id. Until the job starts (idle, held, or never submitted) the retry reports "unresolved".
Only an operator decides that an intent whose job never appears was not submitted: look for the job with its
`HepepToken` classad (for example `condor_q -constraint 'HepepToken == "<token>"'` on the schedd), and do not delete
the intent merely because nothing is visible. Changed requests or targets are refused before any remote call.

## Not exercised against a real grid

The tests (`tests/test_jobsub.py`) run the endpoint's real command lines in a local bash against stand-in
`jobsub_*` and `ifdh` commands (`tests/fakegrid.py`) that model the jobsub_lite behaviour as documented, and run the
rendered job as a worker would (including SIGTERM). Nothing has been run against a schedd, a worker node or dCache.
Before relying on it, check with one test job:

- the flag spellings on the installed jobsub_lite (`jobsub_submit --help`): `--tar-file-name`, `--use-cvmfs-dropbox`
  / `--use-pnfs-dropbox`, `--singularity-image` (or an Apptainer spelling via `image_option`), `--gpu`,
  `--append_condor_requirements`, `--mail_never`, `--lines`, and `-e NAME=VALUE`;
- the `Use job id ... to retrieve output` line, and the `jobsub_q --jobid` table layout (first column the job id,
  ST a single letter) and its exit status for a job that has left the queue (assumed 0 with no row);
- `jobsub_fetchlog --destdir` writing the unpacked logs into that directory;
- that `$JOBSUBJOBID` is set on the worker (otherwise the `jobid` report cannot be adopted and recovery needs an
  operator), and that `ifdh cp`/`mkdir_p`/`rm` work there and on the submit host with the job token scope given by
  `--need-storage-modify`;
- held-job behaviour with your release policy (for example `--lines=+FERMIHTC_AutoRelease=True` in `extra_args`).
