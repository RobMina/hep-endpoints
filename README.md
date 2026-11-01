# hep-endpoints

Experiment-neutral adapters for running batch jobs on HEP compute endpoints from Python: one small interface
(submit / status / fetch / cancel) over

| Endpoint | Module | Where it runs jobs |
|---|---|---|
| `slurm_ssh` | `hependpoints.slurm_ssh` | any Slurm cluster reachable with `ssh <host>` (key or Kerberos) |
| `eaf` | `hependpoints.eaf` | Fermilab EAF: the user's JupyterHub server on an A100 MIG slice, driven with a hub token |
| `jobsub` | `hependpoints.jobsub` | the FNAL grid through jobsub_lite, from a host that has the `jobsub_*` commands |

The package is standard-library only (the EAF terminal needs the optional `websocket-client`, extra `eaf`). It holds
no experiment names, site paths, user names or accounts: every site setting comes from arguments or `HEPEP_*`
environment variables, and the experiment package supplies its job environment through hooks.

Provenance and users: the interface, the Slurm and EAF adapters, the durable submission intents, source snapshots
and artifact bindings were extracted from `mu2e-fastsim-core` (commit 27bd922) and generalized (see NOTICE). The
intended users are experiment packages such as mu2e-fastsim-core, bdnmc-nova and bdnmc-dune, which depend on this
package and keep their own settings.

## Install and test

```bash
python -m pip install -e '.[test]'        # add ,eaf for the EAF terminal client
python -m pytest -q
```

The tests run offline: Slurm, the JupyterHub and the grid are fakes (the jobsub tests put stand-in `jobsub_*` and
`ifdh` commands on `PATH` and run the rendered job in a local bash). CI (`.github/workflows/ci.yml`) runs the same
`pytest` on every push to main and every pull request, for each supported Python version.

## The interface

```python
from hependpoints import Resources, JobStatus, JobRecord, EndpointError, get

ep = get("slurm_ssh", ...)                     # or construct the class directly
rec: JobRecord = ep.submit(handle, script, Resources(cores=8, mem_gb=32, gpus=1, gpu_type="a100", minutes=60))
st: JobStatus = ep.status(handle)              # st.state in working | completed | failed | cancelled
files = ep.fetch(handle, dest_dir)             # copies the job's output directory to dest_dir/<handle>/
ep.cancel(handle)
```

- A handle is the caller's job name (`[A-Za-z0-9_][A-Za-z0-9_.-]*`). Submitting the same handle with the same script
  and resources returns the same record (idempotent); with a different script or resources it is refused.
- The script is a Bash body. It writes its results under `$HEPEP_OUT` (the prefix is configurable, below).
- Records live in a local state directory (default `~/.hependpoints/<endpoint>`, root `$HEPEP_STATE_DIR`), so a
  restarted caller finds its jobs. Final states are cached.
- `admit_resources(resources)` runs the endpoint's deterministic checks (budget, lifetime, device limits) without
  contacting the site; `submit` runs them too.
- Errors are `EndpointError`; a missing setting is `hependpoints.site.SiteConfigError`, naming the variable.

Slurm and jobsub fence every dispatch with a durable intent and a per-handle lock: if the scheduler's reply is lost,
a retry of the same request reconciles (finds the job by its token) and never submits a second time. See
[docs/slurm-recovery.md](docs/slurm-recovery.md) and [docs/jobsub.md](docs/jobsub.md).

## Configuration

### `slurm_ssh`

| Argument | Variable | Meaning |
|---|---|---|
| `host` | `HEPEP_SLURM_HOST` | ssh host of the login node (required when first used) |
| `account` | `HEPEP_SLURM_ACCOUNT` | Slurm account (required when first used) |
| `workdir` | `HEPEP_SLURM_WORKDIR` | job directories on the cluster (required) |
| `partition` | `HEPEP_SLURM_PARTITION` | partition for GPU jobs (default `gpu`) |
| `cpu_partition` | `HEPEP_SLURM_CPU_PARTITION` | partition for CPU-only jobs (default: none, the site default) |
| `max_su_per_job` | `HEPEP_SLURM_MAX_SU_PER_JOB` | budget cap per job (default: none) |
| `su_per_gpu_h`, `su_per_core_h` | `HEPEP_SLURM_SU_PER_GPU_HOUR`, `HEPEP_SLURM_SU_PER_CORE_HOUR` | the site's charge rates |

The budget estimate is `minutes/60 * (gpus * su_per_gpu_h + cores * su_per_core_h)`; for example a site charging
53 SU per GPU-hour and 0.5 SU per core-hour charges about 19 SU for 8 cores and one GPU for 20 minutes. A cap without
rates refuses every job. Extras for tasks that stream inputs into a running job: `read_file`, `run_on_node`,
`stream_in` (`srun --overlap` steps), and `times`.

### `eaf`

| Argument | Variable | Meaning |
|---|---|---|
| `workdir` | `HEPEP_EAF_WORKDIR` | job directories, under the shared prefix (required) |
| `shared_prefix` | `HEPEP_EAF_SHARED_PREFIX` | storage mounted by both EAF and the controlling host (required) |
| `token_file` | `HEPEP_EAF_TOKEN_FILE` (or `EAF_TOKEN_FILE`) | hub API token, mode 600 |
| `hub` | `HEPEP_EAF_HUB` | default `https://analytics-hub.fnal.gov` |
| `linger_s` | `HEPEP_EAF_LINGER_S` (or `EAF_LINGER_S`) | idle time before the server is stopped (default 120 s) |
| `max_age_s` | `HEPEP_EAF_MAX_AGE_S` (or `EAF_MAX_AGE_S`) | server lifetime, if the hub enforces one |

One job runs at a time on the user's single slice; later submits queue and launch on later `status` calls.
`python -m hependpoints.eaf {server,jobs,stop-idle}` inspects or stops the server.

### `jobsub`

| Argument | Variable | Meaning |
|---|---|---|
| `group` | `HEPEP_JOBSUB_GROUP` | `-G` experiment group (required) |
| `workdir` | `HEPEP_JOBSUB_WORKDIR` | submit-host directory for job files and the submit CWD (required) |
| `output_dir` | `HEPEP_JOBSUB_OUTPUT_DIR` | dCache scratch directory for results (required) |
| `host` | `HEPEP_JOBSUB_HOST` | ssh host with jobsub_lite; unset = run on this host |
| `role` | `HEPEP_JOBSUB_ROLE` | `--role` |
| `storage_modify` | `HEPEP_JOBSUB_STORAGE_MODIFY` | `--need-storage-modify` (default: `output_dir`) |
| `image` | `HEPEP_JOBSUB_IMAGE` | Apptainer image (`image_option`, default `--singularity-image`) |
| `tarballs` | `HEPEP_JOBSUB_TARBALL` (comma-separated) | `--tar-file-name` values (`dropbox://...` or an RCDS dir) |
| `dropbox` | `HEPEP_JOBSUB_DROPBOX` | `cvmfs` or `pnfs` (`--use-<x>-dropbox`) |
| `disk` | `HEPEP_JOBSUB_DISK` | `--disk` (e.g. `10GB`) |
| `usage_model` | `HEPEP_JOBSUB_USAGE_MODEL` | default `DEDICATED,OPPORTUNISTIC` |
| `max_minutes` | `HEPEP_JOBSUB_MAX_MINUTES` | admission cap on `--expected-lifetime` |

Also `copy_command` / `mkdir_command` / `remove_command` (default `ifdh cp` / `ifdh mkdir_p` / `ifdh rm`),
`worker_setup` (shell text run first on the worker), `gpu_types` (`{gpu_type: condor requirement}`), `extra_args`,
`held_is_failed`, `absent_grace_s`. Resources map to `--cpu`, `--memory=<mem_gb>GB`, `--expected-lifetime=<minutes>m`
and `--gpu`. Details, the job layout and what has not been run against a real schedd: [docs/jobsub.md](docs/jobsub.md).

## Plugging in an experiment

The endpoints never know the experiment. The experiment package supplies:

1. **A job environment**, `hependpoints.environment.JobEnvironment`:
   - `var_prefix`: job scripts see `$<prefix>_JOB_DIR` and `$<prefix>_OUT` (default `HEPEP`);
   - `job_prefix`: scheduler job names `<job_prefix>_<handle>` and submission tokens `<job_prefix>-v1-<hex>`;
   - `preamble`: shell text placed before every job script (software setup, PYTHONPATH, a venv);
   - `runtime`: optional `runtime(endpoint) -> {name: shell text}`, exposed as `endpoint.runtime()`, for a task
     renderer that needs endpoint-specific definitions (for example a function running a command in the experiment's
     environment on EAF).
2. **Settings**: pass them explicitly, or subclass an endpoint and override its variable lists (`HOST_VARS`,
   `ACCOUNT_VARS`, `WORKDIR_VARS`, `CONFIG_HINT`, ... in each module) to read the experiment's own variable names.
3. **Site names**: `hependpoints.register("mysite", factory)` makes `get("mysite", **kw)` build the experiment's
   configured endpoint.
4. **Source snapshots** (`hependpoints.snapshots`): a `SourceRepo(url, modules, default_modules, branch_modules,
   require_modules)` describes the experiment's public umbrella repository; `prepare_code(endpoint, base, ref, repo)`
   makes a retained, verified, read-only clone at an exact commit and `code_guard(snapshot, repo)` returns a Bash
   prefix that refuses to run on changed code.
5. **Artifact bindings** (`hependpoints.artifacts`): pin code, image and input bytes of a streamed task before
   dispatch; `runtime_prefix(manifest, guard=..., out_var=...)` takes the experiment's guard and output variable,
   `binding(..., identity=..., label=...)` its own identity function for extra artifacts. See
   [docs/artifact-bindings.md](docs/artifact-bindings.md).

## Example

```python
from hependpoints import Resources
from hependpoints.environment import JobEnvironment
from hependpoints.slurm_ssh import SlurmSSHEndpoint

env = JobEnvironment(var_prefix="MYEXP", job_prefix="myexp",
                     preamble="source /cvmfs/myexp.example.org/setup.sh\n")
ep = SlurmSSHEndpoint(environment=env, max_su_per_job=200, su_per_gpu_h=53.0, su_per_core_h=0.5)
# host, account and workdir from HEPEP_SLURM_HOST, HEPEP_SLURM_ACCOUNT, HEPEP_SLURM_WORKDIR

ep.submit("scan.p001", 'python run.py --out "$MYEXP_OUT"\n', Resources(cores=8, mem_gb=32, gpus=1, minutes=45))
while ep.status("scan.p001").state == "working":
    ...                                          # poll at the caller's pace
files = ep.fetch("scan.p001", "results")         # results/scan.p001/out/..., slurm.out
```

The same script runs on the grid with `JobsubEndpoint(environment=env, image=..., tarballs=[...])`.

## Documentation

- [docs/admission.md](docs/admission.md): what each endpoint checks before dispatch
- [docs/slurm-recovery.md](docs/slurm-recovery.md): durable intents, locks, reconciliation, limits
- [docs/jobsub.md](docs/jobsub.md): the jobsub endpoint, its job layout, and what is untested against a real grid
- [docs/artifact-bindings.md](docs/artifact-bindings.md): snapshots, pinned inputs and image checks

License: Apache-2.0 (LICENSE, NOTICE).
