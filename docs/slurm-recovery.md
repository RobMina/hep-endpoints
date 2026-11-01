# Slurm submission recovery

The Slurm-over-SSH adapter uses a durable local intent and per-handle POSIX lock to
close the scheduler-acceptance/local-record gap. This is **at most one dispatch
attempt per intent**, not an unconditional exactly-once execution guarantee.
EAF and higher-level streaming/task records are outside this protocol. The jobsub
adapter uses the same intent and lock machinery with a different reconciliation
source (see [jobsub.md](jobsub.md)).

## State and sequence

- `<handle>.json` job records keep their schema and short `spec_sha`.
  A same-spec retry returns the existing record without scheduler queries or new
  budget checks. Records with retained intents must match their original endpoint
  target on retry and lifecycle calls. Legacy records without intents (written by
  clients older than the intent protocol) keep their behavior; their original
  target cannot be certified from those records.
- `.locks/<sha256(handle)>.lock` serializes submission and final-state updates.
  Lock files are never removed. Distinct handles remain independent.
- `.intents/<sha256(handle)>.json` holds the request's full SHA-256, rendered job
  body/target identity, resources, original timestamp and remote directory, plus a
  random `<job_prefix>-v1-<uuid>` scheduler comment token (`job_prefix` from the
  endpoint's `JobEnvironment`, default `hepep`). The body itself is not stored.
- Intent and Slurm record writes use a unique same-directory temporary file,
  file `fsync`, atomic replacement and directory `fsync`. Intent durability must
  succeed before the runner is invoked. State-root ancestry and sidecar directory
  entries are synced; corrupt/non-regular state files (including dangling symlinks)
  fail closed rather than being treated as absent.

Under the handle lock, a new submission validates its resource budget, prepares the
script and persists its intent. It then stages and calls `sbatch --parsable` once,
with `--comment=<token>` on the command line. The comment is discovery metadata,
**not a scheduler-enforced uniqueness key**. Only a plain positive local allocation
ID is accepted. Normal success durably records the ID and retains the intent.

Any failure after intent persistence leaves that intent in place. This includes
SSH timeout, malformed acknowledgement, failed local persistence, and process
loss. A retry with a changed script/resources/host/account/partition/workdir is
refused before remote work. A matching retry does not stage or submit: it searches
for the exact comment token and adopts exactly one matching allocation.

## Recovery queries and conservative limits

Recovery queries both active and terminal-job sources, scoped to the remote user
and local cluster:

```text
unset SLURM_CLUSTERS
squeue --local -h --me -o '%i|%.128k'
TZ=UTC sacct --local -n -X -P --duplicates --user="$(id -un)" \
  --starttime=<UTC intent timestamp minus one day> -o JobIDRaw,Comment%128
```

The complete token is matched, never a prefix or truncated job name. Repeated
observations of the same allocation ID are deduplicated. Multiple matches, query
errors, malformed rows, matching array/step/heterogeneous IDs, or no visible match
leave the intent unresolved. **No number of empty queries authorizes another
`sbatch`.** Accounting lag, retention, missing comment storage, or a still-running
SSH invocation make absence inconclusive. A completed/failed/cancelled allocation
is an accepted submission and is adopted rather than rerun.

The adapter is single-cluster. Cluster-selection environment variables are cleared
before dispatch. A received cluster-suffixed or compound acknowledgement is
persisted as unsupported and quarantined for operator investigation rather than
silently dropping its suffix. Cross-cluster routing and automatic reconciliation
of those acknowledgements are not implemented.

A crash after persisting an intent but before reaching `sbatch` can strand a job
that was never submitted. This deliberately favors avoiding duplicate allocation
charges. There is no automatic intent reset. Investigate scheduler history and any
outstanding SSH process before deciding whether a new handle is safe; do not delete
an intent merely because `squeue` is empty. No jobs are cancelled by reconciliation.

The guarantees require cooperating callers using the **same durable state root**
with working advisory-lock and `fsync` semantics. Independent state roots, state
loss/manual deletion, external comment changes, or simultaneous old clients that
ignore intents are outside the guarantee. Runtime code and inputs remain mutable;
this protocol does not supply immutable manifests or repair old missing records.

## Verification and rollout

Mock-only tests (`tests/test_submission_recovery.py`) cover lost replies, malformed
acknowledgements, failed record writes, crashes before dispatch, delayed accounting,
query errors, exact matching, conflicts, duplicate identities, legacy records,
durable-write ordering, concurrent threads/processes, lock release after process
death, and unsupported acknowledgements. No test contacts SSH or a scheduler.

Before relying on live recovery, confirm the deployed Slurm CLI supports these
fields and that accounting retains comments (`AccountingStoreFlags=job_comment`).
The tests are not a live lost-acknowledgement exercise. Roll out to all processes
sharing a state root together; do not mix old and new submitters.

Primary CLI references (consulted when the protocol was written, 2026-10-25):
[squeue](https://slurm.schedmd.com/squeue.html) (`%k`, `--local`, `--me`),
[sacct](https://slurm.schedmd.com/sacct.html) (`Comment`, explicit time window,
`--duplicates`, allocation-only/parsable output),
[sbatch](https://slurm.schedmd.com/sbatch.html) (`--comment`, `--parsable`).
Site-specific support and comment retention must be checked per site.
