# Admission: what an endpoint checks before dispatch

Every endpoint has `admit_resources(resources)`, a pure check that contacts no site, and `submit` runs it before
anything is sent (for Slurm and jobsub, also before the submission intent is written). A caller that renders tasks
can run it in its own `check` step so that check and submit admit exactly the same requests. Scheduler state and
endpoint configuration can still change between a check and a submission.

| Endpoint | Checks |
|---|---|
| `slurm_ssh` | with `max_su_per_job` set: estimated charge `minutes/60 * (gpus * su_per_gpu_h + cores * su_per_core_h)` at most the cap; a cap without rates refuses (fail closed). With a `gpu_table` and no flat rates, the table's per-type rates price the job actually submitted (after the GPU-type choice, docs/gpu-types.md). No cap configured: no budget check. |
| `eaf` | `gpus` 0 or 1 (one MIG slice per user); with `max_age_s`: wall time within the server lifetime. At launch, a server too old to fit the job is refused. |
| `jobsub` | positive cores, memory and minutes; a nonempty `gpu_type` must have a condor requirement in `gpu_types`; with `max_minutes`: lifetime at most the cap. |

Handles are restricted to an alphanumeric or underscore start followed by alphanumerics, underscore, dot or hyphen
(`hependpoints.submission.handle_key`); the EAF state dir additionally refuses `/` and leading dots.

Identity of a submission is `spec_sha(script, resources)` (the caller's script and the Resources fields). The
request digest kept in Slurm and jobsub intents also covers the rendered job body (header, preamble, script) and the
endpoint target (host, account/group, directories, partition or image, ...), so a retry after a lost reply is
accepted only when nothing that reaches the site has changed.

Budget rates are configuration. The example rates in the README and the tests (53 SU per GPU-hour, 0.5 SU per
core-hour) are one site's measured charges, not defaults.
