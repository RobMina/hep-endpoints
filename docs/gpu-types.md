# GPU types on a Slurm site

`SlurmSSHEndpoint` can choose the GPU type of each GPU job from a table the experiment or site configuration
supplies. Without a table (the default) every job is submitted exactly as requested and nothing below applies.

## The table

```python
from hependpoints.slurm_ssh import GpuTable, GpuType, SlurmSSHEndpoint

TABLE = GpuTable(                         # example values, not any real site's
    types={
        "a100": GpuType(su_per_gpu_h=50.0, su_per_core_h=0.4, su_per_gb_h=0.04),
        "a40": GpuType(su_per_gpu_h=20.0, su_per_core_h=0.3, su_per_gb_h=0.02),
        "1g.24gb": GpuType(partition="gpu-mig", su_per_gpu_h=0.0, max_gpus=1, max_cores=4, max_mem_gb=32),
    },
    auto=("1g.24gb",),                    # candidates of gpu_type="auto", most preferred first
    default="auto",                       # the endpoint's gpu_type when none is given
    cpu_su_per_core_h=0.2, cpu_su_per_gb_h=0.01,
    price_unknown_as="a100",              # rates for a GPU request whose type is not in the table
)
ep = SlurmSSHEndpoint(gpu_table=TABLE)    # gpu_type="auto" | "<type>" | None
```

Each key is the gres type name (`--gres=gpu:<name>:<n>`). A `GpuType` gives

| Field | Meaning |
|---|---|
| `partition` | partition of jobs of this type (None: the endpoint's GPU partition) |
| `su_per_gpu_h`, `su_per_core_h`, `su_per_gb_h` | charge per GPU-, core- and GB-hour (`su_per_gpu_h` None: not priced) |
| `max_gpus`, `max_cores`, `max_mem_gb` | the most one job of this type may ask for (None: no limit) |

## The rule

| `gpu_type` | GPU job submitted as |
|---|---|
| `"auto"` | the first `auto` candidate whose `max_gpus` and `max_mem_gb` the request fits, with cores capped at its `max_cores`; a request that fits no candidate keeps its own type, cores and memory (memory and GPU count are never reduced) |
| a type name | that type for every GPU job, cores and memory capped at its limits |
| `None` | as requested |

CPU-only jobs are never changed. The type's `partition` is used unless the endpoint's GPU partition was set
explicitly (argument or `HEPEP_SLURM_PARTITION`), in which case that partition wins.

## What is recorded

The choice is a dict `{"policy": "auto" | "explicit", "chosen": <type>, "requested": <the original Resources
fields>, "why": <reason>}`. It is written as one comment line in the job script header,
`# <job_prefix> gpu_choice {json}`, and kept in the job record (`JobRecord.gpu_choice`; None for CPU jobs, without a
table, or with `gpu_type=None`). The record's `resources` and `spec_sha` are those of the job actually submitted. The
reason text comes from `GpuTable.fit_reason` / `nofit_reason`, format strings with the fields `gpus`, `mem_gb`, `cores`
(the request), `type`, `max_gpus`, `max_mem_gb`, `max_cores` (the chosen candidate, or the most preferred one when
none fits) and `candidates`.

Submitting the same handle again with the same request returns the same job: the request is shaped the same way
again. A record or pending intent written before the table was configured (with the unshaped request) is still
found and is not resubmitted. A submission recovered after a lost scheduler reply keeps its choice.

## Budget

With `max_su_per_job` set, the estimate is taken on the job actually submitted. Flat rates (`su_per_gpu_h`,
`su_per_core_h`, or their variables) take precedence; otherwise the table prices the job: wall hours x (GPUs x GPU
rate + cores x core rate + GB x GB rate), CPU-only jobs at the table's CPU rates. A job the table cannot price
(unknown type without `price_unknown_as`, or no CPU rates for a CPU job) is refused when a cap is set.
