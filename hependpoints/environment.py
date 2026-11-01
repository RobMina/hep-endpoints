"""What an experiment package adds to every job an endpoint runs.

An endpoint renders each job as ``header + preamble + script``. The header exports the job directory and the output
directory under the variable prefix (``$HEPEP_JOB_DIR`` and ``$HEPEP_OUT`` by default); the script writes its results
under the output directory. Everything experiment-specific (software setup, PYTHONPATH, a virtual environment,
container wrappers) goes in the preamble or in the ``runtime`` hook, never in hep-endpoints.

    env = JobEnvironment(var_prefix="MYEXP", job_prefix="myexp",
                         preamble="source /cvmfs/<experiment software>/setup.sh\\n")
    ep = SlurmSSHEndpoint(environment=env, ...)

``runtime`` is an optional callable ``runtime(endpoint) -> {name: shell text}`` that an endpoint exposes as
``endpoint.runtime()``: named shell definitions a higher-level task renderer splices into its job bodies (for example
a function that runs a command in the experiment's environment on that endpoint).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


@dataclass(frozen=True)
class JobEnvironment:
    var_prefix: str = "HEPEP"   # job scripts see $<var_prefix>_JOB_DIR and $<var_prefix>_OUT
    job_prefix: str = "hepep"   # scheduler job names <job_prefix>_<handle>; submission tokens <job_prefix>-v1-<hex>
    preamble: str = ""          # shell text placed after the endpoint header, before every job script
    runtime: Optional[Callable[[Any], Dict[str, str]]] = None

    def __post_init__(self):
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,31}", self.var_prefix):
            raise ValueError(f"var_prefix {self.var_prefix!r}: upper-case letters, digits and _ only")
        if not re.fullmatch(r"[a-z][a-z0-9]{0,15}", self.job_prefix):
            raise ValueError(f"job_prefix {self.job_prefix!r}: lower-case letters and digits only, at most 16")
        if not isinstance(self.preamble, str):
            raise ValueError("preamble must be shell text")
        if self.preamble and not self.preamble.endswith("\n"):
            object.__setattr__(self, "preamble", self.preamble + "\n")

    @property
    def job_dir_var(self) -> str:
        return f"{self.var_prefix}_JOB_DIR"

    @property
    def out_var(self) -> str:
        return f"{self.var_prefix}_OUT"

    @property
    def token_var(self) -> str:
        return f"{self.var_prefix}_TOKEN"


DEFAULT = JobEnvironment()
