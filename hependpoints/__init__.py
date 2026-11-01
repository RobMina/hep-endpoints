"""Compute endpoints for HEP jobs: one interface (submit / status / fetch / cancel) per site.

An endpoint only moves a job script to a site, runs it and brings its output directory back. States are
``working``, ``completed``, ``failed`` and ``cancelled``. A handle is the caller's name for the job: submitting the
same name twice with the same script and resources returns the same job (idempotent); with a different script or
resources it is refused.

    slurm_ssh   Slurm over SSH (any Slurm site reachable by ``ssh <host>`` with a key)
    eaf         Fermilab EAF: the user's JupyterHub server on an A100 MIG slice, driven with a hub token
    jobsub      FNAL grid through jobsub_lite, from a host that has the jobsub commands (locally or over ssh)

Experiment packages plug in their job environment with :class:`hependpoints.environment.JobEnvironment` and register
their own endpoint names with :func:`register`.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Protocol

__version__ = "0.1.0"

STATES = ("working", "completed", "failed", "cancelled")


class EndpointError(RuntimeError):
    """A failure the caller should treat as a failed or refused call (transport, refusal, budget)."""


@dataclass(frozen=True)
class Resources:
    """What one job asks for. gpus counts devices of `gpu_type` ("" = any). Experiment packages may subclass this
    with their own defaults; the fields (and so spec_sha) stay the same."""
    cores: int = 1
    mem_gb: int = 2
    gpus: int = 0
    gpu_type: str = ""
    minutes: int = 60


@dataclass
class JobStatus:
    state: str
    message: str = ""
    site_state: str = ""
    job_id: str = ""


@dataclass
class JobRecord:
    """What an endpoint remembers about a submitted job (one JSON file per handle in its state dir)."""
    handle: str
    spec_sha: str
    job_id: str
    remote_dir: str
    submitted_at: float
    resources: Dict = field(default_factory=dict)
    final: Optional[Dict] = None
    gpu_choice: Optional[Dict] = None   # slurm_ssh with a gpu_table: the GPU type chosen for the request, and why


class Endpoint(Protocol):
    name: str

    def submit(self, handle: str, script: str, resources: Resources) -> JobRecord: ...

    def status(self, handle: str) -> JobStatus: ...

    def fetch(self, handle: str, dest: Path) -> List[Path]: ...

    def cancel(self, handle: str) -> str: ...


def spec_sha(script: str, resources: Resources) -> str:
    """Identity of a submission: the script text and the resources."""
    blob = json.dumps({"script": script, "resources": asdict(resources)}, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


class StateDir:
    """Local job records, one JSON per handle, written atomically."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, handle: str) -> Path:
        if not handle or "/" in handle or handle.startswith("."):
            raise EndpointError(f"bad handle {handle!r}")
        return self.root / f"{handle}.json"

    def get(self, handle: str) -> Optional[JobRecord]:
        p = self._path(handle)
        return JobRecord(**json.loads(p.read_text())) if p.exists() else None

    def put(self, rec: JobRecord) -> None:
        p = self._path(rec.handle)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(asdict(rec), indent=1))
        os.replace(tmp, p)


def _slurm(**kw):
    from .slurm_ssh import SlurmSSHEndpoint
    return SlurmSSHEndpoint(**kw)


def _eaf(**kw):
    from .eaf import EAFEndpoint
    return EAFEndpoint(**kw)


def _jobsub(**kw):
    from .jobsub import JobsubEndpoint
    return JobsubEndpoint(**kw)


_REGISTRY: Dict[str, Callable[..., Endpoint]] = {"slurm_ssh": _slurm, "eaf": _eaf, "jobsub": _jobsub}


def register(name: str, factory: Callable[..., Endpoint]) -> None:
    """Make `get(name, **kw)` return `factory(**kw)` (an experiment package names its own sites)."""
    if not isinstance(name, str) or not name:
        raise ValueError("endpoint name must be a nonempty string")
    _REGISTRY[name] = factory


def known() -> List[str]:
    return sorted(_REGISTRY)


def get(name: str, **kw) -> Endpoint:
    """An endpoint by name, configured from keyword arguments and the environment (see each module)."""
    factory = _REGISTRY.get(name)
    if factory is None:
        raise EndpointError(f"unknown endpoint {name!r}; known: {', '.join(known())}")
    return factory(**kw)
