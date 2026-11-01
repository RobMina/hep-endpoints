"""Site configuration: every host, account, user and storage path comes from arguments or the environment, never
from the package.

hep-endpoints reads only ``HEPEP_*`` variables (each endpoint module lists its own). An experiment package can pass
values explicitly, or subclass an endpoint to read its own variable names. A missing variable raises
:class:`SiteConfigError` naming it; nothing falls back to a user name or a built-in site path.

  HEPEP_STATE_DIR   root of the local job records (default ``~/.hependpoints``; one subdirectory per endpoint)
"""
from __future__ import annotations

import os
from pathlib import Path

DEFAULT_HINT = "see the hep-endpoints README"


class SiteConfigError(RuntimeError):
    """A site variable needed by this call is not set."""


def setting(name, default=None):
    """Value of a site variable, or default when it is unset or empty."""
    return os.environ.get(name) or default


def require(name, *alternatives, hint=DEFAULT_HINT):
    """Value of the first set variable among name and alternatives; SiteConfigError when none is set."""
    for n in (name, *alternatives):
        value = os.environ.get(n)
        if value:
            return value
    names = " or ".join((name, *alternatives))
    raise SiteConfigError(f"set {names} ({hint})")


def number(name, default=None):
    """A numeric site variable as float, default when unset; SiteConfigError when it is not a number."""
    value = setting(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        raise SiteConfigError(f"{name}={value!r} is not a number") from None


def state_root() -> Path:
    """Root of the local job records: $HEPEP_STATE_DIR, else ~/.hependpoints."""
    return Path(setting("HEPEP_STATE_DIR", "~/.hependpoints")).expanduser()


def state_dir(endpoint_name: str) -> Path:
    """Default state dir of one endpoint: <state_root>/<endpoint_name>."""
    return state_root() / endpoint_name
