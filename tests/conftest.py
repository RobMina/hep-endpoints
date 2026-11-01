"""Every test starts with no HEPEP_* configuration and a private state root (nothing is written to the home
directory). Tests that need site variables set placeholder values themselves; nothing here names a real site."""
import os

import pytest


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, tmp_path_factory):
    for name in list(os.environ):
        if name.startswith("HEPEP_") or name in ("EAF_TOKEN_FILE", "EAF_LINGER_S", "EAF_MAX_AGE_S"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HEPEP_STATE_DIR", str(tmp_path_factory.mktemp("state-root")))
    return monkeypatch
