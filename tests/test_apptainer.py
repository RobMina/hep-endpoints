"""The nested-Apptainer sandbox fallback, run in a real bash against a stand-in apptainer (no container runtime)."""
import subprocess
from pathlib import Path

import pytest

from hependpoints.apptainer import sandbox_fallback

# exec IMAGE CMD...: a SIF file fails when FAKE_NO_FUSE is set (as without /dev/fuse), a directory runs; CMD is run.
# build --sandbox DIR SIF: makes DIR (fails when FAKE_NO_BUILD is set).
FAKE = r"""#!/bin/bash
op=$1; shift
if [ "$op" = build ]; then
  [ -n "$FAKE_NO_BUILD" ] && { echo "build: no space" >&2; exit 1; }
  [ "$1" = --sandbox ] && mkdir -p "$2" && cp "$3" "$2/image" && exit 0; exit 2
fi
[ "$op" = exec ] || exit 2
while [ "${1#-}" != "$1" ]; do shift; done
img=$1; shift
if [ -f "$img" ] && [ -n "$FAKE_NO_FUSE" ]; then echo "FATAL: fuse: device not found" >&2; exit 255; fi
[ -d "$img" ] && [ -n "$FAKE_SANDBOX_BROKEN" ] && exit 1
"$@"
"""


def _run(tmp_path, **env):
    appt = tmp_path / "apptainer"
    appt.write_text(FAKE)
    appt.chmod(0o755)
    w = tmp_path / "job"
    w.mkdir()
    (w / "img.sif").write_bytes(b"sif")
    script = (f'W={w}; APPT={appt}; CHECK="print(1)"\n'
              + sandbox_fallback(check="true") + 'echo "mode=$MODE img=$IMG"\n')
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                          env={"PATH": "/usr/bin:/bin", **env}), w


def test_a_sif_that_mounts_is_used_as_is(tmp_path):
    p, w = _run(tmp_path)
    assert p.returncode == 0 and f"mode=sif img={w}/img.sif" in p.stdout and not (w / "img").exists()


def test_without_fuse_the_sif_is_unpacked_to_a_sandbox_and_removed(tmp_path):
    p, w = _run(tmp_path, FAKE_NO_FUSE="1")
    assert p.returncode == 0, p.stdout + p.stderr
    assert f"mode=sandbox img={w}/img" in p.stdout and "building a sandbox" in p.stdout
    assert (w / "img" / "image").read_bytes() == b"sif" and not (w / "img.sif").exists()
    assert not (w / "apptmp").exists()


@pytest.mark.parametrize("env, text", [({"FAKE_NO_BUILD": "1"}, "sandbox build failed"),
                                       ({"FAKE_SANDBOX_BROKEN": "1"}, "nested apptainer failed (sandbox)")])
def test_neither_runs_exits_with_the_failure_code(tmp_path, env, text):
    p, w = _run(tmp_path, FAKE_NO_FUSE="1", **env)
    assert p.returncode == 4 and text in p.stdout and "mode=" not in p.stdout


def test_names_and_paths_are_arguments():
    text = sandbox_fallback(apptainer="apptainer", sif='"$J/a.sif"', sandbox='"$J/a"', tmpdir='"$J/t"',
                            log='"$J/l"', check="true", image_var="IMAGE", mode_var="HOW", fail_rc=7)
    assert 'IMAGE="$J/a.sif"; HOW=sif' in text and "exit 7" in text and '"$W' not in text and '"$APPT"' not in text
