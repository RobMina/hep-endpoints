"""No user name, no site, user or storage path, and no experiment-specific name may be written into the repository.

Site paths come from configuration (see hependpoints/site.py and the README). Public locations stay allowed:
/cvmfs, system paths (/usr, /bin, /dev, /proc, /etc), relative and home-relative paths, and placeholders such as
/path/to. Experiment names and experiment configuration prefixes may appear only in the provenance files (NOTICE,
and the README's provenance and users paragraphs), never in the package, its tests or its docs.
"""
import hashlib
import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# User names, matched as whole alphanumeric tokens (case-insensitive) by SHA-256, so the names never appear here.
USER_NAME_SHA256 = {"4baf0263de48597ccb579ba119bb6d2f8fea4bba418326f375a4853462ab82fa",
                    "05436ea5391c8a9c46f09d1200e8669806543032d865c86649dfacc9293603f4"}
_TOKEN = re.compile(r"[A-Za-z0-9]+")


class _UserNames:
    """search() like a compiled pattern: the first token whose hash is in USER_NAME_SHA256, or None."""
    hashes = USER_NAME_SHA256

    def search(self, line):
        for m in _TOKEN.finditer(line):
            if hashlib.sha256(m.group(0).lower().encode()).hexdigest() in self.hashes:
                return m
        return None


USER_NAMES = _UserNames()

# Absolute site, user or storage paths: the prefix must start a path (not follow a word, a dot or a variable).
_PREFIXES = (
    "exp", "pnfs", "scratch", "sfs", "lus", "eagle", "grand", "flare", "home", "nashome", "Users", "project",
    "projects", "gpfs", "lustre", "wclustre", "global",
)
_START = r"(?:(?<=://)|(?<![\w.~}$\-/]))"     # start of a path, including right after file://
SITE_PATH = re.compile(_START + r"/(?:" + "|".join(_PREFIXES) + r")(?=/|\b)")
# Named directories under /tmp (a bare /tmp, ${TMPDIR:-/tmp} or tempfile is fine).
TMP_DIR = re.compile(_START + r"/tmp/[A-Za-z0-9_]")
# Home-relative paths to ordinary directories (a personal layout); hidden tool state such as ~/.hependpoints is fine.
HOME_DIR = re.compile(r"~/(?!\.)[A-Za-z0-9_]")

# Experiment names, experiment packages and their configuration prefixes, as whole tokens (case-insensitive).
EXPERIMENT = re.compile(r"(?i)(?<![A-Za-z0-9])(?:mu2e\w*|nova|dune|crv|m2fs_?\w*|fastsimkit\w*|crvfs\w*|"
                        r"uva|rivanna|afton)(?![A-Za-z0-9])")
EXPERIMENT_ALLOWED = {"NOTICE", "README.md"}

SELF = Path(__file__).resolve()


def _tracked_files():
    try:
        # tracked files plus new files not yet added (ignored files excluded), so a check before `git add` sees them
        out = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT,
                             check=True, capture_output=True).stdout
        names = [n for n in out.decode().split("\0") if n]
        if names:
            return [ROOT / n for n in names]
    except (OSError, subprocess.CalledProcessError):
        pass
    found = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in (".git", "__pycache__", ".pytest_cache", ".venv")]
        found.extend(Path(dirpath) / f for f in filenames)
    return found


def _text(path):
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if b"\0" in data[:8192]:
        return None
    return data.decode("utf-8", errors="replace")


def _violations():
    bad = []
    for path in _tracked_files():
        if path.resolve() == SELF or not path.is_file():
            continue
        text = _text(path)
        if text is None:
            continue
        rel = str(path.relative_to(ROOT))
        patterns = (USER_NAMES, SITE_PATH, TMP_DIR, HOME_DIR) + (() if rel in EXPERIMENT_ALLOWED else (EXPERIMENT,))
        for lineno, line in enumerate(text.splitlines(), 1):
            for rx in patterns:
                m = rx.search(line)
                if m:
                    bad.append(f"{path.relative_to(ROOT)}:{lineno}: {m.group(0)!r} in {line.strip()[:120]}")
                    break
    return bad


def test_patterns_catch_what_they_should():
    assert HOME_DIR.search("cd ~/work/x") and not HOME_DIR.search("~/.hependpoints/eaf")
    for s in ("/exp/someexp/data/x", "cd /pnfs/someexp", "'/scratch/abc'", "/lus/eagle/projects/x", "x=/home/u",
              "/Users/someone", "/project/area", "file:///pnfs/x"):
        assert SITE_PATH.search(s), s
    names = _UserNames()
    names.hashes = {hashlib.sha256(b"alice").hexdigest()}
    assert names.search("user ALICE here") and names.search("/data/alice/x") and not names.search("malice")
    assert TMP_DIR.search("out=/tmp/my_run")
    for s in ("/cvmfs/someexp.opensciencegrid.org", "/usr/bin/env", "/path/to/scratch", "terminal", "determinant",
              "$HOME/exp/x", "${SCRATCH}/scratch/y", "relative/exp/z", "https://example.org/projects/x", "/tmp"):
        assert not (USER_NAMES.search(s) or SITE_PATH.search(s) or TMP_DIR.search(s)), s
    for s in ("MU2E", "export M2FS_OUT=x", "FASTSIMKIT_SCRATCH", "~/.mu2efastsim", "mu2efastsim.endpoints",
              "the nova group", "-G dune", "crv tables", "UVA cluster"):
        assert EXPERIMENT.search(s), s
    for s in ("novaxyz", "renovate", "crvx", "hependpoints", "HEPEP_OUT", "induced", "curvature"):
        assert not EXPERIMENT.search(s), s


def test_no_user_names_or_site_paths_in_tracked_files():
    bad = _violations()
    assert not bad, "site paths or user names in the repository:\n" + "\n".join(bad)
