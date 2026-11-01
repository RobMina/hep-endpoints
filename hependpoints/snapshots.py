"""Retained source snapshots; read-only permissions are not an owner security boundary.

A snapshot is a fresh clone of a public repository (the "umbrella") detached at an exact commit, with a selected set
of its submodules checked out recursively, verified against the Git objects and made read-only. The experiment
package describes its repository with :class:`SourceRepo`:

    REPO = SourceRepo(url="https://github.com/<org>/<repo>.git", modules=("core", "sim"),
                      default_modules=("core", "sim"), branch_modules=("core",))
    snap = prepare_code(endpoint, "/path/to/checkout", "main", REPO)
    guard = code_guard(snap, REPO)       # Bash prefix that refuses to run on changed code

``branch_modules`` are the top-level submodules an optional branch override moves (the others stay at the umbrella's
pins). The override set is configuration, not part of the manifest: verify a snapshot with the same SourceRepo that
prepared it (a snapshot with a branch override fails verification without it, which is the safe direction).
"""
from __future__ import annotations

import json
import posixpath
import re
import shlex
import uuid
from dataclasses import dataclass
from typing import Optional, Tuple

from . import EndpointError

_POLICY = "retained-readonly-git-v1"
_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*")


@dataclass(frozen=True)
class SourceRepo:
    url: str                                   # clone URL (https; a local absolute path enables file:// for tests)
    modules: Optional[Tuple[str, ...]] = None  # allowed top-level submodules; None = any safe name
    default_modules: Tuple[str, ...] = ()      # prepare_code's selection when none is given
    branch_modules: Tuple[str, ...] = ()       # submodules moved by a branch override
    require_modules: bool = False              # refuse an empty module selection

    def __post_init__(self):
        for field in ("modules", "default_modules", "branch_modules"):
            value = getattr(self, field)
            if value is None:
                continue
            if isinstance(value, str):
                raise ValueError(f"SourceRepo.{field} must be a sequence of module names")
            value = tuple(value)
            if any(not isinstance(v, str) or not _module_name(v) for v in value):
                raise ValueError(f"SourceRepo.{field}: invalid module name")
            object.__setattr__(self, field, value)


def _module_name(value):
    return isinstance(value, str) and bool(_NAME.fullmatch(value)) and value not in (".", "..", ".git")


# This stdlib-only program also runs on endpoints without hependpoints installed.
_REMOTE = r'''
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

request = json.loads(sys.argv[1])
env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
           GIT_TERMINAL_PROMPT="0", GIT_OPTIONAL_LOCKS="0", GIT_NO_REPLACE_OBJECTS="1")
local_fixture = request.get("url", "").startswith("/")


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def git(repo, *args):
    cmd = ["git", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
           "-c", "protocol.allow=never", "-c", "protocol.https.allow=always",
           "-c", "protocol.file.allow=" + ("always" if local_fixture else "never")]
    if repo is not None:
        cmd += ["-C", str(repo)]
    return subprocess.check_output(cmd + list(args), env=env)


def walk_error(error):
    raise error


def inside(path, root):
    return path == root or root in path.parents


def safe_ancestry(path):
    require(path.is_absolute(), "snapshot path must be absolute")
    for part in (path, *path.parents):
        require(not part.is_symlink(), "symlink in snapshot ancestry: " + str(part))


def commit(repo):
    value = git(repo, "rev-parse", "--verify", "HEAD^{commit}").decode().strip()
    require(len(value) in (40, 64) and all(c in "0123456789abcdef" for c in value),
            "unresolved commit")
    return value


def entries(repo):
    result = []
    for record in git(repo, "ls-tree", "-rz", "--full-tree", "HEAD").split(b"\0"):
        if not record:
            continue
        metadata, name = record.split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        name = os.fsdecode(name)
        require(not name.startswith("/") and all(p not in ("", ".", "..", ".git")
                for p in name.split("/")), "unsafe tracked path")
        result.append((name, mode, kind, oid))
    return result


def verify(snapshot, root):
    safe_ancestry(root)
    require(root.is_dir(), "missing snapshot")
    expected = snapshot["submodules"]
    observed = {}
    allowed = set()
    admin = {root / ".git"}

    def repository(repo, relative, wanted):
        require(repo.is_dir() and not repo.is_symlink(), "missing repository: " + str(repo))
        require(commit(repo) == wanted, "snapshot commit changed: " + str(repo))
        require(git(repo, "rev-parse", "--show-toplevel").decode().strip() == str(repo),
                "wrong repository worktree")
        head = git(repo, "rev-parse", "--abbrev-ref", "HEAD").decode().strip()
        require(head == "HEAD", "snapshot is not detached")
        for option in ("--absolute-git-dir", "--git-common-dir"):
            location = Path(git(repo, "rev-parse", option).decode().strip())
            if not location.is_absolute():
                location = repo / location
            require(inside(location.resolve(), root), "external Git metadata")
        git(repo, "diff", "--no-ext-diff", "--no-textconv", "--cached", "--quiet", "HEAD", "--")
        admin.add(repo / ".git")
        for name, mode, kind, oid in entries(repo):
            path = repo / name
            key = (relative + "/" if relative else "") + name
            if mode == "160000":
                selected = bool(relative) or name in snapshot["policy"]["modules"]
                if selected:
                    require(key in expected, "missing submodule pin: " + key)
                    override = (not relative and name in request.get("branch_modules", [])
                                and snapshot["policy"]["branch"])
                    require(override or expected[key] == oid, "submodule gitlink mismatch: " + key)
                    observed[key] = expected[key]
                    repository(path, key, expected[key])
                else:
                    require(not path.is_symlink() and (not path.exists() or
                            (path.is_dir() and not any(path.iterdir()))), "unexpected submodule content")
                continue
            require(kind == "blob" and mode in ("100644", "100755", "120000"), "unsupported Git entry")
            require(path.exists() or path.is_symlink(), "missing tracked file: " + key)
            require(inside(path.resolve(strict=True), root), "escaping symlink: " + key)
            info = path.lstat()
            if mode == "120000":
                require(stat.S_ISLNK(info.st_mode), "changed symlink: " + key)
                data = os.fsencode(os.readlink(path))
                digest = hashlib.sha1() if len(oid) == 40 else hashlib.sha256()
                digest.update(b"blob " + str(len(data)).encode() + b"\0" + data)
            else:
                require(stat.S_ISREG(info.st_mode), "non-regular tracked file: " + key)
                require(bool(info.st_mode & 0o111) == (mode == "100755"), "changed executable mode: " + key)
                digest = hashlib.sha1() if len(oid) == 40 else hashlib.sha256()
                digest.update(b"blob " + str(info.st_size).encode() + b"\0")
                with path.open("rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
            require(digest.hexdigest() == oid, "dirty tracked file: " + key)
            allowed.add(path)

    repository(root, "", snapshot["umbrella_commit"])
    require(observed == expected, "unexpected submodule pins")
    for directory, dirs, files in os.walk(root, followlinks=False, onerror=walk_error):
        for name in dirs + files:
            path = Path(directory) / name
            info = path.lstat()
            metadata = any(inside(path, item) for item in admin)
            if stat.S_ISLNK(info.st_mode):
                require(inside(path.resolve(strict=True), root), "escaping snapshot symlink: " + str(path))
            else:
                require(stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode), "special snapshot file")
            if metadata:
                require(name not in ("alternates", "http-alternates"), "external Git object store")
            elif not stat.S_ISDIR(info.st_mode):
                require(path in allowed, "untracked snapshot content: " + str(path))


def prepare():
    snapshot = request["snapshot"]
    requested_root = Path(snapshot["path"])
    base = Path(request["requested_base"])
    require(requested_root.parent == Path(str(base) + ".snapshots"),
            "snapshot location does not match requested base")
    # Resolve site aliases (e.g. a symlinked scratch area) once, never the checkout,
    # snapshots directory, or anything below it. Guards use only this stored
    # canonical path, even if the original site alias is later retargeted.
    parent = base.parent.resolve() / (base.name + ".snapshots")
    root = parent / requested_root.name
    snapshot["path"] = str(root)
    temporary = parent / (".preparing-" + root.name)
    safe_ancestry(parent)
    parent.mkdir(parents=True, exist_ok=True)
    safe_ancestry(parent)
    require(not os.path.lexists(root) and not os.path.lexists(temporary), "snapshot path already exists")
    git(None, "clone", "--quiet", "--no-checkout", "--no-local", "--", request["url"], str(temporary))
    git(temporary, "fetch", "--quiet", "--no-tags", "origin", request["snapshot"]["requested_ref"])
    resolved = git(temporary, "rev-parse", "--verify", "FETCH_HEAD^{commit}").decode().strip()
    git(temporary, "checkout", "--quiet", "--detach", resolved)
    snapshot["umbrella_commit"] = commit(temporary)
    modules = snapshot["policy"]["modules"]
    links = {name for name, mode, kind, oid in entries(temporary) if mode == "160000"}
    require(set(modules) <= links, "selected submodule is absent")
    if modules:                     # with no paths, "submodule update" would initialize every submodule
        git(temporary, "submodule", "update", "--quiet", "--init", "--recursive", "--", *modules)
    branch = snapshot["policy"]["branch"]
    for name in modules:
        repo = temporary / name
        if branch and name in request.get("branch_modules", []):
            git(repo, "fetch", "--quiet", "--no-tags", "origin", branch)
            target = git(repo, "rev-parse", "--verify", "FETCH_HEAD^{commit}").decode().strip()
            git(repo, "checkout", "--quiet", "--detach", target)
            git(repo, "submodule", "update", "--quiet", "--init", "--recursive")

    def collect(repo, relative):
        snapshot["submodules"][relative] = commit(repo)
        for name, mode, kind, oid in entries(repo):
            if mode == "160000":
                collect(repo / name, relative + "/" + name)

    for name in modules:
        collect(temporary / name, name)
    verify(snapshot, temporary)
    for directory, dirs, files in os.walk(temporary, topdown=False, followlinks=False, onerror=walk_error):
        for name in dirs + files:
            path = Path(directory) / name
            if not path.is_symlink():
                path.chmod(stat.S_IMODE(path.stat().st_mode) & ~0o222)
        path = Path(directory)
        path.chmod(stat.S_IMODE(path.stat().st_mode) & ~0o222)
    require(not os.path.lexists(root), "snapshot destination already exists")
    temporary.rename(root)
    verify(snapshot, root)
    print(json.dumps(snapshot, sort_keys=True))


if request["action"] == "prepare":
    prepare()
elif request["action"] == "verify":
    verify(request["snapshot"], Path(request["snapshot"]["path"]))
else:
    raise RuntimeError("unknown snapshot action")
'''


def _ref(value, label):
    if (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", value)
            or ".." in value or value.endswith(("/", ".", ".lock"))
            or any(part in ("", ".", "..") for part in value.split("/"))):
        raise ValueError(f"invalid snapshot {label}")
    return value


def _path(value):
    if (not isinstance(value, str) or not value.startswith("/") or value.startswith("//")
            or value == "/" or posixpath.normpath(value) != value
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise ValueError("snapshot path must be a normalized absolute path")
    return value


def _validate(snapshot, repo=None, *, resolved=True):
    if not isinstance(snapshot, dict) or set(snapshot) != {
            "path", "requested_ref", "umbrella_commit", "submodules", "policy"}:
        raise ValueError("invalid snapshot manifest")
    path = _path(snapshot["path"])
    if (not posixpath.dirname(path).endswith(".snapshots")
            or not re.fullmatch(r"[0-9a-f]{32}", posixpath.basename(path))):
        raise ValueError("invalid snapshot location")
    _ref(snapshot["requested_ref"], "ref")
    policy = snapshot["policy"]
    if not isinstance(policy, dict) or set(policy) != {"name", "modules", "branch"} or policy["name"] != _POLICY:
        raise ValueError("unsupported snapshot policy")
    modules = policy["modules"]
    if (not isinstance(modules, list) or any(not _module_name(m) for m in modules)
            or len(set(modules)) != len(modules)
            or (repo is not None and repo.require_modules and not modules)
            or (repo is not None and repo.modules is not None and not set(modules) <= set(repo.modules))):
        raise ValueError("invalid snapshot modules")
    if policy["branch"] != "":
        _ref(policy["branch"], "branch")
    pins = snapshot["submodules"]
    if not isinstance(pins, dict) or not set(modules) <= set(pins):
        raise ValueError("missing snapshot submodule pins")
    for name in pins:
        if (not isinstance(name, str) or name.split("/")[0] not in modules
                or any(p in ("", ".", "..", ".git") for p in name.split("/"))):
            raise ValueError("invalid submodule path")
    for value in [snapshot["umbrella_commit"], *pins.values()]:
        if not resolved and value == "":
            continue
        if (not isinstance(value, str) or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", value)
                or value == "0" * len(value)):
            raise ValueError("unresolved snapshot commit")
    return snapshot


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate snapshot JSON key")
        result[key] = value
    return result


def prepare_code(endpoint, base, ref, repo: SourceRepo, modules=None, branch="") -> dict:
    """Create a unique, retained snapshot, returning exact umbrella and recursive pins.

    `endpoint` needs only ``_sh(command, stdin=, timeout=)`` running Bash on the host that will see the snapshot.
    Only repo.branch_modules receive an optional branch override. No existing checkout or
    snapshot is modified or removed, including on failure or a lost reply.
    The endpoint resolves site aliases in the base's parent once; the returned
    manifest and subsequent guards retain the canonical snapshot location.
    """
    if not isinstance(repo, SourceRepo):
        raise ValueError("prepare_code needs a SourceRepo")
    if modules is None:
        modules = repo.default_modules
    base = _path(base)
    ref = _ref(ref, "ref")
    if branch != "":
        _ref(branch, "branch")
    if isinstance(modules, str):
        raise ValueError("modules must be a sequence of module names")
    modules = list(modules)
    if any(not isinstance(name, str) for name in modules):
        raise ValueError("modules must contain module names")
    snapshot = {
        "path": base + ".snapshots/" + uuid.uuid4().hex,
        "requested_ref": ref,
        "umbrella_commit": "",
        "submodules": {name: "" for name in modules},
        "policy": {"name": _POLICY, "modules": modules, "branch": branch},
    }
    _validate(snapshot, repo, resolved=False)
    request = {"action": "prepare", "url": repo.url, "requested_base": base,
               "snapshot": snapshot, "branch_modules": list(repo.branch_modules)}
    command = "set -e\npython3 -I - " + shlex.quote(json.dumps(request))
    raw = endpoint._sh(command, stdin=_REMOTE.encode(), timeout=900)
    try:
        result = _validate(json.loads(raw, object_pairs_hook=_unique_object), repo)
        # Only the endpoint can resolve its filesystem aliases. Accept a changed
        # parent prefix, but bind the acknowledgement to this snapshots directory
        # name and unique ID; _validate also requires a normalized absolute path.
        if (posixpath.basename(result["path"]) != posixpath.basename(snapshot["path"])
                or posixpath.basename(posixpath.dirname(result["path"])) !=
                posixpath.basename(posixpath.dirname(snapshot["path"]))
                or any(result[key] != snapshot[key] for key in ("requested_ref", "policy"))):
            raise ValueError("snapshot acknowledgement does not match request")
    except (ValueError, TypeError, KeyError) as exc:
        raise EndpointError(f"invalid source snapshot acknowledgement: {exc}") from exc
    return result


def code_guard(snapshot, repo: Optional[SourceRepo] = None) -> str:
    """Return a non-mutating Bash prefix; failures exit before subsequent runtime code.

    Requires Bash, Git and Python 3, not a remote hependpoints installation. This
    checks content at launch, not against an owner changing it after the check.
    Pass the SourceRepo that prepared the snapshot (its module rules and branch override set).
    """
    _validate(snapshot, repo)
    request = {"action": "verify", "snapshot": snapshot,
               "branch_modules": list(repo.branch_modules) if repo is not None else []}
    return ("python3 -I -c " + shlex.quote(_REMOTE) + " " + shlex.quote(json.dumps(request))
            + " || exit $?\n")
