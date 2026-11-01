"""Snapshot/guard tests use local fixture repositories, never SSH or a network."""
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

from hependpoints import EndpointError
from hependpoints import snapshots

MODULES = ("core", "sim", "reco")
URL = "/nonexistent/umbrella"     # replaced by the repositories fixture


def repo(**overrides):
    """The fixture umbrella's description, as an experiment package would declare it."""
    kw = dict(url=URL, modules=MODULES, default_modules=MODULES, branch_modules=("core", "sim"),
              require_modules=True)
    kw.update(overrides)
    return snapshots.SourceRepo(**kw)


def prep(endpoint, base, ref, **kwargs):
    return snapshots.prepare_code(endpoint, base, ref, repo(), **kwargs)


def git(repo, *args):
    env = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null")
    return subprocess.check_output(
        ["git", "-c", "protocol.file.allow=always", "-C", str(repo), *args], env=env,
        stderr=subprocess.PIPE).decode().strip()


def init(path):
    path.mkdir()
    git(path, "init", "-q", "-b", "main")
    git(path, "config", "user.name", "Snapshot Test")
    git(path, "config", "user.email", "snapshots@example.invalid")
    (path / "source.py").write_text("VERSION = 'A'\n")
    (path / ".gitignore").write_text("*.ignored\n")
    git(path, "add", ".")
    git(path, "commit", "-qm", "initial")
    return path


class LocalEndpoint:
    def __init__(self):
        self.calls = []

    def _sh(self, command, stdin=None, timeout=None):
        self.calls.append((command, stdin, timeout))
        result = subprocess.run(["bash", "-c", command], input=stdin, capture_output=True, timeout=timeout)
        if result.returncode:
            raise EndpointError(result.stderr.decode())
        return result.stdout.decode()


@pytest.fixture
def repositories(tmp_path, monkeypatch):
    nested = init(tmp_path / "nested")
    modules = {}
    for name in MODULES:
        repo = init(tmp_path / name)
        if name == "core":
            git(repo, "submodule", "add", "-q", str(nested), "nested")
            git(repo, "commit", "-qam", "nested")
        git(repo, "checkout", "-qb", "experiment")
        (repo / "source.py").write_text("VERSION = 'experiment'\n")
        git(repo, "commit", "-qam", "experiment")
        git(repo, "checkout", "-q", "main")
        modules[name] = repo
    umbrella = init(tmp_path / "umbrella")
    for name, repo in modules.items():
        git(umbrella, "submodule", "add", "-q", str(repo), name)
    git(umbrella, "commit", "-qam", "modules")
    git(umbrella, "tag", "release-a")
    monkeypatch.setattr(sys.modules[__name__], "URL", str(umbrella))
    return umbrella, modules


def prepare(tmp_path, ref="main", **kwargs):
    return prep(LocalEndpoint(), str(tmp_path / "shared checkout"), ref, **kwargs)


def guard(snapshot):
    return subprocess.run(["bash", "-c", snapshots.code_guard(snapshot, repo()) + "printf runtime-reached"],
                          capture_output=True, text=True)


def writable(path):
    path.chmod(stat.S_IMODE(path.stat().st_mode) | 0o200)


def inventory(path):
    return {str(p.relative_to(path)): (p.lstat().st_mode, p.read_bytes())
            for p in path.rglob("*") if p.is_file() and not p.is_symlink()}


def test_snapshots_pin_exact_commits_and_survive_ref_changes(tmp_path, repositories):
    umbrella, modules = repositories
    shared = tmp_path / "shared checkout"
    shared.mkdir()
    (shared / "untouched").write_text("do not modify")
    a = prepare(tmp_path)
    a_tree = inventory(Path(a["path"]))
    assert a["umbrella_commit"] == git(umbrella, "rev-parse", "HEAD")
    assert a["submodules"]["core/nested"] == git(modules["core"] / "nested", "rev-parse", "HEAD")
    assert a["requested_ref"] == "main"
    assert all(not mode & 0o222 for mode, data in a_tree.values())
    assert guard(a).stdout == "runtime-reached"
    (modules["core"] / "source.py").write_text("VERSION = 'B'\n")
    git(modules["core"], "commit", "-qam", "B")
    git(umbrella / "core", "fetch", "-q", "origin", "main")
    git(umbrella / "core", "checkout", "-q", "--detach", "FETCH_HEAD")
    git(umbrella, "commit", "-qam", "B pin")
    b = prepare(tmp_path)
    assert a["path"] != b["path"]
    assert a["umbrella_commit"] != b["umbrella_commit"]
    assert a["submodules"]["core"] != b["submodules"]["core"]
    assert inventory(Path(a["path"])) == a_tree
    assert guard(a).stdout == "runtime-reached"
    assert guard(b).stdout == "runtime-reached"
    assert (shared / "untouched").read_text() == "do not modify"
    assert set(shared.iterdir()) == {shared / "untouched"}
    assert set(Path(a["path"]).parent.iterdir()) == {Path(a["path"]), Path(b["path"])}


@pytest.mark.parametrize("ref_kind", ["tag", "sha", "qualified"])
def test_explicit_tag_sha_and_qualified_refs(tmp_path, repositories, ref_kind):
    umbrella, _ = repositories
    ref = {"tag": "release-a", "sha": git(umbrella, "rev-parse", "HEAD"),
           "qualified": "refs/heads/main"}[ref_kind]
    snapshot = prepare(tmp_path, ref, modules=("core",))
    assert snapshot["requested_ref"] == ref
    assert snapshot["umbrella_commit"] == git(umbrella, "rev-parse", "HEAD")
    assert set(snapshot["submodules"]) == {"core", "core/nested"}
    assert guard(snapshot).returncode == 0


def test_branch_override_only_changes_branch_modules(tmp_path, repositories):
    umbrella, modules = repositories
    snapshot = prepare(tmp_path, "release-a", branch="experiment")
    for name in ("core", "sim"):
        assert snapshot["submodules"][name] == git(modules[name], "rev-parse", "experiment")
    assert snapshot["submodules"]["reco"] == git(modules["reco"], "rev-parse", "main")
    assert snapshot["umbrella_commit"] == git(umbrella, "rev-parse", "release-a")
    assert snapshot["policy"]["branch"] == "experiment"
    assert guard(snapshot).stdout == "runtime-reached"


def test_failure_retains_existing_snapshots_and_shared_checkout(tmp_path, repositories):
    a = prepare(tmp_path)
    original = inventory(Path(a["path"]))
    shared = tmp_path / "shared checkout"
    shared.mkdir()
    (shared / "local").write_text("private existing content")
    before = inventory(shared)
    with pytest.raises(EndpointError):
        prepare(tmp_path, "does-not-exist")
    assert inventory(Path(a["path"])) == original
    assert inventory(shared) == before
    assert guard(a).returncode == 0
    assert any(p.name.startswith(".preparing-") for p in Path(a["path"]).parent.iterdir())


@pytest.mark.parametrize("change", ["tracked", "ignored", "untracked", "missing", "submodule",
                                    "index", "assume-unchanged", "symlink", "missing-root"])
def test_guard_rejects_tampering_before_runtime(tmp_path, repositories, change):
    snapshot = prepare(tmp_path)
    root = Path(snapshot["path"])
    source = root / "source.py"
    if change == "tracked":
        writable(source)
        source.write_text("tampered")
    elif change in ("untracked", "ignored"):
        writable(root)
        (root / ("new.ignored" if change == "ignored" else "new.py")).write_text("tampered")
    elif change == "missing":
        writable(root)
        source.unlink()
    elif change == "submodule":
        source = root / "core" / "source.py"
        writable(source)
        source.write_text("tampered")
    elif change == "index":
        writable(root / ".git")
        writable(root / ".git" / "index")
        git(root, "update-index", "--force-remove", "source.py")
    elif change == "assume-unchanged":
        writable(root / ".git")
        writable(root / ".git" / "index")
        git(root, "update-index", "--assume-unchanged", "source.py")
        writable(source)
        source.write_text("tampered")
    elif change == "symlink":
        writable(root)
        (root / "escape").symlink_to(tmp_path)
    else:
        root.rename(root.with_name("hidden"))
    result = guard(snapshot)
    assert result.returncode != 0
    assert "runtime-reached" not in result.stdout


def test_prepare_rejects_escaping_tracked_symlink(tmp_path, repositories):
    umbrella, _ = repositories
    (umbrella / "escape").symlink_to(tmp_path)
    git(umbrella, "add", "escape")
    git(umbrella, "commit", "-qm", "unsafe symlink")
    with pytest.raises(EndpointError, match="escaping symlink"):
        prepare(tmp_path)
    parent = tmp_path / "shared checkout.snapshots"
    assert all(p.name.startswith(".preparing-") for p in parent.iterdir())


def test_internal_symlink_and_shell_metacharacters_in_base(tmp_path, repositories):
    umbrella, _ = repositories
    (umbrella / "alias.py").symlink_to("source.py")
    git(umbrella, "add", "alias.py")
    git(umbrella, "commit", "-qm", "internal symlink")
    base = str(tmp_path / "checkout ' ; $(touch SENTINEL)")
    snapshot = prep(LocalEndpoint(), base, "main")
    assert guard(snapshot).stdout == "runtime-reached"
    assert not (tmp_path / "SENTINEL").exists()


def test_symlink_snapshot_parent_is_refused(tmp_path, repositories):
    destination = tmp_path / "elsewhere"
    destination.mkdir()
    (tmp_path / "shared checkout.snapshots").symlink_to(destination, target_is_directory=True)
    with pytest.raises(EndpointError, match="symlink in snapshot ancestry"):
        prepare(tmp_path)
    assert not list(destination.iterdir())


@pytest.mark.parametrize("bad_ref", ["-main", "main;touch sentinel", "$(id)", "../main", "", "main\n", "a//b"])
def test_bad_refs_fail_before_endpoint_use(tmp_path, bad_ref):
    ep = LocalEndpoint()
    with pytest.raises(ValueError):
        prep(ep, str(tmp_path / "base"), bad_ref)
    assert ep.calls == []


@pytest.mark.parametrize("base", ["relative", "/", "/srv/../base", "/srv/base/", "/srv/a\nb", "//srv/base"])
def test_bad_paths_fail_before_endpoint_use(base):
    ep = LocalEndpoint()
    with pytest.raises(ValueError):
        prep(ep, base, "main")
    assert ep.calls == []


@pytest.mark.parametrize("alter", ["junk", "empty", "sha", "zero", "path", "path-id",
                                  "path-directory", "policy", "modules", "duplicate"])
def test_bad_acknowledgements_fail_closed(tmp_path, repositories, alter):
    class BadEndpoint(LocalEndpoint):
        def _sh(self, command, stdin=None, timeout=None):
            raw = super()._sh(command, stdin, timeout)
            if alter == "junk":
                return raw + "not json"
            if alter == "empty":
                return "{}"
            if alter == "duplicate":
                return raw.rstrip()[:-1] + ',"requested_ref":"main"}'
            value = json.loads(raw)
            if alter == "sha":
                value["umbrella_commit"] = "main"
            elif alter == "zero":
                value["umbrella_commit"] = "0" * 40
            elif alter == "path":
                value["path"] = str(tmp_path / "elsewhere")
            elif alter == "path-id":
                path = Path(value["path"])
                value["path"] = str(path.with_name("0" * 32 if path.name != "0" * 32 else "1" * 32))
            elif alter == "path-directory":
                path = Path(value["path"])
                value["path"] = str(path.parent.with_name("other.snapshots") / path.name)
            elif alter == "policy":
                value["policy"]["branch"] = "experiment"
            else:
                value["submodules"].pop("core")
            return json.dumps(value)

    with pytest.raises(EndpointError, match="invalid source snapshot acknowledgement"):
        prep(BadEndpoint(), str(tmp_path / "shared checkout"), "main")
    published = list((tmp_path / "shared checkout.snapshots").iterdir())
    assert len(published) == 1
    assert not published[0].name.startswith(".preparing-")


def test_guard_rejects_bad_manifest_without_endpoint():
    with pytest.raises(ValueError):
        snapshots.code_guard({"path": "/srv/not-a-snapshot"}, repo())


@pytest.mark.parametrize("modules", [(), ("beam",), ("core", "core"), "core", ({"core": 1},)])
def test_bad_modules_fail_before_endpoint_use(tmp_path, modules):
    ep = LocalEndpoint()
    with pytest.raises(ValueError):
        prep(ep, str(tmp_path / "base"), "main", modules=modules)
    assert not ep.calls


def test_failed_branch_override_does_not_publish_or_change_old_snapshot(tmp_path, repositories):
    original = prepare(tmp_path)
    before = inventory(Path(original["path"]))
    with pytest.raises(EndpointError):
        prepare(tmp_path, branch="absent-branch")
    assert inventory(Path(original["path"])) == before
    entries = list(Path(original["path"]).parent.iterdir())
    assert [p for p in entries if not p.name.startswith(".preparing-")] == [Path(original["path"])]


def test_guard_rejects_changed_head_and_external_git_metadata(tmp_path, repositories):
    snapshot = prepare(tmp_path)
    root = Path(snapshot["path"])
    head = root / ".git" / "HEAD"
    writable(head)
    head.write_text(git(repositories[0], "rev-parse", "HEAD~1") + "\n")
    assert guard(snapshot).returncode != 0
    head.write_text(snapshot["umbrella_commit"] + "\n")
    assert guard(snapshot).returncode == 0
    dotgit = root / "core" / ".git"
    writable(dotgit)
    dotgit.write_text("gitdir: " + str(repositories[1]["core"] / ".git") + "\n")
    assert guard(snapshot).returncode != 0


def test_git_environment_cannot_redirect_preparation(tmp_path, repositories, monkeypatch):
    monkeypatch.setenv("GIT_DIR", str(repositories[0] / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(repositories[0]))
    snapshot = prepare(tmp_path)
    assert guard(snapshot).returncode == 0


def test_guard_does_not_change_files_or_modes(tmp_path, repositories):
    snapshot = prepare(tmp_path)
    before = inventory(Path(snapshot["path"]))
    assert guard(snapshot).returncode == 0
    assert inventory(Path(snapshot["path"])) == before


@pytest.mark.parametrize("nested", [False, True])
def test_site_alias_is_canonicalized_once_and_retargeting_is_ignored(tmp_path, repositories, nested):
    physical = tmp_path / "physical"
    physical.mkdir()
    replacement = tmp_path / "replacement"
    replacement.mkdir()
    alias = tmp_path / "scratch"
    alias.symlink_to(physical, target_is_directory=True)
    requested_parent = alias / "user" if nested else alias
    # Exercise nonexistent parent creation below the site alias as well.
    base = requested_parent / "shared checkout"
    endpoint = LocalEndpoint()
    snapshot = prep(endpoint, str(base), "main", modules=("core",))
    expected_parent = physical / "user" if nested else physical
    assert Path(snapshot["path"]).parent == expected_parent.resolve() / "shared checkout.snapshots"
    assert '"requested_base":' in endpoint.calls[0][0]
    original_path = snapshot["path"]
    before = inventory(Path(original_path))
    assert guard(snapshot).stdout == "runtime-reached"
    alias.unlink()
    alias.symlink_to(replacement, target_is_directory=True)
    assert snapshot["path"] == original_path
    assert guard(snapshot).stdout == "runtime-reached"
    assert inventory(Path(original_path)) == before
    assert not list(replacement.iterdir())


def test_prepare_resolves_only_checkout_parent(tmp_path, repositories):
    existing = tmp_path / "existing checkout"
    existing.mkdir()
    (existing / "untouched").write_text("private checkout")
    base = tmp_path / "checkout alias"
    base.symlink_to(existing, target_is_directory=True)
    snapshot = prep(LocalEndpoint(), str(base), "main", modules=("core",))
    assert Path(snapshot["path"]).parent == tmp_path.resolve() / "checkout alias.snapshots"
    assert (existing / "untouched").read_text() == "private checkout"
    assert list(existing.iterdir()) == [existing / "untouched"]
    assert guard(snapshot).returncode == 0


@pytest.mark.parametrize("dangling", [False, True])
def test_site_alias_does_not_allow_symlink_snapshot_parent(tmp_path, repositories, dangling):
    physical = tmp_path / "physical"
    physical.mkdir()
    alias = tmp_path / "scratch"
    alias.symlink_to(physical, target_is_directory=True)
    destination = tmp_path / "elsewhere"
    if not dangling:
        destination.mkdir()
    (physical / "shared checkout.snapshots").symlink_to(destination, target_is_directory=True)
    with pytest.raises(EndpointError, match="symlink in snapshot ancestry"):
        prep(LocalEndpoint(), str(alias / "shared checkout"), "main")
    assert not destination.exists() if dangling else not list(destination.iterdir())


@pytest.mark.parametrize("location", ["snapshot", "snapshots-parent", "canonical-ancestor"])
def test_guard_rejects_symlink_replacing_canonical_ancestry(tmp_path, repositories, location):
    physical = tmp_path / "physical"
    physical.mkdir()
    alias = tmp_path / "scratch"
    alias.symlink_to(physical, target_is_directory=True)
    snapshot = prep(LocalEndpoint(), str(alias / "checkout"), "main", modules=("core",))
    root = Path(snapshot["path"])
    path = {"snapshot": root, "snapshots-parent": root.parent,
            "canonical-ancestor": physical}[location]
    moved = path.with_name(path.name + "-moved")
    path.rename(moved)
    path.symlink_to(moved, target_is_directory=True)
    result = guard(snapshot)
    assert result.returncode != 0
    assert "symlink in snapshot ancestry" in result.stderr
    assert "runtime-reached" not in result.stdout


def test_branch_override_needs_the_preparing_repo_description(tmp_path, repositories):
    """The override set is configuration: without it, an overridden snapshot fails verification (fail closed)."""
    snapshot = prepare(tmp_path, "release-a", branch="experiment")
    assert guard(snapshot).returncode == 0
    bare = subprocess.run(["bash", "-c", snapshots.code_guard(snapshot) + "printf runtime-reached"],
                          capture_output=True, text=True)
    assert bare.returncode != 0 and "runtime-reached" not in bare.stdout


def test_single_repository_without_submodules(tmp_path, repositories):
    """A repository snapshot with no module selection leaves every submodule uninitialized and still verifies."""
    single = snapshots.SourceRepo(url=URL)
    snapshot = snapshots.prepare_code(LocalEndpoint(), str(tmp_path / "checkout"), "main", single)
    assert snapshot["submodules"] == {} and snapshot["policy"]["modules"] == []
    root = Path(snapshot["path"])
    assert all(not any((root / name).iterdir()) for name in MODULES)
    ok = subprocess.run(["bash", "-c", snapshots.code_guard(snapshot, single) + "printf runtime-reached"],
                        capture_output=True, text=True)
    assert ok.stdout == "runtime-reached"


def test_source_repo_validation():
    with pytest.raises(ValueError):
        snapshots.SourceRepo(url="x", modules="core")
    with pytest.raises(ValueError):
        snapshots.SourceRepo(url="x", branch_modules=("../up",))
    with pytest.raises(ValueError):
        snapshots.prepare_code(LocalEndpoint(), "/srv/base", "main", "not-a-repo")
