"""Artifact contracts using temporary files and local shell commands, never a live endpoint."""
import hashlib
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from unittest.mock import Mock

import pytest

from hependpoints import EndpointError, Resources
from hependpoints import artifacts as A


class Local:
    name = "test"

    def _sh(self, cmd, timeout=900):
        r = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=timeout)
        if r.returncode:
            raise EndpointError(r.stderr)
        return r.stdout

    def run_on_node(self, name, host, cmd, timeout=900):
        return self._sh(cmd, timeout)


class Source:
    host = "fixture"

    def __init__(self, root):
        self.root = str(root)

    def argv(self, src):
        return ["cat", str(Path(self.root) / src)]

    def fingerprint(self, src):
        return A.parse_fingerprint(Local()._sh(A.fingerprint_command(str(Path(self.root) / src))))


def manifest(tmp_path):
    (tmp_path / "data").write_bytes(b"input")
    (tmp_path / "image").write_bytes(b"image")
    inputs = A.resolve_inputs(Source(tmp_path), {"data": "input"})
    return A.make_manifest("test", {"path": "/snapshot", "umbrella_commit": "a" * 40},
                           A.resolve_image(Local(), str(tmp_path / "image")), inputs, {"seed": 1})


def test_content_identity_not_path_identity(tmp_path):
    m = manifest(tmp_path)
    (tmp_path / "other").write_bytes(b"input")
    other = A.resolve_inputs(Source(tmp_path), {"other": "input"})
    assert A.input_digest(other) == m["input_sha"]
    (tmp_path / "data").write_bytes(b"new")
    new = A.resolve_inputs(Source(tmp_path), {"data": "input"})
    assert A.input_digest(new) != m["input_sha"]
    assert m["inputs"]["files"][0]["sha256"] == hashlib.sha256(b"input").hexdigest()
    assert m["coverage"]["full_immutability"] is False


def test_cache_checks_contents_not_ready(tmp_path):
    m = manifest(tmp_path)
    (tmp_path / "input").write_bytes(b"input")
    (tmp_path / "ready").touch()
    assert A.verify_cache(Local(), "p", "n", str(tmp_path), m["inputs"]["files"])
    (tmp_path / "input").write_bytes(b"other")
    with pytest.raises(EndpointError):
        A.verify_cache(Local(), "p", "n", str(tmp_path), m["inputs"]["files"])
    (tmp_path / "input").unlink()
    (tmp_path / "input").symlink_to(tmp_path / "data")
    with pytest.raises(EndpointError):
        A.verify_cache(Local(), "p", "n", str(tmp_path), m["inputs"]["files"])


def test_binding_freezes_body_and_manifest_on_retry(tmp_path):
    m = manifest(tmp_path)
    prepare = Mock(return_value={"manifest": m, "body": "original", "resources": asdict(Resources())})
    root = tmp_path / "state"
    first = A.binding(root, "p", {"seed": 1}, {"endpoint": "fixture"}, prepare)
    prepare.return_value = {"manifest": {}, "body": "different", "resources": {}}
    second = A.binding(root, "p", {"seed": 1}, {"endpoint": "fixture"}, prepare)
    assert first == second and prepare.call_count == 1
    for p, t in (({"seed": 2}, {"endpoint": "fixture"}), ({"seed": 1}, {"endpoint": "other"})):
        with pytest.raises(ValueError, match="different"):
            A.binding(root, "p", p, t, prepare)
    path = root / "p.json"
    damaged = json.loads(path.read_text())
    damaged["body"] = "unbound"
    path.write_text(json.dumps(damaged))
    with pytest.raises(ValueError, match="binding"):
        A.binding(root, "p", {"seed": 1}, {"endpoint": "fixture"}, prepare)


def test_concurrent_binding_prepares_once(tmp_path):
    m = manifest(tmp_path)
    prepare = Mock(return_value={"manifest": m, "body": "body", "resources": {}})
    def submit(_):
        return A.binding(tmp_path / "state", "p", {}, {}, prepare)
    with ThreadPoolExecutor(max_workers=4) as pool:
        result = list(pool.map(submit, range(8)))
    assert prepare.call_count == 1 and all(r == result[0] for r in result)


def test_legacy_and_corrupt_records_fail_closed(tmp_path):
    path = tmp_path / "p.json"
    path.write_text('{"params": {}}')
    with pytest.raises(ValueError, match="legacy"):
        A.checked_record(path)
    path.write_text("{")
    with pytest.raises(EndpointError):
        A.checked_record(path)


def test_result_manifest_must_match_exactly(tmp_path):
    m = manifest(tmp_path)
    with pytest.raises(ValueError, match="differs"):
        A.verify_result(tmp_path, m)
    p = tmp_path / "artifact-manifest.json"
    p.write_text(json.dumps(m))
    A.verify_result(tmp_path, m)
    changed = dict(m, manifest_sha="0" * 64)
    p.write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="differs"):
        A.verify_result(tmp_path, m)


def test_image_changed_behind_path_fails_before_runtime(tmp_path, monkeypatch):
    from hependpoints import snapshots
    monkeypatch.setattr(snapshots, "code_guard", lambda _: "true\n")
    m = manifest(tmp_path)
    out = tmp_path / "out"
    out.mkdir()
    body = f"export HEPEP_OUT={out}\n" + A.runtime_prefix(m)
    Local()._sh(body)
    A.verify_result(out, m)
    (tmp_path / "image").write_bytes(b"other")
    with pytest.raises(EndpointError, match="image changed"):
        Local()._sh(body)


@pytest.mark.parametrize("value", ["{}", "null", "[]", '{"md5":"x","sha256":"y","size":1}',
                                   json.dumps({"md5": "a" * 32, "sha256": "b" * 64, "size": True})])
def test_invalid_fingerprints_fail_closed(value):
    with pytest.raises(EndpointError):
        A.parse_fingerprint(value)


def test_bad_destination_and_collisions(tmp_path):
    manifest(tmp_path)
    with pytest.raises(ValueError, match="destination"):
        A.resolve_inputs(Source(tmp_path), {"data": "../escape"})
    with pytest.raises(ValueError, match="unique"):
        A.resolve_inputs(Source(tmp_path), {"data": "same", "image": "same"})


@pytest.mark.parametrize("value", ["null", "[]", "4", "true"])
def test_invalid_json_state_is_not_absence(tmp_path, value):
    p = tmp_path / "p.json"
    p.write_text(value)
    with pytest.raises(ValueError, match="binding"):
        A.checked_record(p)


def test_legacy_read_only_lifecycle_keeps_explicit_unverified_record(tmp_path):
    p = tmp_path / "p.json"
    legacy = {"params": {"seed": 1}, "code_ref": "main"}
    p.write_text(json.dumps(legacy))
    assert A.checked_record(p, allow_legacy=True) == legacy
    with pytest.raises(ValueError, match="legacy"):
        A.binding(tmp_path, "p", legacy["params"], {}, lambda: {})


def test_consumption_guard_refuses_corrupt_ready_cache(tmp_path):
    m = manifest(tmp_path)
    (tmp_path / "ready").touch()
    (tmp_path / "input").write_bytes(b"wrong")
    body = A.consumption_guard(str(tmp_path), m["inputs"]["files"]) + "echo PHYSICS_RAN"
    result = subprocess.run(["bash", "-c", body], capture_output=True, text=True)
    assert result.returncode == 3 and "PHYSICS_RAN" not in result.stdout


def test_runtime_prefix_uses_the_endpoint_output_variable_and_guard(tmp_path):
    m = manifest(tmp_path)
    out = tmp_path / "custom"
    out.mkdir()
    seen = []
    body = f"export MYEXP_OUT={out}\n" + A.runtime_prefix(m, guard=lambda code: seen.append(code) or "true\n",
                                                          out_var="MYEXP_OUT")
    Local()._sh(body)
    A.verify_result(out, m)
    assert seen == [m["code"]]
    with pytest.raises(ValueError):
        A.runtime_prefix(m, guard=lambda code: "true\n", out_var="BAD; rm")


def test_binding_identity_hook_and_label(tmp_path):
    m = manifest(tmp_path)
    geometry = {"reference": {"sha256": "a" * 64, "metadata": {"name": "g"}}, "path": "/somewhere"}
    m = A.make_manifest("test", m["code"], m["image"], m["inputs"], {"seed": 1}, geometry=geometry)
    prepare = Mock(return_value={"manifest": m, "body": "b", "resources": {}})
    ident = lambda g: None if g is None else g["reference"]["sha256"]   # noqa: E731
    A.binding(tmp_path / "s", "p", {}, {}, prepare, geometry=geometry, identity=ident, label="mykit")
    moved = dict(geometry, path="/elsewhere")                      # same identity: accepted
    assert A.binding(tmp_path / "s", "p", {}, {}, prepare, geometry=moved, identity=ident)["body"] == "b"
    with pytest.raises(ValueError, match="mykit: 'p' geometry differs"):
        A.binding(tmp_path / "s", "p", {}, {}, prepare, geometry={"reference": {"sha256": "b" * 64}},
                  identity=ident, label="mykit")
    with pytest.raises(ValueError, match="geometry differs"):            # default identity: the whole artifact
        A.binding(tmp_path / "s", "p", {}, {}, prepare, geometry=moved)
