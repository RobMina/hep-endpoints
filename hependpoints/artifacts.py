"""Durable execution bindings for streamed tasks: what code, image and input bytes a job was bound to.

A binding is execution evidence, separate from any measurement identity a caller keeps. See docs/artifact-bindings.md.
"""
from __future__ import annotations

import hashlib
import json
import re
import shlex
from pathlib import Path

from . import EndpointError
from .submission import durable_json, read_json, submission_lock

DEFAULT_OUT_VAR = "HEPEP_OUT"


FINGERPRINT = r'''
import hashlib, json, os, stat, sys
p = sys.argv[1]
with open(p, "rb") as f:
    before = os.fstat(f.fileno())
    if not stat.S_ISREG(before.st_mode):
        raise RuntimeError("artifact must be a regular file")
    md5, sha = hashlib.md5(), hashlib.sha256()
    for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
        md5.update(block)
        sha.update(block)
    after = os.fstat(f.fileno())
    current = os.stat(p)
    fields = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if fields(before) != fields(after) or fields(after) != fields(current):
        raise RuntimeError("artifact changed while hashing")
print(json.dumps(dict(md5=md5.hexdigest(), sha256=sha.hexdigest(), size=after.st_size)))
'''


def fingerprint_command(path):
    return f"python3 -c {shlex.quote(FINGERPRINT)} {shlex.quote(path)}"


def parse_fingerprint(text):
    try:
        value = json.loads(text)
        if (not isinstance(value, dict) or not re.fullmatch(r"[0-9a-f]{32}", value["md5"])
                or not re.fullmatch(r"[0-9a-f]{64}", value["sha256"])
                or type(value["size"]) is not int or value["size"] < 0):
            raise ValueError("invalid fingerprint")
        return {k: value[k] for k in ("md5", "sha256", "size")}
    except (ValueError, TypeError, KeyError) as exc:
        raise EndpointError("invalid artifact fingerprint") from exc


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def resolve_inputs(source, mapping):
    """Pin expected bytes, not a source snapshot; later transfers must match or fail."""
    files = []
    for src, dest in sorted(mapping.items()):
        if not isinstance(dest, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", dest):
            raise ValueError(f"unsafe input destination {dest!r}")
        fp = parse_fingerprint(json.dumps(source.fingerprint(src)))
        files.append({"src": src, "dest": dest, **fp, "argv": source.argv(src)})
    if len({f["dest"] for f in files}) != len(files):
        raise ValueError("input destinations must be unique")
    return {"source": {"host": source.host, "root": source.root}, "files": files,
            "policy": "pinned-expectations; verified-staging; source-not-snapshotted"}


def input_digest(inputs):
    return digest(sorted(({k: f[k] for k in ("dest", "sha256", "size")} for f in inputs["files"]),
                         key=lambda f: f["dest"]))


def resolve_image(ep, sif):
    fp = parse_fingerprint(ep._sh(fingerprint_command(sif), timeout=900))
    return {"path": sif, "sha256": fp["sha256"], "size": fp["size"],
            "policy": "verify-before-run-not-snapshot"}


def make_manifest(task, code, image, inputs, params, geometry=None):
    manifest = {"schema_version": 1, "identity_role": "execution-evidence-not-board-measurement-id",
                "task": task, "code": code, "image": image, "inputs": inputs,
                "input_sha": input_digest(inputs), "resolved_params": params,
                "coverage": {"code": "retained-read-only-snapshot",
                             "streamed_inputs": "pinned-and-verified-before-ready; no-source-snapshot",
                             "image": "verified-before-run; no-lifetime-snapshot",
                             "downloaded_dependencies": "unresolved", "full_immutability": False}}
    if geometry is not None:
        manifest["geometry"] = geometry
    return {**manifest, "manifest_sha": digest(manifest)}


def validate_manifest(manifest):
    try:
        payload = {k: v for k, v in manifest.items() if k != "manifest_sha"}
        if manifest["schema_version"] != 1 or digest(payload) != manifest["manifest_sha"]:
            raise ValueError("manifest digest")
        if input_digest(manifest["inputs"]) != manifest["input_sha"]:
            raise ValueError("input digest")
    except (TypeError, KeyError, ValueError, AttributeError) as exc:
        raise ValueError("invalid artifact manifest") from exc
    return manifest


def runtime_prefix(manifest, guard=None, out_var=DEFAULT_OUT_VAR):
    """Guard the runtime and emit the exact admission manifest, without claiming a snapshot of the image.

    `guard(code_manifest) -> bash` checks the code (default: snapshots.code_guard without a SourceRepo; pass a
    function bound to the experiment's SourceRepo when it uses branch overrides). The manifest is written to
    ``$<out_var>/artifact-manifest.json`` (the endpoint's JobEnvironment.out_var)."""
    if guard is None:
        from . import snapshots
        guard = snapshots.code_guard
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", out_var):
        raise ValueError(f"bad output variable {out_var!r}")
    validate_manifest(manifest)
    image = manifest["image"]
    script = '''import hashlib, os, sys
p, want, size = sys.argv[1:]
h = hashlib.sha256()
with open(p, "rb") as f:
    for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
        h.update(block)
    good = os.fstat(f.fileno()).st_size == int(size) and h.hexdigest() == want
if not good:
    raise SystemExit("runtime image changed since submission")
'''
    args = " ".join(shlex.quote(str(v)) for v in (image["path"], image["sha256"], image["size"]))
    data = shlex.quote(json.dumps(manifest, sort_keys=True, allow_nan=False))
    return (guard(manifest["code"]) + f"\npython3 -c {shlex.quote(script)} {args} || exit 5\n"
            + f"printf '%s\\n' {data} > \"${out_var}/artifact-manifest.json\" || exit 5\n")


def verify_result(out, manifest):
    validate_manifest(manifest)
    try:
        actual = read_json(Path(out) / "artifact-manifest.json")
    except EndpointError as exc:
        raise ValueError("unreadable result artifact manifest") from exc
    if actual != manifest:
        raise ValueError("result artifact manifest differs from submission manifest")


def cache_command(tmp, entries, shell_path=False):
    """Build a verifier shared by the staging controller and the consuming job."""
    payload = [{k: f[k] for k in ("dest", "sha256", "md5", "size")} for f in entries]
    script = r'''
import hashlib, json, os, stat, sys
root, entries = sys.argv[1], json.loads(sys.argv[2])
for e in entries:
    p = os.path.join(root, e["dest"])
    s = os.lstat(p)
    if not stat.S_ISREG(s.st_mode) or s.st_size != e["size"]:
        raise SystemExit("staged artifact type or size mismatch")
    sha, md5 = hashlib.sha256(), hashlib.md5()
    with open(p, "rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            sha.update(block)
            md5.update(block)
    if sha.hexdigest() != e["sha256"] or md5.hexdigest() != e["md5"]:
        raise SystemExit("staged artifact digest mismatch")
print("VERIFIED")
'''
    location = tmp if shell_path else shlex.quote(tmp)
    return f"python3 -c {shlex.quote(script)} {location} {shlex.quote(json.dumps(payload))}"


def verify_cache(ep, name, host, tmp, entries):
    """Rehash staged bytes, including ready caches; a marker is not content evidence."""
    return ep.run_on_node(name, host, cache_command(tmp, entries), timeout=900).strip() == "VERIFIED"


def consumption_guard(shell_path, entries):
    return cache_command(shell_path, entries, shell_path=True) + " >/dev/null || exit 3\n"


def target_identity(ep, source):
    return {"endpoint": ep.name, **{k: getattr(ep, k, "") for k in ("host", "account", "partition", "workdir")},
            "source_host": source.host, "source_root": source.root}


def _binding_digest(record):
    return digest({k: record[k] for k in ("params", "target", "manifest", "body", "resources")})


def checked_record(path, allow_legacy=False):
    record = read_json(Path(path))
    if record is None:
        if Path(path).exists():
            raise ValueError("invalid persisted artifact binding: null record")
        return None
    if not isinstance(record, dict):
        raise ValueError("invalid persisted artifact binding: expected an object")
    if "manifest" not in record:
        if allow_legacy and isinstance(record.get("params"), dict):
            return record
        raise ValueError("legacy task record has no pinned artifact binding; investigate rather than resubmit")
    try:
        validate_manifest(record["manifest"])
        if _binding_digest(record) != record["binding_sha"]:
            raise ValueError("binding digest")
    except (TypeError, KeyError, ValueError) as exc:
        raise ValueError("invalid persisted artifact binding") from exc
    return record


def _default_identity(artifact):
    return None if artifact is None else digest(artifact)


def binding(directory, name, params, target, prepare, geometry=None, identity=None, label="binding"):
    """Persist one original rendering before endpoint dispatch, under a cooperating-process lock.

    `geometry` is an optional extra artifact bound into the manifest; `identity(artifact)` reduces it to what must
    match on retry (default: its canonical digest). `label` prefixes refusal messages."""
    identity = identity or _default_identity
    directory = Path(directory)
    with submission_lock(directory, name):
        path = directory / f"{name}.json"
        old = checked_record(path)
        if old is not None:
            if old["params"] != params:
                raise ValueError(f"{label}: {name!r} was already submitted with different params")
            if old["target"] != target:
                raise ValueError(f"{label}: {name!r} artifact binding belongs to a different target/source")
            if identity(old["manifest"].get("geometry")) != identity(geometry):
                raise ValueError(f"{label}: {name!r} geometry differs from the bound request")
            return old
        record = {**prepare(), "params": params, "target": target}
        validate_manifest(record["manifest"])
        record["binding_sha"] = _binding_digest(record)
        durable_json(path, record)
        return record
