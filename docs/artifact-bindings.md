# Artifact bindings and retained code snapshots

`hependpoints.artifacts` and `hependpoints.snapshots` let a task renderer bind a streamed job to the exact code,
runtime image and input bytes it was admitted with, before the job is dispatched. They extend the endpoint
submission fence; they do not define any measurement identity of the caller. No live rollout or physics
validation is implied by the fixture tests.

## What is pinned

After parameter/resource admission, a new handle resolves:

- A fresh public-code clone under `<configured-code>.snapshots/<uuid>`, detached at the exact requested umbrella
  commit with the selected recursive submodule commits (`prepare_code(endpoint, base, ref, SourceRepo(...))`). An
  optional branch override moves only the repository's `branch_modules`. Branches, tags and commit IDs are
  locators; the resolved commits are evidence.
- Each streamed file's source locator/argv, destination, size, MD5 and SHA-256 (`resolve_inputs`). These are
  **pinned expectations**, not snapshots of the mutable source. Replacement after resolution must match the saved
  expectations or fail.
- The runtime image's SHA-256 and size at its configured path (`resolve_image`). This is a preflight observation,
  **not an image snapshot or a lifetime immutability guarantee**.

The version-1 manifest (`make_manifest`) records coverage and `full_immutability: false` explicitly. Its canonical
digest identifies execution evidence only. An optional extra artifact (for example a candidate geometry) can be
included in the manifest; `binding(..., identity=...)` decides what of it must match on retry.

## Prepare, bind, dispatch and resume

`binding(directory, name, params, target, prepare)` takes a per-handle POSIX lock and durably saves the original
normalized parameters, target/source identity, manifest, complete rendered body and resources **before** the
caller calls the endpoint. A binding digest detects damaged saved state. A matching retry uses this original
rendering and does not resolve today's code, image, inputs or templates. Different parameters or target/source
configuration are refused. The Slurm/jobsub intent then binds that body and keeps its no-blind-redispatch rule
after an ambiguous outcome.

Source snapshots are atomically published after verification and made read-only. No existing code checkout is
reset or modified. Site filesystem aliases such as a symlinked scratch area are resolved at preparation; the
resulting canonical snapshot path is retained. Internal/escaping snapshot symlinks, dirty/staged/untracked
(including ignored) content, missing pins and changed executable modes fail verification. The job verifies source
bytes against Git objects before its runtime (`code_guard`, emitted by `runtime_prefix`). Git, Python 3 and Bash are
required on the endpoint outside any container. The branch-override set is configuration of the `SourceRepo`, not
part of the manifest: verify with the same `SourceRepo` that prepared the snapshot (without it, an overridden
snapshot fails verification).

There is **no automatic snapshot garbage collection**, including after failed preparation, a lost preparation
reply, cancellation or ambiguous submission. A controller exit or missing scheduler record does not prove an
artifact unused. Plan storage/retention and inspect all bindings/intents before manual cleanup. Read-only modes and
launch-time guards protect cooperating workflows, not against owner/admin modification after verification.

Legacy task records without a binding can be read with `checked_record(path, allow_legacy=True)` for status/cancel
and result collection; `binding` refuses to resubmit them. Never erase an old record merely to get a new
submission through.

## Staging and cache reuse

`cache_command`/`verify_cache` rehash staged bytes (full SHA-256 and MD5 and size); a `ready` marker is not
content evidence. `consumption_guard` lets the consuming job check the pinned staged bytes itself before its
payload runs. The job emits the exact bound `artifact-manifest.json` into `$<PREFIX>_OUT` (`runtime_prefix(manifest,
guard=..., out_var=...)`); `verify_result` refuses a missing or different manifest. This verifies the binding/guard
evidence, not full dependency closure or an attestation against concurrent external mutation.

## Explicit gaps

- Image bytes are checked at job start but not frozen throughout later container invocations. Replacing the image
  after verification remains unsafe.
- Downloaded dependencies, conditions fetched at runtime and include trees are not resolved or pinned here.
- Independent state roots, old clients and external writers are not coordinated.

## Verification

`tests/test_snapshots.py` and `tests/test_artifacts.py` use local Git fixture repositories, synthetic files and
local process tests; they contact no scheduler and transfer no data. They cover branch/SHA/submodule resolution,
old snapshot survival after a new ref, dirty/ignored/symlink rejection, site-alias canonicalization, changed bytes
at stable input/image paths, concurrent durable binding, original-rendering retries, cache corruption,
consumer-side guards and exact result-manifest matching.
