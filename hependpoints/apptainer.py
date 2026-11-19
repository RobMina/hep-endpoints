"""Running a SIF image inside a grid job that is itself a container (nested Apptainer), on workers with or without
/dev/fuse.

A grid job that ships its own image runs it with the Apptainer of the worker (for FNAL grid workers the OSG build on
/cvmfs, which runs unprivileged inside the job's container). Mounting a SIF file needs FUSE (squashfuse); many grid
workers have no /dev/fuse, so ``apptainer exec image.sif`` fails there. The image can still run from a sandbox
directory: ``apptainer build --sandbox DIR image.sif`` unpacks it without FUSE (seconds to a minute for a few
hundred MB; the sandbox needs about 3-4 times the SIF size on the job's disk).

``sandbox_fallback`` renders the shell for a job script: try the SIF with a check command; when that fails, build the
sandbox in the job directory, delete the SIF, and run the check again from the sandbox. Afterwards the image variable
names what to pass to ``apptainer exec`` and the mode variable says ``sif`` or ``sandbox``. The rendered text uses
only the job's own variables; all names and paths are arguments.
"""
from __future__ import annotations


def sandbox_fallback(apptainer: str = '"$APPT"', sif: str = '"$W/img.sif"', sandbox: str = '"$W/img"',
                     tmpdir: str = '"$W/apptmp"', log: str = '"$W/sandbox.log"',
                     check: str = 'python3 -c "$CHECK"', image_var: str = "IMG", mode_var: str = "MODE",
                     fail_rc: int = 4) -> str:
    """Shell lines (each a quoted shell word or expression, as written into the job) that set `image_var` to a
    runnable image: `sif` if ``apptainer exec --cleanenv <sif> <check>`` succeeds, else a sandbox built at `sandbox`
    (``APPTAINER_TMPDIR`` set to `tmpdir` for the build, `log` keeps its output). Exits `fail_rc` when neither runs.
    `mode_var` is set to ``sif`` or ``sandbox``; the build time is printed."""
    I, M = image_var, mode_var
    return (
        f"{I}={sif}; {M}=sif\n"
        f"if ! {apptainer} exec --cleanenv \"${I}\" {check}; then\n"
        f"  echo \"SIF mount failed (no /dev/fuse on this worker?): building a sandbox\"; t1=$SECONDS\n"
        f"  export APPTAINER_TMPDIR={tmpdir}; mkdir -p \"$APPTAINER_TMPDIR\"\n"
        f"  {apptainer} build --sandbox {sandbox} \"${I}\" > {log} 2>&1 || "
        f"{{ tail -5 {log}; echo \"sandbox build failed\"; exit {fail_rc}; }}\n"
        f"  rm -f \"${I}\"; rm -rf \"$APPTAINER_TMPDIR\"; {I}={sandbox}; {M}=sandbox\n"
        f"  echo \"sandbox $((SECONDS-t1)) s\"\n"
        f"  {apptainer} exec --cleanenv \"${I}\" {check} || "
        f"{{ echo \"nested apptainer failed (sandbox)\"; exit {fail_rc}; }}\n"
        "fi\n")
