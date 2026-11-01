"""hependpoints.site and the endpoint registry: configuration only from the environment, failing closed."""
from pathlib import Path

import pytest

import hependpoints
from hependpoints import EndpointError, site


def test_unset_variables_raise_and_name_the_variable():
    with pytest.raises(site.SiteConfigError, match="HEPEP_X or HEPEP_Y"):
        site.require("HEPEP_X", "HEPEP_Y")
    with pytest.raises(site.SiteConfigError, match="see here"):
        site.require("HEPEP_X", hint="see here")


def test_values_fallbacks_and_numbers(clean_env):
    assert site.setting("HEPEP_UNSET_FOR_TEST", "d") == "d"
    clean_env.setenv("HEPEP_Y", "y")
    assert site.require("HEPEP_X", "HEPEP_Y") == "y"
    clean_env.setenv("HEPEP_X", "")
    assert site.require("HEPEP_X", "HEPEP_Y") == "y"              # empty counts as unset
    assert site.number("HEPEP_N", 3.0) == 3.0
    clean_env.setenv("HEPEP_N", "2.5")
    assert site.number("HEPEP_N") == 2.5
    clean_env.setenv("HEPEP_N", "x")
    with pytest.raises(site.SiteConfigError, match="not a number"):
        site.number("HEPEP_N")


def test_state_root(clean_env, tmp_path):
    clean_env.setenv("HEPEP_STATE_DIR", str(tmp_path / "s"))
    assert site.state_dir("eaf") == tmp_path / "s" / "eaf"
    clean_env.delenv("HEPEP_STATE_DIR")
    assert site.state_root() == Path("~/.hependpoints").expanduser()


def test_registry(tmp_path):
    assert {"slurm_ssh", "eaf", "jobsub"} <= set(hependpoints.known())
    with pytest.raises(EndpointError, match="unknown endpoint 'nope'"):
        hependpoints.get("nope")
    made = []
    hependpoints.register("mysite", lambda **kw: made.append(kw) or "endpoint")
    try:
        assert hependpoints.get("mysite", state_dir=tmp_path) == "endpoint" and made == [{"state_dir": tmp_path}]
    finally:
        hependpoints._REGISTRY.pop("mysite")
    ep = hependpoints.get("slurm_ssh", workdir="/path/to/w", state_dir=tmp_path)
    assert ep.name == "slurm_ssh" and ep.workdir == "/path/to/w"
    with pytest.raises(ValueError):
        hependpoints.register("", lambda **kw: None)


def test_job_environment_validation():
    from hependpoints.environment import JobEnvironment
    env = JobEnvironment(var_prefix="ABC", job_prefix="abc", preamble="x")
    assert (env.job_dir_var, env.out_var, env.token_var, env.preamble) == ("ABC_JOB_DIR", "ABC_OUT", "ABC_TOKEN", "x\n")
    for bad in (dict(var_prefix="abc"), dict(var_prefix="A B"), dict(job_prefix="Abc"), dict(job_prefix="a-b"),
                dict(job_prefix="x" * 17)):
        with pytest.raises(ValueError):
            JobEnvironment(**bad)
