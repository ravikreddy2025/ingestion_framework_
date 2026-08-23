"""The five-layer merge and every guarantee framework/config.py makes.

Everything here runs against a SYNTHETIC source type ("demo") declared entirely in
tests/conftest.py. That is the point: if a validation rule passes here, it passed on the
SOURCE_SPEC alone, because nothing in framework/ has ever heard of "demo".

Needs no Spark, no secrets and no network.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from kafka_ingest.framework.config import (
    ConfigError,
    apply_overrides,
    read_source_type,
    resolve_config,
)

SRC = Path(__file__).resolve().parent.parent / "src" / "kafka_ingest"


def _resolve(config_root, spec, environment="prod", **kwargs):
    return resolve_config(config_root, "demo_source", environment, spec, **kwargs)


def _append_to_source(config_root, text: str) -> None:
    """Add settings to the fixture's `source:` block, verbatim.

    Written literally rather than dedented: these are YAML fragments where the two-space
    indent IS the meaning, and dedent would strip it off a single-line fragment and quietly
    promote the setting to the top level of the document.
    """
    with open(f"{config_root}/sources/demo_source.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n" + text.strip("\n") + "\n")


# --------------------------------------------------------------------------------------
# The five layers, one test each, in precedence order
# --------------------------------------------------------------------------------------


def test_defaults_only(demo_config_root, demo_spec):
    """Layer 1 + 1b. prod overrides nothing, so every value comes from the defaults."""
    cfg = _resolve(demo_config_root, demo_spec)
    assert cfg.get("trigger") == "availableNow"
    assert cfg.get("batch_limit") == 1000
    assert cfg.get("partition_by") == ["ingest_date"]
    assert cfg.get("audit_table") == "cat_prod.audit.ingest_audit"


def test_per_source_type_defaults_beat_common_defaults(demo_config_root, demo_spec):
    """Layer 1b beats layer 1. defaults.yaml says QUARANTINE for everything; the demo
    type's own defaults file says FAILFAST, and the more specific file wins."""
    assert _resolve(demo_config_root, demo_spec).get("failure_mode") == "FAILFAST"


def test_environment_overrides_defaults(demo_config_root, demo_spec):
    """Layer 2 beats layers 1 and 1b, for a key every source type shares."""
    assert _resolve(demo_config_root, demo_spec, "dev").get("audit_table") == "cat_dev.audit.dev_audit"
    assert _resolve(demo_config_root, demo_spec, "prod").get("audit_table") == "cat_prod.audit.ingest_audit"


def test_environment_per_type_defaults_beat_environment_defaults(demo_config_root, demo_spec):
    """Layer 2b. Without it, a type-specific tuning value in an environment file would be
    applied to every OTHER source type in that environment and rejected as an unknown key."""
    assert _resolve(demo_config_root, demo_spec, "dev").get("batch_limit") == 10
    assert _resolve(demo_config_root, demo_spec, "prod").get("batch_limit") == 1000


def test_source_overrides_environment_and_defaults(demo_config_root, demo_spec):
    """Layer 3 beats layers 1, 1b, 2 and 2b."""
    _append_to_source(demo_config_root, "  batch_limit: 7\n")
    assert _resolve(demo_config_root, demo_spec, "dev").get("batch_limit") == 7


def test_control_table_overrides_yaml(demo_config_root, demo_spec):
    """Layer 4 beats every structural layer - that is the whole point of it existing."""
    cfg = _resolve(demo_config_root, demo_spec, control={"batch_limit": 42})
    assert cfg.get("batch_limit") == 42


def test_job_parameters_beat_the_control_table(demo_config_root, demo_spec):
    """Layer 5. An urgent override typed into Workflows must not need an UPDATE first."""
    cfg = _resolve(demo_config_root, demo_spec, control={"batch_limit": 42}, job_parameters={"batch_limit": 99})
    assert cfg.get("batch_limit") == 99


def test_an_absent_key_falls_through_every_layer(demo_config_root, demo_spec):
    """No layer sets cursor_column, so it is simply absent - not defaulted, not an error."""
    cfg = _resolve(demo_config_root, demo_spec)
    assert cfg.get("cursor_column") is None
    assert "cursor_column" not in cfg.settings


def test_an_empty_override_does_not_erase_a_configured_value(demo_config_root, demo_spec):
    """Workflows sends "" for a parameter left blank. Treating that as a value would wipe
    the configured one on every run that did not fill the box in."""
    cfg = _resolve(demo_config_root, demo_spec, job_parameters={"batch_limit": "", "trigger": None})
    assert cfg.get("batch_limit") == 1000
    assert cfg.get("trigger") == "availableNow"


# --------------------------------------------------------------------------------------
# Spec-driven validation. None of these rules is written per source type.
# --------------------------------------------------------------------------------------


def test_unknown_key_is_rejected_and_names_the_key_and_the_source_type(demo_config_root, demo_spec):
    """A typo in structural config is silent misconfiguration - fail at load."""
    _append_to_source(demo_config_root, "  batch_limt: 5\n")
    with pytest.raises(ConfigError, match=r"unknown keys \['batch_limt'\].*source_type 'demo'"):
        _resolve(demo_config_root, demo_spec)


def test_missing_required_key_is_rejected(demo_config_root, demo_spec):
    path = f"{demo_config_root}/sources/demo_source.yaml"
    content = open(path, encoding="utf-8").read().replace("  object_name: WIDGET_EVENTS\n", "")
    open(path, "w", encoding="utf-8").write(content)
    with pytest.raises(ConfigError, match=r"missing required key\(s\) \['object_name'\]"):
        _resolve(demo_config_root, demo_spec)


def test_mutually_exclusive_keys_are_rejected_naming_both(demo_config_root, demo_spec):
    _append_to_source(demo_config_root, "  cursor_column: updated_at\n  full_refresh: true\n")
    with pytest.raises(ConfigError, match=r"\['cursor_column', 'full_refresh'\] are mutually exclusive"):
        _resolve(demo_config_root, demo_spec)


def test_one_of_a_mutually_exclusive_pair_is_fine(demo_config_root, demo_spec):
    """The rule is 'not both', not 'not either' - the negative case has to be checked too,
    or a rule that rejected everything would pass the test above."""
    _append_to_source(demo_config_root, "  cursor_column: updated_at\n")
    assert _resolve(demo_config_root, demo_spec).get("cursor_column") == "updated_at"


def test_an_operational_only_key_cannot_be_set_in_yaml(demo_config_root, demo_spec):
    """An incident-scoped bypass checked into Git would silently re-apply on every future
    deploy, long after the incident that justified it."""
    _append_to_source(demo_config_root, "  reset_id: INC12345\n")
    with pytest.raises(ConfigError, match=r"\['reset_id'\].*operational-only"):
        _resolve(demo_config_root, demo_spec)


def test_the_same_key_is_accepted_from_the_control_table(demo_config_root, demo_spec):
    """The other half of the rule above: operational-only means operational, not banned."""
    cfg = _resolve(demo_config_root, demo_spec, control={"reset_id": "INC12345"})
    assert cfg.get("reset_id") == "INC12345"


def test_a_structural_key_cannot_be_overridden_operationally(demo_config_root, demo_spec):
    """IGNORED, not rejected - CORE section 5.2. Partitioning describes what is already on
    disk, so an operational lever must not be able to move it."""
    cfg = _resolve(
        demo_config_root,
        demo_spec,
        control={"partition_by": ["nonsense"]},
        job_parameters={"landing_table": "somewhere.else.entirely"},
    )
    assert cfg.get("partition_by") == ["ingest_date"]
    assert cfg.get("landing_table") == "cat_prod.landing.demo_source"


def test_an_unknown_key_in_an_operational_override_is_still_rejected(demo_config_root, demo_spec):
    """A typo in the control table's source_overrides JSON must fail exactly like a YAML
    typo - otherwise the operational layer is a hole in the validation."""
    with pytest.raises(ConfigError, match=r"the control table contains unknown keys \['btch_limit'\]"):
        _resolve(demo_config_root, demo_spec, control={"btch_limit": 5})


def test_a_job_parameter_is_coerced_to_the_type_it_replaces(demo_config_root, demo_spec):
    """Job parameters arrive from Workflows as strings; batch_limit is an int everywhere
    else, and a string here would reach the reader as one."""
    cfg = _resolve(demo_config_root, demo_spec, job_parameters={"batch_limit": "250"})
    assert cfg.get("batch_limit") == 250


def test_apply_overrides_leaves_the_input_untouched(demo_spec):
    """It returns a new dict. A layer that mutated its input would make the merge order
    depend on which caller ran first."""
    settings = {"batch_limit": 1}
    result = apply_overrides(demo_spec, settings, {"batch_limit": 2}, "job parameters")
    assert settings == {"batch_limit": 1}
    assert result == {"batch_limit": 2}


# --------------------------------------------------------------------------------------
# Placeholders
# --------------------------------------------------------------------------------------


def test_unresolved_placeholder_is_a_hard_error(demo_config_root, demo_spec):
    """A {token} with no matching var would otherwise create a table literally named
    '{region}.landing...' and fail much later, much less clearly."""
    _append_to_source(demo_config_root, '  landing_table: "{region}.landing.demo"\n')
    with pytest.raises(ConfigError, match=r"uses \{region\}, which is not defined"):
        _resolve(demo_config_root, demo_spec)


def test_source_settings_may_use_source_key_and_domain(demo_config_root, demo_spec):
    _append_to_source(demo_config_root, '  landing_table: "{catalog}.{domain}.{source_key}"\n')
    assert _resolve(demo_config_root, demo_spec).get("landing_table") == "cat_prod.demo.demo_source"


def test_the_same_source_resolves_differently_per_environment(demo_config_root, demo_spec):
    """One source file, one catalog per environment, no duplication."""
    dev, prod = _resolve(demo_config_root, demo_spec, "dev"), _resolve(demo_config_root, demo_spec, "prod")
    assert dev.get("landing_table") == "cat_dev.landing.demo_source"
    assert prod.get("landing_table") == "cat_prod.landing.demo_source"


# --------------------------------------------------------------------------------------
# Layer 3a - a value for ONE source in ONE environment
# --------------------------------------------------------------------------------------


def test_source_environment_override_beats_the_bare_source_value(demo_config_root, demo_spec):
    _append_to_source(
        demo_config_root,
        "  batch_limit: 500\n  environments:\n    prod:\n      batch_limit: 5000\n",
    )
    assert _resolve(demo_config_root, demo_spec, "prod").get("batch_limit") == 5000
    # dev falls through to the bare value, exactly as if the override did not exist.
    assert _resolve(demo_config_root, demo_spec, "dev").get("batch_limit") == 500


def test_source_environment_block_naming_an_unknown_environment_is_rejected(demo_config_root, demo_spec):
    """A typo here would otherwise be silently unused - it only takes effect when THAT
    environment happens to be the one being resolved, so nothing would ever catch it."""
    _append_to_source(demo_config_root, "  environments:\n    staging:\n      batch_limit: 1\n")
    with pytest.raises(ConfigError, match=r"environments block names \['staging'\]"):
        _resolve(demo_config_root, demo_spec, "dev")


def test_the_environments_key_never_leaks_as_an_unknown_key(demo_config_root, demo_spec):
    """`environments:` is structure, not a setting - only what is INSIDE it is validated
    as one."""
    _append_to_source(demo_config_root, "  environments:\n    prod:\n      batch_limit: 3\n")
    assert _resolve(demo_config_root, demo_spec, "dev").get("batch_limit") == 10


def test_an_unknown_key_inside_a_source_environment_override_is_still_rejected(demo_config_root, demo_spec):
    _append_to_source(demo_config_root, "  environments:\n    prod:\n      btch_limit: 3\n")
    with pytest.raises(ConfigError, match="unknown keys"):
        _resolve(demo_config_root, demo_spec, "prod")


# --------------------------------------------------------------------------------------
# Registers - one mechanism for every source type
# --------------------------------------------------------------------------------------


def test_registers_are_discovered_and_overlaid_by_the_environment(demo_config_root, demo_spec):
    """The register says what is TRUE EVERYWHERE (auth mode, secret key names); the
    environment file supplies the endpoint."""
    profile = _resolve(demo_config_root, demo_spec, "dev").profile("widgets", "main")
    assert profile["auth_mode"] == "basic"
    assert profile["password_key"] == "widget-pw"
    assert profile["endpoint"] == "dev-endpoint:1521"
    assert _resolve(demo_config_root, demo_spec, "prod").profile("widgets", "main")["endpoint"] == (
        "prod-endpoint:1521"
    )


def test_register_values_get_vars_but_not_source_scoped_placeholders(demo_config_root, demo_spec):
    """A profile is shared by many sources, so substituting {source_key} into a wallet path
    would silently produce a per-source path."""
    profile = _resolve(demo_config_root, demo_spec).profile("widgets", "main")
    assert profile["wallet_path"] == "/Volumes/cat_prod/certs/wallet"


def test_an_environment_cannot_invent_a_register_profile(demo_config_root, demo_spec):
    """The register stays the single answer to 'what do we connect to?', so a typo in an
    environment file is an error rather than a silently-unused new profile."""
    path = f"{demo_config_root}/environments/prod.yaml"
    content = open(path, encoding="utf-8").read()
    open(path, "w", encoding="utf-8").write(content.replace("  main:\n", "  mian:\n"))
    with pytest.raises(ConfigError, match=r"'mian'.*not defined in widgets\.yaml"):
        _resolve(demo_config_root, demo_spec)


def test_referencing_a_profile_that_does_not_exist_names_the_known_ones(demo_config_root, demo_spec):
    cfg = _resolve(demo_config_root, demo_spec)
    with pytest.raises(ConfigError, match=r"known: \['main', 'spare'\]"):
        cfg.profile("widgets", "nope")


def test_an_unknown_top_level_key_in_an_environment_file_is_rejected(demo_config_root, demo_spec):
    """Anything that is not `vars`, `defaults`, `defaults_by_type` or a register name is a
    typo - and a typo'd register block would be silently ignored."""
    with open(f"{demo_config_root}/environments/prod.yaml", "a", encoding="utf-8") as handle:
        handle.write("\nwidgts: {}\n")
    with pytest.raises(ConfigError, match=r"unknown top-level key\(s\) \['widgts'\]"):
        _resolve(demo_config_root, demo_spec)


def test_a_register_file_must_use_its_own_filename_as_its_top_level_key(demo_config_root, demo_spec):
    open(f"{demo_config_root}/widgets.yaml", "w", encoding="utf-8").write("wigets:\n  main: {}\n")
    with pytest.raises(ConfigError, match=r"widgets\.yaml: expected a top-level 'widgets:' mapping"):
        _resolve(demo_config_root, demo_spec)


# --------------------------------------------------------------------------------------
# source_type - the key that selects everything else
# --------------------------------------------------------------------------------------


def test_a_source_file_must_declare_its_source_type(demo_config_root):
    path = f"{demo_config_root}/sources/demo_source.yaml"
    content = open(path, encoding="utf-8").read()
    open(path, "w", encoding="utf-8").write(content.replace("source_type: demo", ""))
    with pytest.raises(ConfigError, match="must declare a top-level `source_type:`"):
        read_source_type(demo_config_root, "demo_source")


def test_resolving_against_the_wrong_spec_is_refused(demo_config_root, demo_spec):
    """Defence against a caller that looked the module up by the wrong key - the resolved
    config would otherwise be validated against rules for a different source type."""
    from dataclasses import replace

    with pytest.raises(ConfigError, match="declares source_type 'demo' but was validated"):
        _resolve(demo_config_root, replace(demo_spec, source_type="other"))


def test_unknown_environment_lists_the_valid_ones(demo_config_root, demo_spec):
    with pytest.raises(ConfigError, match=r"unknown environment 'uat'.*\['dev', 'prod'\]"):
        _resolve(demo_config_root, demo_spec, "uat")


def test_the_resolved_settings_cannot_be_mutated(demo_config_root, demo_spec):
    """A source that edited its own config would make the audit row a lie."""
    cfg = _resolve(demo_config_root, demo_spec)
    with pytest.raises(TypeError):
        cfg.settings["batch_limit"] = 1


def test_enabled_defaults_to_true_and_is_operationally_overridable(demo_config_root, demo_spec):
    """`enabled` is framework-owned - no SOURCE_SPEC declares it, and every source has it."""
    assert _resolve(demo_config_root, demo_spec).enabled is True
    assert _resolve(demo_config_root, demo_spec, control={"enabled": False}).enabled is False


# --------------------------------------------------------------------------------------
# The rule that keeps all of the above runnable without a cluster
# --------------------------------------------------------------------------------------

PYSPARK_FREE = [
    SRC / "framework" / "config.py",
    SRC / "framework" / "contracts.py",
    SRC / "framework" / "logs.py",
    *sorted(SRC.glob("sources/*/spec.py")),
]


@pytest.mark.parametrize("path", PYSPARK_FREE, ids=lambda p: str(p.name))
def test_these_modules_import_no_pyspark(path):
    """framework/config.py and every sources/<name>/spec.py must stay importable with no
    Spark installed. That is what makes configuration testable in plain CI, and it is one
    `import pyspark.sql.functions as F` away from being lost.

    Parsed rather than imported: an import test would pass on any machine that happens to
    have PySpark installed, which is every developer machine and therefore useless.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            imported.add(node.module)
    offenders = sorted(name for name in imported if name.split(".")[0] in {"pyspark", "delta"})
    assert not offenders, f"{path.name} imports {offenders}"
