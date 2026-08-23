"""Five-layer configuration loading, merging and spec-driven validation.

FIVE LAYERS. Later always wins on a per-key basis; absent keys fall through.

  STRUCTURAL (YAML in Git, PR-reviewed, deployed by DAB)
    1. conf/defaults.yaml                 common to every source of every type
       conf/defaults/<source_type>.yaml   common to every source of ONE type
    2. conf/environments/<env>.yaml       vars, plus `defaults:` (every type) and
                                          `defaults_by_type:` (one type) for this env
    3. conf/sources/<source_key>.yaml     only what is unique to this source
         3a. source:.environments.<env>   OPTIONAL, in the same file - what is unique to
                                          this source IN ONE environment. Rare.

  OPERATIONAL (no deploy required)
    4. the operational control table      support-team runtime overrides
    5. job parameters                     one-off overrides from Workflows

  Supporting registers, merged into layer 2:
    every conf/<name>.yaml other than defaults.yaml. A register records what EXISTS
    (auth mode, secret KEY names, endpoints); an environment file overrides named
    profiles in it. A profile named in an environment file that does not exist in its
    register is an error, which is what keeps the register the single answer to "what
    do we connect to?". Adding a register is a new file and no code change: this module
    discovers them by listing, and never names one.

The "common, then per-type" pairing appears at both structural levels on purpose. Without
it, a Kafka tuning value in conf/environments/dev.yaml would be applied to every Oracle
source in dev and rejected there as an unknown key.

{placeholder} tokens in layers 1-3 resolve from the environment's `vars:`, plus
{source_key} and {domain} for source settings. Register values get vars ONLY - a profile
is shared by many sources, so substituting {source_key} into a cert path would silently
produce a per-source path. An unresolved placeholder is a hard error, with ONE declared
exception: the tokens a source type lists in SOURCE_SPEC.target_tokens are left for the
source itself to fill, because only it knows them. See framework/tables.py.

VALIDATION IS DATA-DRIVEN. There is no `if source_type == ...` in this module, and there
must never be one. Everything comes from the source's SOURCE_SPEC (framework/contracts.py):
unknown key, missing required key, mutually exclusive keys, operational overrides of
structural fields, and structural YAML setting an operational-only key.

NO PYSPARK IMPORT. Not here, and not in any sources/<name>/spec.py. That is what keeps
configuration testable with no cluster, and tests/test_framework_config.py asserts it.
Reading the control table needs Spark, so it does not happen here - framework/control.py
hands `resolve_config` a plain dict.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

import yaml

from .contracts import SourceSpec

LOG = logging.getLogger(__name__)


class ConfigError(ValueError):
    """Raised for any structurally invalid or internally inconsistent configuration."""


# Keys the framework itself owns for every source type, so no SOURCE_SPEC has to declare
# them. Keep this list very short: anything here is a key three source authors can no
# longer use for their own purposes.
#
#   domain            owning team. Appears in every audit row and fills {domain}.
#   enabled           the emergency stop. Settable in YAML and, more usefully, in the
#                     control table - turning a source off must not need a deploy.
#   audit_table       the one shared audit table. Written by framework/audit.py.
#   state_table       durable watermarks and run sequences. framework/state.py.
#   control_table     where the layer-4 overrides are read from. framework/control.py.
#   table_properties  TBLPROPERTIES for every table the framework creates.
#   rerun_id          identifies a replay. Operational ONLY - a replay id checked into Git
#                     would re-apply on every future deploy.
#
# The three table names are here rather than in each SOURCE_SPEC because the framework
# reads them and no source does; a source type that had to declare them could also
# misspell them.
FRAMEWORK_STRUCTURAL_KEYS = frozenset(
    {
        "domain",
        "enabled",
        "audit_table",
        "state_table",
        "control_table",
        "table_properties",
    }
)
FRAMEWORK_OPERATIONAL_KEYS = frozenset({"enabled", "rerun_id"})

# Top-level keys an environment file may use that are not register names.
_ENV_RESERVED_KEYS = frozenset({"vars", "defaults", "defaults_by_type"})

# Placeholders look like {catalog}. Only lower_snake_case names are recognised, so a
# stray brace in a value (e.g. a JSON fragment) is left alone rather than half-substituted.
_PLACEHOLDER = re.compile(r"\{([a-z_][a-z0-9_]*)\}")


# --------------------------------------------------------------------------------------
# The resolved configuration the runner and the sources consume
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedConfig:
    """One source, one environment, all five layers merged, substituted and validated.

    `settings` is intentionally a read-only mapping rather than a per-source dataclass:
    the framework has no way to name a source's dataclass without knowing the source type,
    and knowing the source type is precisely what CORE section 7 forbids. Each source
    builds its own frozen dataclass from these settings inside its own package - see
    docs/build_log/STAGE_1_REPORT.md.
    """

    source_key: str
    source_type: str
    environment: str
    layers: tuple[str, ...]
    settings: Mapping[str, Any]
    registers: Mapping[str, Mapping[str, Mapping[str, Any]]]

    @property
    def enabled(self) -> bool:
        return bool(self.settings.get("enabled", True))

    def get(self, key: str, default: Any = None) -> Any:
        return self.settings.get(key, default)

    def profile(self, register: str, name: str) -> Mapping[str, Any]:
        """One profile from one register, e.g. profile("clusters", settings["cluster"]).

        Lives here rather than in each source so that "you named something that does not
        exist" reads the same however you got there.
        """
        if register not in self.registers:
            raise ConfigError(
                f"source '{self.source_key}' asked for register '{register}', which has no "
                f"conf/{register}.yaml (known: {sorted(self.registers)})"
            )
        profiles = self.registers[register]
        if name not in profiles:
            raise ConfigError(
                f"source '{self.source_key}' references '{name}' which is not in "
                f"{register}.yaml (known: {sorted(profiles)})"
            )
        return profiles[name]


# --------------------------------------------------------------------------------------
# Reading YAML
# --------------------------------------------------------------------------------------


def _read_yaml(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        raise ConfigError(f"structural config file not found: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: expected a YAML mapping at the top level")
    return data


def _read_yaml_if_present(path: str) -> dict[str, Any]:
    """A per-source-type defaults file is optional - a source type with nothing to say
    platform-wide should not need an empty file to exist."""
    return _read_yaml(path) if os.path.exists(path) else {}


def _mapping(value: Any, where: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{where}: expected a mapping, got {type(value).__name__}")
    return dict(value)


def available_environments(config_root: str) -> list[str]:
    directory = os.path.join(config_root, "environments")
    if not os.path.isdir(directory):
        return []
    return sorted(f[:-5] for f in os.listdir(directory) if f.endswith(".yaml"))


def source_file(config_root: str, source_key: str) -> str:
    return os.path.join(config_root, "sources", f"{source_key}.yaml")


def read_source_type(config_root: str, source_key: str) -> str:
    """Which source type is this? Read before anything else, because it selects the spec.

    The runner calls this, looks the module up in its dispatch dict, and hands the
    resulting SOURCE_SPEC back to resolve_config. Two reads of one small file, and no
    dynamic import by string anywhere.
    """
    doc = _read_yaml(source_file(config_root, source_key))
    declared = doc.get("source_type")
    if not declared:
        raise ConfigError(
            f"sources/{source_key}.yaml must declare a top-level `source_type:` - it is what "
            "selects which source implementation and which set of valid keys apply."
        )
    return str(declared)


# --------------------------------------------------------------------------------------
# Placeholders
# --------------------------------------------------------------------------------------


def _substitute(value: Any, scope: Mapping[str, Any], where: str, deferred: frozenset[str] = frozenset()) -> Any:
    """Resolve {placeholder} tokens in strings, recursing into lists and dicts.

    An unresolved placeholder is a hard error naming the setting and the offending token.
    Letting it through would produce a table literally called `{catalog}.landing...`, which
    fails much later and much less clearly.

    `deferred` is the exception, and it is a short, declared list: a source type's
    SOURCE_SPEC.target_tokens names the placeholders only the SOURCE can fill, because they
    come from data the configuration does not have - a Kafka topic name, a database table
    name. Those are passed through untouched for framework/tables.py to fill at run time.
    A token NOT in that list is still an error, which is what keeps a typo a startup
    failure rather than a table with a brace in its name.
    """
    if isinstance(value, str):

        def _replace(match: re.Match) -> str:
            name = match.group(1)
            if name in deferred:
                return match.group(0)
            if name not in scope:
                raise ConfigError(
                    f"{where}: '{value}' uses {{{name}}}, which is not defined. Available: "
                    f"{sorted(scope)}. Add it under `vars:` in the environment file, or - if "
                    "only the source can supply it - to that source's SOURCE_SPEC.target_tokens."
                )
            return str(scope[name])

        return _PLACEHOLDER.sub(_replace, value)
    if isinstance(value, list):
        return [_substitute(item, scope, where, deferred) for item in value]
    if isinstance(value, dict):
        return {k: _substitute(v, scope, where, deferred) for k, v in value.items()}
    return value


# --------------------------------------------------------------------------------------
# Registers
# --------------------------------------------------------------------------------------


def _register_names(config_root: str) -> list[str]:
    """Every conf/<name>.yaml except defaults.yaml is a register."""
    return sorted(f[:-5] for f in os.listdir(config_root) if f.endswith(".yaml") and f != "defaults.yaml")


def _overlay_profiles(
    base: Mapping[str, Any], overrides: Mapping[str, Any], register_file: str, environment: str
) -> dict[str, dict[str, Any]]:
    """Merge per-environment profile overrides over the base register, key by key.

    A profile named in the environment file must already exist in the base file. That keeps
    the register the single answer to "which clusters do we consume from?", and turns a typo
    into an error instead of a silently-unused new profile.
    """
    merged = {name: dict(profile) for name, profile in base.items()}
    for name, override in (overrides or {}).items():
        if name not in merged:
            raise ConfigError(
                f"environments/{environment}.yaml overrides the profile '{name}', which is not "
                f"defined in {register_file} (known: {sorted(merged)}). Add it there first."
            )
        merged[name].update(override)
    return merged


def _load_registers(
    config_root: str, env_doc: Mapping[str, Any], environment: str, env_vars: Mapping[str, Any]
) -> dict[str, dict[str, dict[str, Any]]]:
    """Load every register, apply this environment's overlays, substitute vars.

    A register file's top-level key must equal its filename stem (clusters.yaml holds
    `clusters:`). One rule, no per-register special cases, and a new register is a new file
    with no framework change.
    """
    names = _register_names(config_root)

    unknown_env_keys = set(env_doc) - _ENV_RESERVED_KEYS - set(names)
    if unknown_env_keys:
        raise ConfigError(
            f"environments/{environment}.yaml has unknown top-level key(s) "
            f"{sorted(unknown_env_keys)}. Expected {sorted(_ENV_RESERVED_KEYS)} or a register "
            f"name from {names}."
        )

    registers: dict[str, dict[str, dict[str, Any]]] = {}
    for name in names:
        file_name = f"{name}.yaml"
        doc = _read_yaml(os.path.join(config_root, file_name))
        if name not in doc:
            raise ConfigError(
                f"{file_name}: expected a top-level '{name}:' mapping (a register file's "
                "top-level key must match its filename)."
            )
        overlaid = _overlay_profiles(
            _mapping(doc[name], file_name),
            _mapping(env_doc.get(name), f"environments/{environment}.yaml"),
            file_name,
            environment,
        )
        registers[name] = {
            profile_name: _substitute(profile, env_vars, f"{file_name}/{profile_name}")
            for profile_name, profile in overlaid.items()
        }
    return registers


# --------------------------------------------------------------------------------------
# Layers 1-3: the structural merge
# --------------------------------------------------------------------------------------


def load_structural(
    config_root: str, source_key: str, environment: str, deferred: frozenset[str] = frozenset()
) -> dict[str, Any]:
    """Merge and substitute layers 1-3 for one source in one environment.

    Returns the merged settings. Spec-free on purpose: `resolve_config` validates, and
    keeping the merge separate means a caller that only wants to see what the YAML says
    (a notebook, a config dump) does not need a spec to get it.

    `deferred` is that rule's one concession - a caller that HAS a spec passes
    `spec.target_tokens` so a target pattern naming a source-derived value survives the
    merge instead of failing on it. A caller with no spec passes nothing and gets the
    strict behaviour, which is the right default for a config dump.
    """
    env_path = os.path.join(config_root, "environments", f"{environment}.yaml")
    if not os.path.exists(env_path):
        raise ConfigError(
            f"unknown environment '{environment}' - no {env_path}. Available: {available_environments(config_root)}"
        )
    env_doc = _read_yaml(env_path)
    source_doc = _read_yaml(source_file(config_root, source_key))
    source_type = read_source_type(config_root, source_key)

    source_raw = _mapping(source_doc.get("source"), f"sources/{source_key}.yaml: 'source:'")
    if not source_raw:
        raise ConfigError(f"sources/{source_key}.yaml: expected a top-level 'source:' mapping")

    this_env_override = _source_environment_override(config_root, source_key, environment, source_raw)

    # Layers 1 -> 1b -> 2 -> 2b -> 3 -> 3a. Shallow per key: a list or scalar replaces
    # wholesale rather than merging, which is what makes an override predictable to read.
    env_where = f"environments/{environment}.yaml"
    merged: dict[str, Any] = {
        **_mapping(_read_yaml(os.path.join(config_root, "defaults.yaml")).get("defaults"), "defaults.yaml"),
        **_mapping(
            _read_yaml_if_present(os.path.join(config_root, "defaults", f"{source_type}.yaml")).get("defaults"),
            f"defaults/{source_type}.yaml",
        ),
        **_mapping(env_doc.get("defaults"), env_where),
        **_mapping(
            _mapping(env_doc.get("defaults_by_type"), env_where).get(source_type),
            f"{env_where}: defaults_by_type.{source_type}",
        ),
        **source_raw,
        **this_env_override,
    }

    # Source settings may use vars plus these two derived values. Resolved AFTER 3a is
    # folded in, so an environment-specific override can use them too.
    scope = {
        **_mapping(env_doc.get("vars"), env_where),
        "source_key": source_key,
        "domain": merged.get("domain", ""),
    }
    return _substitute(merged, scope, f"sources/{source_key}.yaml [{environment}]", deferred)


def _source_environment_override(
    config_root: str, source_key: str, environment: str, source_raw: dict[str, Any]
) -> dict[str, Any]:
    """Layer 3a: `environments:` nested inside the source file.

    Popped out of `source_raw` (which this function mutates) before the main merge, so it
    never reaches the unknown-key check as a key in its own right - only the settings
    inside it do, exactly like every other source setting.

    Every environment named must be real. This is the one place a typo would otherwise be
    silently unused rather than a startup error, because it only takes effect when THAT
    environment happens to be the one being resolved.
    """
    overrides = source_raw.pop("environments", None) or {}
    if not isinstance(overrides, dict):
        raise ConfigError(
            f"sources/{source_key}.yaml: 'environments:' must be a mapping of environment name "
            "-> override settings, e.g. 'environments: {prod: {batch_limit: ...}}'"
        )
    known = set(available_environments(config_root))
    unknown = set(overrides) - known
    if unknown:
        raise ConfigError(
            f"sources/{source_key}.yaml: environments block names {sorted(unknown)}, which "
            f"{'is' if len(unknown) == 1 else 'are'} not in Available: {sorted(known)}"
        )
    this_env = overrides.get(environment) or {}
    if not isinstance(this_env, dict):
        raise ConfigError(
            f"sources/{source_key}.yaml: environments.{environment} must be a mapping of "
            "settings to override, not a scalar"
        )
    return this_env


# --------------------------------------------------------------------------------------
# Spec-driven validation
# --------------------------------------------------------------------------------------


def layer_table_keys(spec: SourceSpec) -> frozenset[str]:
    """`<layer>_table` for every layer this source type has.

    ONE convention, defined here and read in framework/tables.py: a source's target table
    for layer L is the setting `<L>_table`. Framework-owned for the same reason the audit
    table is - the framework resolves, validates and creates those tables, so it also
    decides what they are called in configuration.
    """
    return frozenset(f"{layer}_table" for layer in spec.layers)


def known_keys(spec: SourceSpec) -> frozenset[str]:
    """Every key any layer may set for this source type."""
    return (
        spec.structural_keys
        | spec.operational_keys
        | FRAMEWORK_STRUCTURAL_KEYS
        | FRAMEWORK_OPERATIONAL_KEYS
        | layer_table_keys(spec)
    )


def _operational_only(spec: SourceSpec) -> frozenset[str]:
    return (spec.operational_keys | FRAMEWORK_OPERATIONAL_KEYS) - (spec.structural_keys | FRAMEWORK_STRUCTURAL_KEYS)


def _validate_structural(spec: SourceSpec, settings: Mapping[str, Any], origin: str) -> None:
    unknown = sorted(set(settings) - known_keys(spec))
    if unknown:
        raise ConfigError(
            f"{origin} contains unknown keys {unknown} for source_type '{spec.source_type}'. "
            "Typos here are silent misconfiguration - fix the YAML or extend that source's "
            f"SOURCE_SPEC. Known keys: {sorted(known_keys(spec))}"
        )
    forbidden = sorted(_operational_only(spec) & set(settings))
    if forbidden:
        raise ConfigError(
            f"{origin}: {forbidden} {'is an' if len(forbidden) == 1 else 'are'} "
            f"operational-only override(s) for source_type '{spec.source_type}' and must not "
            "be set in YAML - a value checked into Git would silently re-apply on every "
            "future deploy. Set it in the control table instead."
        )


def _validate_complete(spec: SourceSpec, settings: Mapping[str, Any], origin: str) -> None:
    missing = sorted(spec.required_keys - set(settings))
    if missing:
        raise ConfigError(
            f"{origin} is missing required key(s) {missing} for source_type "
            f"'{spec.source_type}'. They are not set in any configuration layer."
        )
    for group in spec.mutually_exclusive:
        both = sorted(key for key in group if settings.get(key) is not None)
        if len(both) > 1:
            raise ConfigError(
                f"{origin}: {both} are mutually exclusive for source_type '{spec.source_type}' - set exactly one."
            )


def validate_override_keys(spec: SourceSpec, overrides: Mapping[str, Any], origin: str) -> None:
    """An operational override naming a key no source of this type has is an error.

    Shared with framework/control.py so that a typo in the control table's
    `source_overrides` JSON produces the same class of error a YAML typo produces.
    """
    unknown = sorted(set(overrides) - known_keys(spec))
    if unknown:
        raise ConfigError(
            f"{origin} contains unknown keys {unknown} for source_type '{spec.source_type}'. "
            f"Known keys: {sorted(known_keys(spec))}"
        )


def apply_overrides(
    spec: SourceSpec, settings: Mapping[str, Any], overrides: Mapping[str, Any], origin: str
) -> dict[str, Any]:
    """Apply one operational layer. Unknown key -> error; structural key -> IGNORED.

    Ignoring rather than rejecting a structural key is deliberate, and is CORE section 5.2:
    partitioning, merge keys and target names describe what is already on disk, so an
    operational lever must not be able to move them. It is logged, never silent.
    """
    validate_override_keys(spec, overrides, origin)
    operational = spec.operational_keys | FRAMEWORK_OPERATIONAL_KEYS
    result = dict(settings)
    for key, value in overrides.items():
        if value is None or value == "":
            continue
        if key not in operational:
            LOG.warning(
                "%s tried to override the structural key '%s'; ignored. Structural fields "
                "describe what is already on disk and can only change via a PR.",
                origin,
                key,
            )
            continue
        result[key] = _coerce_like(result.get(key), value)
    return result


def _coerce_like(existing: Any, value: Any) -> Any:
    """Job parameters arrive as strings. Coerce to the type of the value being replaced.

    Deliberately conservative: with no prior value there is nothing to infer from, so the
    string is passed through rather than guessed at. SourceSpec carries no per-key types
    today - see the Stage 1 report's decisions list.
    """
    if not isinstance(value, str) or existing is None or isinstance(existing, str):
        return value
    if isinstance(existing, bool):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    if isinstance(existing, int):
        return int(value)
    if isinstance(existing, float):
        return float(value)
    return value


# --------------------------------------------------------------------------------------
# Full resolution
# --------------------------------------------------------------------------------------


def resolve_config(
    config_root: str,
    source_key: str,
    environment: str,
    spec: SourceSpec,
    control: Mapping[str, Any] | None = None,
    job_parameters: Mapping[str, Any] | None = None,
) -> ResolvedConfig:
    """Full five-layer resolution for one source in one environment.

        defaults -> defaults/<type> -> environment -> source -> source.environments.<env>
            -> control table -> job parameters

    `control` is the override dict framework/control.py read from the control table, and
    `job_parameters` are what a support engineer typed into the Workflows UI. Job
    parameters win, so an urgent one-off does not require an UPDATE statement first.
    """
    declared = read_source_type(config_root, source_key)
    if declared != spec.source_type:
        raise ConfigError(
            f"sources/{source_key}.yaml declares source_type '{declared}' but was validated "
            f"against the '{spec.source_type}' spec."
        )

    origin = f"sources/{source_key}.yaml"
    settings = load_structural(config_root, source_key, environment, spec.target_tokens)
    _validate_structural(spec, settings, origin)

    settings = apply_overrides(spec, settings, control or {}, "the control table")
    settings = apply_overrides(spec, settings, job_parameters or {}, "job parameters")
    _validate_complete(spec, settings, origin)

    env_where = f"environments/{environment}.yaml"
    env_doc = _read_yaml(os.path.join(config_root, "environments", f"{environment}.yaml"))
    registers = _load_registers(config_root, env_doc, environment, _mapping(env_doc.get("vars"), env_where))

    return ResolvedConfig(
        source_key=source_key,
        source_type=spec.source_type,
        environment=environment,
        layers=spec.layers,
        settings=MappingProxyType(dict(settings)),
        registers=MappingProxyType({name: MappingProxyType(profiles) for name, profiles in registers.items()}),
    )
