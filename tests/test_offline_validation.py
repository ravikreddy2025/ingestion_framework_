"""Offline substitutes for the cluster/CLI validation this project cannot run locally.

CORE section 3 names five checks explicitly, standing in for what a real Databricks
workspace and `databricks bundle validate` would otherwise catch (VB-13). Every check here
is pure Python and pure YAML/AST parsing - no Spark session, no network, no CLI.

These are deliberately GENERIC over source type: a sixth source type should make every test
in this file pass without editing it, the same falsifiable property the CORE section 7 grep
gate proves for framework/ itself.

A SIXTH check, beyond CORE's original five, was added in Stage 8: `notebooks/*.py` cannot be
executed here (no dbutils, no cluster), so nothing previously caught them importing a name
that no longer exists. That is exactly what happened between Stage 3 and Stage 7 - all four
notebooks imported the pre-refactor `kafka_ingest.config`/`kafka_ingest.pipeline` module
names for four stages before anyone noticed (`docs/build_log/STAGE_7_REPORT.md`'s own
research section, and its "what I would change" item 2). The check below is the cheap,
mechanical substitute Stage 7 recommended: AST-parse every notebook's `kafka_ingest` imports
and resolve each one against the real, current source tree.
"""

from __future__ import annotations

import ast
import importlib
import tomllib
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
CONF_ROOT = REPO / "conf"
RESOURCES_ROOT = REPO / "resources"
SOURCES_PKG_ROOT = REPO / "src" / "kafka_ingest" / "sources"
NOTEBOOKS_ROOT = REPO / "notebooks"
DATABRICKS_YML = REPO / "databricks.yml"
PYPROJECT = REPO / "pyproject.toml"


# ========================================================================================
# 1. yaml.safe_load succeeds on every file under conf/ and resources/, and on databricks.yml
# ========================================================================================


def _all_yaml_files() -> list[Path]:
    files: set[Path] = set()
    for root in (CONF_ROOT, RESOURCES_ROOT):
        files.update(root.rglob("*.yaml"))
        files.update(root.rglob("*.yml"))
    files.add(DATABRICKS_YML)
    return sorted(files)


@pytest.mark.parametrize("path", _all_yaml_files(), ids=lambda p: str(p.relative_to(REPO)))
def test_every_yaml_file_parses(path):
    """A file that does not parse fails every job that tries to load it, at startup, in
    every environment - the loudest possible version of this failure, but only if someone
    tries. This is what tries on every PR instead."""
    assert path.is_file(), f"{path} does not exist"
    yaml.safe_load(path.read_text(encoding="utf-8"))


# ========================================================================================
# 2. Every job template's referenced entrypoint file exists on disk
# ========================================================================================


def _job_files() -> list[Path]:
    return sorted(RESOURCES_ROOT.glob("*.yml"))


def _tasks_in(path: Path) -> list[dict]:
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    tasks = []
    for job in (document.get("resources", {}).get("jobs") or {}).values():
        tasks.extend(job.get("tasks") or [])
    return tasks


def _wheel_entry_points() -> dict[str, str]:
    """pyproject.toml's [project.scripts] - name -> "module.path:function"."""
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    return data["project"]["scripts"]


def _entry_point_file_and_function(target: str) -> tuple[Path, str]:
    module, _, function = target.partition(":")
    return REPO / "src" / Path(*module.split(".")).with_suffix(".py"), function


@pytest.mark.parametrize("path", _job_files(), ids=lambda p: p.name)
def test_every_wheel_task_entry_point_resolves_to_a_real_function_on_disk(path):
    """A `python_wheel_task.entry_point` that is not in pyproject.toml's [project.scripts],
    or whose target function does not exist, installs fine and fails on the first run - an
    argparse/import error nobody reads until the schedule fires."""
    scripts = _wheel_entry_points()
    for task in _tasks_in(path):
        wheel = task.get("python_wheel_task")
        if not wheel:
            continue
        entry_point = wheel["entry_point"]
        assert entry_point in scripts, (
            f"{path.name} task '{task.get('task_key')}': entry_point '{entry_point}' is not "
            f"declared in pyproject.toml [project.scripts] {sorted(scripts)}"
        )
        file_path, function = _entry_point_file_and_function(scripts[entry_point])
        assert file_path.is_file(), (
            f"pyproject.toml's '{entry_point}' script points at '{scripts[entry_point]}', "
            f"but {file_path} does not exist"
        )
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
        defined = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
        assert function in defined, (
            f"pyproject.toml's '{entry_point}' script points at '{scripts[entry_point]}', "
            f"but '{function}' is not defined in {file_path}"
        )


@pytest.mark.parametrize("path", _job_files(), ids=lambda p: p.name)
def test_every_sql_task_file_exists_on_disk(path):
    """A `sql_task.file.path` is resolved relative to resources/ by the bundle - the same
    directory this test resolves it from."""
    for task in _tasks_in(path):
        sql_task = task.get("sql_task")
        file_ref = (sql_task or {}).get("file", {}).get("path")
        if not file_ref:
            continue
        resolved = (RESOURCES_ROOT / file_ref).resolve()
        assert resolved.is_file(), (
            f"{path.name} task '{task.get('task_key')}': sql_task.file.path '{file_ref}' -> {resolved} does not exist"
        )


# ========================================================================================
# 3. Every source_type in conf/sources/*.yaml has a matching package in sources/, exposing
#    SOURCE_SPEC and run
# ========================================================================================


def _declared_source_types() -> list[str]:
    types = set()
    for path in (CONF_ROOT / "sources").glob("*.yaml"):
        if path.name.startswith("_"):
            continue
        declared = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("source_type")
        if declared:
            types.add(declared)
    return sorted(types)


@pytest.mark.parametrize("source_type", _declared_source_types())
def test_every_declared_source_type_has_a_package_exposing_the_contract(source_type):
    """CORE section 4.1: a source's entire public surface is SOURCE_SPEC and run(ctx). A
    source_type with no package, or a package missing either half, is a source that cannot
    actually be dispatched - runner.py's _SOURCES would raise KeyError at the first run."""
    package_dir = SOURCES_PKG_ROOT / source_type
    assert package_dir.is_dir(), (
        f"conf/sources declares source_type '{source_type}', but sources/{source_type}/ does not exist"
    )
    module = importlib.import_module(f"kafka_ingest.sources.{source_type}")
    assert hasattr(module, "SOURCE_SPEC"), f"sources/{source_type}/__init__.py does not expose SOURCE_SPEC"
    assert hasattr(module, "run") and callable(module.run), f"sources/{source_type}/__init__.py does not expose run()"
    assert module.SOURCE_SPEC.source_type == source_type, (
        f"sources/{source_type}/spec.py's SOURCE_SPEC.source_type is "
        f"'{module.SOURCE_SPEC.source_type}', not '{source_type}'"
    )


def test_every_dispatchable_type_in_runner_has_a_declared_source():
    """The reverse direction: a type runner.py can dispatch to, but no shipped source ever
    declares, is not wrong - but every one SHOULD at least resolve the same way, which this
    both documents and pins."""
    from kafka_ingest.framework.runner import _SOURCES

    for source_type, module in _SOURCES.items():
        assert (SOURCES_PKG_ROOT / source_type).is_dir()
        assert hasattr(module, "SOURCE_SPEC") and hasattr(module, "run")


# ========================================================================================
# 4. Every register reference - jdbc_ref, storage_ref, cluster, registry - named in any
#    source or environment file exists in its register file
# ========================================================================================

# setting name (as a source file spells it) -> (register file, its top-level key)
REGISTER_FILES: dict[str, tuple[Path, str]] = {
    "cluster": (CONF_ROOT / "clusters.yaml", "clusters"),
    "registry": (CONF_ROOT / "registries.yaml", "registries"),
    "jdbc_ref": (CONF_ROOT / "jdbc.yaml", "jdbc"),
    "storage_ref": (CONF_ROOT / "storage.yaml", "storage"),
}


def _register_names(setting: str) -> set[str]:
    path, top_key = REGISTER_FILES[setting]
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return set((document.get(top_key) or {}).keys())


def _source_file_references(setting: str) -> list[tuple[str, str]]:
    """[(source_key, referenced_name), ...] - skips a source file that does not set this
    setting at all, which is legal (e.g. a Unity Catalog Volume file source, D-13, sets no
    storage_ref by construction)."""
    references = []
    for path in sorted((CONF_ROOT / "sources").glob("*.yaml")):
        if path.name.startswith("_"):
            continue
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        value = (document.get("source") or {}).get(setting)
        if value:
            references.append((path.stem, str(value)))
    return references


def _environment_overlay_references(top_key: str) -> list[tuple[str, str]]:
    """[(environment_name, profile_name), ...] for every profile an environment file
    overlays under `<top_key>:` - the OTHER place a register profile is named besides a
    source file (CORE section 6: "a profile named in an environment file that does not
    exist in its register is an error")."""
    references = []
    for path in sorted((CONF_ROOT / "environments").glob("*.yaml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for name in document.get(top_key) or {}:
            references.append((path.stem, name))
    return references


@pytest.mark.parametrize("setting", sorted(REGISTER_FILES))
def test_every_source_file_register_reference_exists_in_its_register(setting):
    register_path, _ = REGISTER_FILES[setting]
    register = _register_names(setting)
    for source_key, name in _source_file_references(setting):
        assert name in register, (
            f"conf/sources/{source_key}.yaml sets {setting}='{name}', which is not a profile "
            f"in {register_path.name} ({sorted(register)})"
        )


@pytest.mark.parametrize("setting", sorted(REGISTER_FILES))
def test_every_environment_overlay_register_reference_exists_in_its_register(setting):
    register_path, top_key = REGISTER_FILES[setting]
    register = _register_names(setting)
    for environment, name in _environment_overlay_references(top_key):
        assert name in register, (
            f"conf/environments/{environment}.yaml overlays a profile '{name}' under "
            f"'{top_key}:' that is not in {register_path.name} ({sorted(register)})"
        )


# ========================================================================================
# 5. Every SOURCE_SPEC's required_keys appears in structural_keys
#
# STAGE_6_gates.md also asks that structural_keys and operational_keys be DISJOINT sets.
# That does not hold against the current, deliberate architecture, and is not enforced here
# - see docs/build_log/STAGE_6_REPORT.md's "Not reproduced" section for the finding and the
# reasoning: CORE section 4.2's own contracts.py docstring documents a key in BOTH sets as
# the ordinary case ("settable in YAML and overridable at run time without a deploy"), and
# every shipped SOURCE_SPEC (kafka's failure_mode/max_offsets_per_trigger, oracle's
# fetch_size/num_partitions/incremental_mode, file's failure_mode/max_files_per_trigger)
# relies on exactly that overlap. Asserting disjointness here would fail against settled,
# tested, intentional behaviour from Stages 2b-5 - which CORE's own rule ("the fast suite
# must be green before and after every stage... if a stage makes it red, the stage is
# wrong, not the test") says not to do.
# ========================================================================================


def _all_source_specs():
    from kafka_ingest.framework.runner import _SOURCES

    return [(name, module.SOURCE_SPEC) for name, module in _SOURCES.items()]


@pytest.mark.parametrize("source_type, spec", _all_source_specs(), ids=lambda v: v if isinstance(v, str) else "")
def test_every_required_key_is_settable_in_yaml(source_type, spec):
    """A required key that is NOT structural could never be given a platform default (an
    operational-only key cannot be set in conf/defaults*.yaml at all - CORE section 4.2),
    so a fresh environment with no control-table row for this source would fail to resolve
    on every single run. `required_keys` existing at all is what this test protects."""
    missing = spec.required_keys - spec.structural_keys
    assert not missing, (
        f"{source_type}: required_keys {sorted(missing)} are not in structural_keys, so "
        "nothing could ever set them from Git - only an operational override could, and a "
        "missing control-table row is not an error (D-01), so a fresh source would never resolve"
    )


# ========================================================================================
# 6. Every notebook's `kafka_ingest` import resolves against the current source tree
#
# A notebook cannot be executed here - no dbutils, no cluster - so nothing else in this
# suite would catch an import left behind by a rename or a retired module. AST-parsing
# avoids the problem that made this hard to check before: the file is valid Python (the
# `# MAGIC` / `# COMMAND` markers are comments), but RUNNING it fails immediately on the
# first `dbutils` reference, long before a stale import would even be reached.
# ========================================================================================


def _notebook_files() -> list[Path]:
    return sorted(NOTEBOOKS_ROOT.glob("*.py"))


def _kafka_ingest_imports(path: Path) -> list[tuple[str, list[str]]]:
    """[(module, [name, ...]), ...] for every `kafka_ingest` import in one notebook.

    A plain `import kafka_ingest.x.y` carries an empty name list - the module itself is the
    only thing to resolve. A `from kafka_ingest.x import a, b as c` carries the names as
    written (aliases are irrelevant here; it is the SOURCE name being imported that must
    exist, not whatever the notebook calls it afterwards).
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: list[tuple[str, list[str]]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("kafka_ingest"):
            imports.append((node.module, [alias.name for alias in node.names]))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("kafka_ingest"):
                    imports.append((alias.name, []))
    return imports


def _notebook_import_cases() -> list[tuple[Path, str, tuple[str, ...]]]:
    return [(path, module, tuple(names)) for path in _notebook_files() for module, names in _kafka_ingest_imports(path)]


_NOTEBOOK_IMPORT_CASES = _notebook_import_cases()


@pytest.mark.parametrize(
    "path, module, names",
    _NOTEBOOK_IMPORT_CASES,
    ids=[f"{path.name}::{module}" for path, module, _names in _NOTEBOOK_IMPORT_CASES],
)
def test_every_notebook_kafka_ingest_import_resolves(path, module, names):
    """The mechanical substitute `docs/build_log/STAGE_7_REPORT.md` recommended (its own
    "what I would change" item 2): every `kafka_ingest` name a notebook imports must exist
    in the real source tree - not the copy in someone's memory of what Stage 3 looked like.

    A real import, not a name-string comparison: `from kafka_ingest.sources import kafka`
    names a SUBMODULE, which is only importable, never a plain attribute, until something
    has imported it - so the submodule import is attempted as a fallback, exactly the
    resolution order Python itself uses for `from package import name`.
    """
    try:
        imported = importlib.import_module(module)
    except ImportError as exc:
        pytest.fail(f"{path.name}: 'import {module}' does not resolve: {exc}")
    for name in names:
        if hasattr(imported, name):
            continue
        try:
            importlib.import_module(f"{module}.{name}")
        except ImportError:
            pytest.fail(
                f"{path.name}: 'from {module} import {name}' - '{name}' is neither an "
                f"attribute of {module} nor a submodule of it. This import is stale."
            )
