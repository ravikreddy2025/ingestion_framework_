"""sources/file/config.py - value-level validation the spec cannot express.

framework/config.py validates KEYS; this module validates VALUES - enumerations,
cross-field rules, and the two things STAGE_5's exit gate names explicitly: an unknown
`format_options` key, and a `filename_columns` regex that does not compile or does not
have exactly one capture group.
"""

from __future__ import annotations

import pytest

from conftest import make_file_cfg, write_file_source
from kafka_ingest.framework.config import ConfigError


def _cfg(file_config_root, **settings):
    write_file_source(file_config_root, **settings)
    return make_file_cfg(file_config_root)


# --------------------------------------------------------------------------------------
# schema_mode / schema
# --------------------------------------------------------------------------------------


def test_schema_mode_provided_requires_a_schema(file_config_root):
    with pytest.raises(ConfigError, match="requires `schema:`"):
        _cfg(file_config_root, schema_mode="provided", schema=None)


def test_schema_is_rejected_outside_provided_mode(file_config_root):
    with pytest.raises(ConfigError, match="only applies under schema_mode"):
        _cfg(file_config_root, schema_mode="infer", schema="a STRING")


def test_infer_mode_needs_no_schema(file_config_root):
    cfg = _cfg(file_config_root, schema_mode="infer", schema=None)
    assert cfg.schema_mode == "infer"
    assert cfg.schema is None


def test_an_unknown_schema_mode_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="schema_mode"):
        _cfg(file_config_root, schema_mode="guess")


# --------------------------------------------------------------------------------------
# format_options - known set per file_format
# --------------------------------------------------------------------------------------


def test_a_known_csv_option_is_accepted(file_config_root):
    cfg = _cfg(file_config_root, format_options={"header": "true", "delimiter": "|"})
    assert cfg.format_options == {"header": "true", "delimiter": "|"}


def test_an_unknown_format_option_is_rejected_and_lists_the_known_set(file_config_root):
    with pytest.raises(ConfigError) as exc:
        _cfg(file_config_root, format_options={"delimeter": "|"})
    message = str(exc.value)
    assert "delimeter" in message
    assert "delimiter" in message


def test_an_option_valid_for_one_format_is_rejected_for_another(file_config_root):
    """`avroSchema` means something for Avro and nothing for CSV - Spark would silently
    ignore it, which is exactly the failure this validation exists to catch."""
    with pytest.raises(ConfigError, match="avroSchema"):
        _cfg(file_config_root, file_format="csv", format_options={"avroSchema": "..."})


# --------------------------------------------------------------------------------------
# filename_columns - compiled and shaped at config load
# --------------------------------------------------------------------------------------


def test_a_filename_column_with_one_capture_group_is_accepted(file_config_root):
    cfg = _cfg(file_config_root, filename_columns={"business_date": r"claims_(\d{8})\.csv"})
    assert cfg.filename_columns[0].column == "business_date"
    assert cfg.filename_columns[0].pattern == r"claims_(\d{8})\.csv"


def test_a_filename_column_regex_that_does_not_compile_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="does not compile"):
        _cfg(file_config_root, filename_columns={"business_date": r"claims_(\d{8"})


def test_a_filename_column_regex_with_zero_capture_groups_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="capture group"):
        _cfg(file_config_root, filename_columns={"business_date": r"claims_\d{8}\.csv"})


def test_a_filename_column_regex_with_two_capture_groups_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="capture group"):
        _cfg(file_config_root, filename_columns={"business_date": r"claims_(\d{4})(\d{4})\.csv"})


# --------------------------------------------------------------------------------------
# storage_ref - the register
# --------------------------------------------------------------------------------------


def test_a_storage_ref_naming_a_non_existent_profile_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="adls_demo"):
        _cfg(file_config_root, storage_ref="does_not_exist")


def test_the_account_key_auth_mode_requires_its_secret_key_names(file_config_root):
    """StorageProfile validates on construction - a register entry missing a required key
    name fails when the profile is built, not when a secret lookup finally fails at read
    time."""
    import yaml

    storage_path = f"{file_config_root}/storage.yaml"
    with open(storage_path, encoding="utf-8") as handle:
        doc = yaml.safe_load(handle)
    doc["storage"]["adls_demo"].pop("account_key_secret_key")
    with open(storage_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(doc, handle)
    with pytest.raises(ConfigError, match="account_key_secret_key"):
        _cfg(file_config_root)


# --------------------------------------------------------------------------------------
# access_mode: volume | adls (docs/build_log/DECISIONS.md D-15, supersedes D-13's
# shape-inference) - an explicit, mode-conditional choice: "volume" takes volume_path and
# no storage_ref/source_path; "adls" takes storage_ref + source_path and no volume_path.
# --------------------------------------------------------------------------------------

VOLUME_PATH = "/Volumes/cat_dev/files_claims/landing/claims/inbound/"


def _volume_cfg(file_config_root, **overrides):
    settings = {"access_mode": "volume", "volume_path": VOLUME_PATH, "storage_ref": None, "source_path": None}
    settings.update(overrides)
    return _cfg(file_config_root, **settings)


def test_a_volume_source_needs_no_storage_ref(file_config_root):
    cfg = _volume_cfg(file_config_root)
    assert cfg.access_mode == "volume"
    assert cfg.storage_ref is None
    assert cfg.storage is None


def test_a_volume_source_is_read_as_is_with_no_abfss_wrapping(file_config_root):
    cfg = _volume_cfg(file_config_root)
    assert cfg.full_source_path == VOLUME_PATH


def test_a_volume_source_with_storage_ref_set_is_rejected(file_config_root):
    """A source declaring both leaves no honest answer to which one governs the read."""
    with pytest.raises(ConfigError, match="'storage_ref' is set, but access_mode is 'volume'"):
        _volume_cfg(file_config_root, storage_ref="adls_demo")


def test_a_volume_source_with_source_path_set_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="'source_path' is set, but access_mode is 'volume'"):
        _volume_cfg(file_config_root, source_path="claims/inbound/")


def test_a_volume_source_requires_volume_path(file_config_root):
    with pytest.raises(ConfigError, match="requires `volume_path:`"):
        _volume_cfg(file_config_root, volume_path=None)


@pytest.mark.parametrize(
    "bad_path",
    [
        "/Volumes/cat_dev",
        "/Volumes/cat_dev/",
        "/Volumes/cat_dev/files_claims",
        "not/even/a/volumes/path",
    ],
)
def test_a_malformed_volume_path_is_rejected(file_config_root, bad_path):
    with pytest.raises(ConfigError, match="does not match"):
        _volume_cfg(file_config_root, volume_path=bad_path)


def test_an_adls_source_still_requires_storage_ref(file_config_root):
    with pytest.raises(ConfigError, match=r"requires \['storage_ref'\]"):
        _cfg(file_config_root, storage_ref=None)  # source_path stays the default container path


def test_an_adls_source_with_volume_path_set_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="'volume_path' is set, but access_mode is 'adls'"):
        _cfg(file_config_root, volume_path=VOLUME_PATH)


def test_an_unknown_access_mode_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="access_mode"):
        _cfg(file_config_root, access_mode="dbfs")


def test_the_default_source_is_adls_mode(file_cfg):
    assert file_cfg.access_mode == "adls"
    assert file_cfg.volume_path is None


# --------------------------------------------------------------------------------------
# source_path - never a full URL
# --------------------------------------------------------------------------------------


def test_source_path_containing_a_scheme_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="full URL"):
        _cfg(file_config_root, source_path="abfss://landing@acct.dfs.core.windows.net/x/")


def test_full_source_path_is_built_from_the_storage_profile(file_cfg):
    assert file_cfg.full_source_path == "abfss://landing@acct-dev.dfs.core.windows.net/claims/inbound/"


# --------------------------------------------------------------------------------------
# listing_mode / failure_mode enumerations
# --------------------------------------------------------------------------------------


def test_an_unknown_listing_mode_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="listing_mode"):
        _cfg(file_config_root, listing_mode="magic")


def test_an_unknown_failure_mode_is_rejected(file_config_root):
    with pytest.raises(ConfigError, match="failure_mode"):
        _cfg(file_config_root, failure_mode="RETRY")


def test_checkpoint_root_must_be_volume_backed(file_config_root):
    """checkpoint_root normally comes from conf/defaults/file.yaml; a source can still
    override it, and a non-Volume path must fail here rather than at the first
    `.option("checkpointLocation", ...)` call on a cluster."""
    with pytest.raises(ConfigError, match="Volume-backed"):
        _cfg(file_config_root, checkpoint_root="/dbfs/not-a-volume")


# --------------------------------------------------------------------------------------
# The checkpoint-reset id
# --------------------------------------------------------------------------------------


def test_checkpoint_reset_id_must_match_the_safe_id_pattern(file_config_root):
    with pytest.raises(ConfigError, match="checkpoint_reset_id"):
        make_file_cfg(file_config_root, checkpoint_reset_id="not a safe id!")


def test_checkpoint_reset_id_reaches_the_config_from_job_parameters(file_config_root):
    cfg = make_file_cfg(file_config_root, checkpoint_reset_id="INC-42")
    assert cfg.checkpoint_reset_id == "INC-42"
    assert "INC-42" in cfg.txn_app_id
