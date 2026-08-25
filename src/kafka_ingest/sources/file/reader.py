"""Building the Auto Loader (`cloudFiles`) stream reader.

FOUR OPTIONS ARE NOT NEGOTIABLE, for the same reason Kafka's four are (sources/kafka/
reader.py):

  cloudFiles.schemaLocation      always set, regardless of schema_mode. It is a
                                 checkpoint-like resource with its own lifecycle (STAGE_5
                                 brief) and Auto Loader uses it for more than inference
                                 bookkeeping even when a schema is provided.
  cloudFiles.maxFilesPerTrigger  always set. Unset means one microbatch for the entire
                                 backlog under `availableNow` - a first run over a landing
                                 zone with years of files becomes one enormous batch whose
                                 failure costs the whole run.
  cloudFiles.rescuedDataColumn   always "_rescued_data", not configurable. It is this
                                 source's quarantine signal - see run.py - and a knob that
                                 turned it off would silently disable that signal.
  pathGlobFilter                 always set from `path_glob`, even when it is "*".

`schema_mode` maps onto Auto Loader as follows. The STAGE_5 brief's validation rule -
`schema` is required for `provided` and REJECTED otherwise - is read literally, so `hints`
does not carry a value in this stage; it is `cloudFiles.inferColumnTypes` rather than
`cloudFiles.schemaHints`, which is documented as narrowing broad string-biased inference
towards real types without a human naming individual columns. Flagged in the stage report
as a place the brief was ambiguous rather than asserted silently.
"""

from __future__ import annotations

from typing import Any

from .config import SCHEMA_HINTS, SCHEMA_PROVIDED, FileConfig
from .landing import RESCUED_DATA_COLUMN


def build_source_options(cfg: FileConfig) -> dict[str, str]:
    """Every `cloudFiles.*` and reader option this source always sets."""
    options: dict[str, str] = {
        "cloudFiles.format": cfg.file_format,
        "cloudFiles.schemaLocation": cfg.schema_location_path,
        "cloudFiles.maxFilesPerTrigger": str(cfg.max_files_per_trigger),
        "cloudFiles.rescuedDataColumn": RESCUED_DATA_COLUMN,
        "cloudFiles.useNotifications": str(cfg.listing_mode != "directory").lower(),
        "pathGlobFilter": cfg.path_glob,
    }
    if cfg.schema_mode == SCHEMA_HINTS:
        options["cloudFiles.inferColumnTypes"] = "true"
    options.update(cfg.format_options)
    return options


def build_stream_reader(spark: Any, cfg: FileConfig) -> Any:
    """The Auto Loader readStream, with the provided schema applied when there is one."""
    reader = spark.readStream.format("cloudFiles")
    for key, value in build_source_options(cfg).items():
        reader = reader.option(key, value)
    if cfg.schema_mode == SCHEMA_PROVIDED:
        # A DDL column-list string - DataFrameReader.schema() accepts one directly, the
        # same call Kafka and Oracle never need because their shapes are never inferred.
        reader = reader.schema(cfg.schema)
    return reader.load(cfg.full_source_path)
