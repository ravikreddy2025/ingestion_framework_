# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Run the unit tests on this cluster
# MAGIC
# MAGIC Every test runs here - including the Spark-backed ones that need a JDK and the
# MAGIC `spark-avro` jar to run on a laptop. DBR ships both.
# MAGIC
# MAGIC **No test connects to Kafka, reads a secret, or writes to a table.** Safe on any cluster.
# MAGIC
# MAGIC Running these on your target DBR is the only way to confirm the `from_avro`
# MAGIC reader/writer schema behaviour on **your** runtime rather than on a local Spark build.

# COMMAND ----------

# MAGIC %pip install pytest pyyaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, f"{REPO_ROOT}/src")
sys.path.insert(0, f"{REPO_ROOT}/tests")   # so `from conftest import FakeSpark` resolves
os.chdir(REPO_ROOT)
print("REPO_ROOT:", REPO_ROOT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fast gate - no Spark needed
# MAGIC Config resolution, auth option construction, registry client, shipped YAML validation.
# MAGIC This is the suite to wire into CI.

# COMMAND ----------

import pytest

exit_code = pytest.main(["-q", "-m", "not spark", "tests"])
assert exit_code == 0, f"pytest failed with exit code {exit_code}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full suite - includes the Spark-backed tests
# MAGIC Confluent wire-format byte parsing, the mixed-writer-schema microbatch decode,
# MAGIC quarantine splitting, landing projection vs DDL, CloudEvent header extraction,
# MAGIC event_date derivation, and the `from_avro` self-check.

# COMMAND ----------

exit_code = pytest.main(["-q", "tests"])
assert exit_code == 0, f"pytest failed with exit code {exit_code}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The single most important check on a new runtime
# MAGIC Proves that `from_avro`'s positional argument is the **writer** schema and the
# MAGIC `avroSchema` option is the **reader** schema. The curated `payload` struct's shape
# MAGIC depends on this. The ingestion job runs the same check at startup and refuses to
# MAGIC proceed if it fails - running it here tells you *before* you schedule anything.

# COMMAND ----------

from kafka_ingest.curated_writer import assert_from_avro_semantics

assert_from_avro_semantics(spark)
print("from_avro writer/reader schema semantics verified on DBR:",
      spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown"))
