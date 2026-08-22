"""Config-driven Kafka -> Databricks landing/curated ingestion framework.

Scope is Kafka -> landing -> curated. Nothing downstream of curated is in this package.

Module map - read them in this order, it is the order data flows through them:

    config              two-tier config: YAML in Git + Delta control table -> TopicConfig
    security            Key Vault secrets + UC Volume certs -> connection options
    kafka_source        readStream / batch read, primary vs replay positioning
    landing_writer      raw wire bytes + CloudEvent columns -> landing (one table per topic)
    schema_resolver     wire-format parsing + Schema Registry lookup by schema id
    curated_writer      per-writer-schema Avro decode -> curated (one table per topic)
    audit               per-batch, per-layer status rows
    tables              DDL and partitioning for the tables this framework owns
    pipeline            the chained foreachBatch body and the run shapes
    entrypoints         thin argparse wrappers, one per job

Start with docs/DESIGN.md for the file lineage and dependency graph.
"""

__version__ = "0.2.0"
