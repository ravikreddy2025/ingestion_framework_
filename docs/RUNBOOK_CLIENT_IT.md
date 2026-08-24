# Architecture and Governance Overview

For the client IT and architecture teams. Explains what this framework is, how it is built,
what it needs from your platform, and what it does with your data.

No code reading required. Engineers wanting implementation depth should read
[DESIGN.md](DESIGN.md); [NAVIGATION.md](NAVIGATION.md) maps every file in the package.

---

## 1. What it does, in one paragraph

It moves events from Confluent Kafka into Databricks Delta tables on a daily schedule. It
writes each message twice: once as **raw bytes exactly as they arrived** (the *landing*
layer, the evidential record), and once **parsed into queryable structure** (the *curated*
layer). Both writes happen inside a single Spark streaming job, from a single read of Kafka.
Every batch records what it did in an *audit* table. Which topics it processes, and how each
behaves, is entirely configuration - adding a topic requires no code change.

**Scope boundary:** the framework stops at curated. Business logic, aggregation and data
marts are downstream and out of scope.

---

## 2. Data flow

```
  Confluent Kafka topic
          |
          |  one read, once per day (drain what is there, then stop)
          v
  +-------------------------------------------------------------+
  |  ONE Spark streaming job, per microbatch:                   |
  |                                                             |
  |   1. write LANDING   raw bytes, verbatim                    |
  |   2. record audit    landing COMPLETED, row count           |
  |   3. parse the same in-memory batch (Avro, via registry)    |
  |   4. write CURATED   structured, payload kept nested        |
  |   5. record audit    curated COMPLETED, row count           |
  +-------------------------------------------------------------+
          |                        |                    |
          v                        v                    v
     landing tables           curated tables       audit table
     (one per topic)          (one per topic)      (one, all topics)
```

Records that cannot be parsed go to a **quarantine** table with their raw bytes retained,
rather than being dropped or silently nulled.

### The three layers

| Layer | Tables | Contains | Who reads it |
|---|---|---|---|
| **landing** | ONE per topic | Kafka message bytes **verbatim**, plus Kafka metadata and CloudEvent attributes | Engineering; recovery and audit only |
| **curated** | ONE per topic | Same metadata, plus the parsed message as a nested structure | Downstream data products, analysts |
| **audit** | ONE, shared | One row per batch per layer: status, row counts, offsets, errors | Support and monitoring |

---

## 3. Why two layers

The landing layer exists so that **the raw evidence of what arrived is never lost**.

Kafka retains messages for a limited window - typically days. Once that window passes, a
message that was mis-parsed is gone forever. By storing the original bytes on first contact,
the framework can re-parse historical data **years later** against a corrected schema,
without going back to Kafka and without the producing system being involved.

In practice this converts a class of incident that would otherwise mean permanent data loss
into a routine reprocessing job.

The curated layer is the consumable one: same records, parsed and typed, one table per
topic, partitioned by event date.

---

## 4. Environments

The same code and the same topic definitions serve dev, preprod and prod. Configuration is
merged from layers, so a topic is defined **once** and the environment supplies only what
differs:

| Layer | Holds | Example |
|---|---|---|
| Platform defaults | Common to everything | Table naming pattern, partitioning, failure mode |
| **Per environment** | **Catalog, broker endpoints, secret scopes** | `platform_dev` vs `platform_prod` |
| Per topic | Unique to one feed | Topic name, schema subject, dedup keys |

Consequences worth noting:

- **No environment-specific branches or forks.** Promotion is a deployment, not a merge.
- **Environments cannot collide.** Each has its own catalog, tables and checkpoints; an
  automated test asserts no two environments share any of them.
- A configuration mistake that would only appear in one environment is caught before
  deployment, because validation resolves every topic against every environment.

---

## 5. Security model

### Credentials

**No credential value appears anywhere in the codebase or configuration.** Configuration
files reference secrets *by name only* - a Databricks secret scope and a key within it.
Values are resolved at runtime from **Azure Key Vault** through the Databricks secrets API.

Configuration in Git therefore contains no sensitive material and can be reviewed by anyone
with repository access. Credential-bearing values are masked before they reach any log line
or audit record.

Each environment uses its own secret scope, so a dev credential cannot reach production
brokers.

### Certificates

TLS material (truststores, keystores, client certificates) is read from **Unity Catalog
Volumes**. The framework **rejects** any certificate path that is not on a Volume -
including DBFS and workspace file paths - as a configuration error at startup, not as a
warning. This is enforced in code, not left to convention.

### Authentication modes supported

Per Kafka cluster, and per Schema Registry independently:

| | Supported |
|---|---|
| Kafka | SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512, mutual TLS |
| Schema Registry | none, HTTP basic, mutual TLS |

Different domains can use entirely different clusters, registries and authentication modes
simultaneously. The framework does not assume a shared credential between Kafka and its
registry - those are configured separately by design.

### Access control (Unity Catalog)

Privileges are deliberately split so that no single identity can both run the pipeline and
change its rules:

| Identity | Landing / curated | Audit | Control table | Secrets |
|---|---|---|---|---|
| Ingestion service principal | read + write | read + write | **read only** | read |
| Support team | read | read | **read + write** | none |
| Data consumers | read (curated) | - | - | none |

The ingestion job **cannot disable itself or alter its own replay parameters**. The support
team **cannot change table targets or authentication**. Structural changes require a
reviewed pull request.

---

## 6. What the framework needs from your platform

Provisioned once per environment, then once per new Kafka cluster.

| Requirement | Detail |
|---|---|
| **Databricks workspace** | Unity Catalog enabled; runtime DBR 13.3 LTS or later |
| **Azure Key Vault** | One per environment, surfaced as Databricks secret scopes |
| **Service principal** | Runs the jobs. Needs READ on secret scopes, READ/WRITE on the checkpoint Volume, READ on cert Volumes, MODIFY on target tables, SELECT on the control table |
| **UC Volumes** | One for checkpoints (read/write), one for certificates (read) |
| **Network connectivity** | From Databricks compute to **every** Kafka bootstrap endpoint **and every** Schema Registry. On serverless compute this means NCC private endpoints or firewall allowlisting of stable egress IPs |
| **Kafka credentials** | One consumer credential per cluster per environment |

Storage accounts, external locations and metastore wiring are handled by your Databricks
deployment; the code never references them.

### Unity Catalog privileges

**Provisioned via Terraform, outside this repository.** Neither the framework code nor the
SQL scripts under `sql/` issue a `GRANT` - the identities below are environment-specific
and a job that can grant privileges is a job that can grant itself more. This table is the
specification for whoever writes that Terraform, not a suggestion: it is exactly what the
(now-removed) `GRANT` statements used to say, moved here so it is reviewable without
reading SQL.

Three schemas live under the ops catalog (`{ops_catalog}`), split by purpose rather than by
data-vs-operational - `{audit_schema}` / `{control_schema}` / `{logs_schema}` are each an
environment variable, defaulting to `audit` / `ingestion` / `logs` respectively:

| Catalog / schema | Principal | Privileges |
|---|---|---|
| `{ops_catalog}` | Ingestion service principal | `USE CATALOG` |
| `{ops_catalog}` | Support group | `USE CATALOG` |
| `{ops_catalog}.{control_schema}` (`ingest_control`, `ingest_state`) | Ingestion service principal | `USE SCHEMA`; `SELECT` on `ingest_control`; `SELECT, MODIFY` on `ingest_state`; `CREATE TABLE` on the schema, so a freshly provisioned environment can bootstrap `ingest_state` on its first run (see VB-16) |
| `{ops_catalog}.{control_schema}` | Support group | `USE SCHEMA`; `SELECT, MODIFY` on `ingest_control`; `SELECT` only on `ingest_state` - a hand-edited watermark is a silent data-loss incident |
| `{ops_catalog}.{audit_schema}` (`ingest_audit`) | Ingestion service principal | `USE SCHEMA`, `CREATE TABLE`, `SELECT, MODIFY` |
| `{ops_catalog}.{audit_schema}` | Support group | `USE SCHEMA`, `SELECT` |
| `{ops_catalog}.{logs_schema}` | - | Reserved. Nothing is written here yet, so nothing needs granting until structured logging ships |
| `{data_catalog}` | Ingestion service principal | `USE CATALOG` |
| `{data_catalog}.landing`, `{data_catalog}.curated` (one schema per layer, one table per topic) | Ingestion service principal | `USE SCHEMA`, `CREATE TABLE`, `SELECT, MODIFY` per topic table |
| `{data_catalog}.landing`, `{data_catalog}.curated` | Support group / data consumers | `SELECT` |

The split within `{control_schema}` is the load-bearing one: the ingestion job can never
disable itself (it only reads `ingest_control`), and support can never hand-move a
watermark (it only reads `ingest_state`). See VB-16 for what remains to confirm once a
workspace exists to check it against.

### Two prerequisites that are commonly missed

**Executor visibility of certificates (mTLS topics only).** Keystores and truststores are
opened by the Kafka client running on Spark *executors*, not the driver. Volume paths must
be readable from executors on the chosen compute configuration. A supplied notebook verifies
this in about thirty seconds; it should be run before onboarding any mTLS topic.

**Serverless network reachability is not assumed.** For clusters reachable only over private
networking, connectivity must be confirmed per cluster, or those topics run on classic
compute instead. This is called out explicitly rather than discovered at go-live.

---

## 7. Operating model

| Aspect | Detail |
|---|---|
| Schedule | Once daily per topic, via Databricks Workflows |
| Execution | Drains everything on the topic, then stops - a run has a natural end |
| Retries | Automatic, up to 2, on the primary job. Safe by design (section 8) |
| Failure alerting | Email on job failure, plus a FAILED row in the audit table |
| Monitoring | The audit table. One SQL query answers "did everything run last night?" |
| Manual intervention | Support has parameterised jobs for recovery - no code editing during an incident |

### Change management

| Change type | Route | Effect |
|---|---|---|
| New topic; new cluster; table, partition or environment changes | Pull request, review, deployment | Next deployment |
| Enable/disable a topic; failure-mode toggle; batch sizing; replay | SQL update, or a job parameter | **Next run - no deployment** |

This split is deliberate. Changes with production blast radius get code review. Changes
needed during an incident at 3am do not require a release.

---

## 8. Data integrity guarantees

**Each Kafka message is written exactly once**, even when a job fails midway or is retried.

Mechanically: Spark records which Kafka offsets a batch covers *before* processing it, and
marks the batch complete only *after* both writes succeed. A retry therefore reprocesses
**the same messages**, not new ones, and Delta's transactional markers cause the
already-completed write to be skipped rather than repeated.

Practical consequence: if the landing write succeeds and the curated write fails, re-running
the job completes the curated write **without duplicating the landing data**. No manual
cleanup, no reconciliation job.

Recovery operations are separated by cause:

| Situation | Operation | Contacts Kafka? | Works after Kafka retention? |
|---|---|---|---|
| Data never arrived | Kafka replay | Yes | No |
| Data arrived but was mis-parsed | Curated replay | **No** | **Yes** |

Replayed records are tagged, so reprocessed data is distinguishable from primary-stream data
by any downstream consumer.

**One item pending verification:** the transactional de-duplication behaviour is taken from
Delta Lake's documented contract but has not been measured on the target runtime. It is
listed as a go-live verification step with a defined test.

---

## 9. Data protection considerations

Points worth review by data protection / compliance:

**Landing retains raw message payloads indefinitely.** If topics carry personal or sensitive
data, that data is stored verbatim, including any fields the curated layer does not expose.
This is intentional - it is what makes historical reprocessing possible - but it means:

- Landing should be governed at the sensitivity of the **most sensitive topic** it holds
- Erasure requests (e.g. GDPR Article 17) must consider landing, curated **and** the
  quarantine tables, **in every environment**

**Retention: the policy is defined, the enforcement is not switched on.** Both halves matter:

| | Status |
|---|---|
| **Policy** | **20 years** for landing rows, set as `landing_retention_days` in `databricks.yml` and overridable per environment. Changing it is a compliance decision, not tuning. |
| **Enforcement** | The `DELETE` that applies it is written and parameterised in `sql/04_maintenance.sql` but is **deliberately commented out**. Nothing deletes landing data today. |

Enabling automatic deletion of raw payloads was left as an explicit decision for the data
owner rather than shipped as a default. At the 20-year setting the statement would remove
nothing for two decades in any case, so there is time to make that decision deliberately.
Landing is partitioned by ingestion date specifically so the deletion is cheap when it runs.

**Two questions remain open**, and both should be answered before enforcement is enabled:

- Does the 20 years apply to the **quarantine** tables? They retain full raw payloads, so the
  same reasoning applies, but the requirement was stated for landing only.
- Does it apply to **curated**? Curated is derived and can be rebuilt from landing, so it may
  warrant a shorter window rather than the same one.

**Quarantine tables hold full raw payloads** for records that failed to parse, and should
carry the same access controls as landing.

**The audit table holds no message content** - only counts, offsets, timestamps, status and
error text. Error messages may include Kafka offsets and schema identifiers, never payload
values.

**Landing is one table per topic.** Access control can therefore be set per feed, at the
table level, not per topic. If a topic requires stricter isolation than its peers, that is a
design conversation before onboarding, not after.

---

## 10. Assurance

| | |
|---|---|
| Automated tests | Over 200 (run `pytest` to see the exact, current count - the number changes as the suite grows), covering configuration validation, authentication construction, credential redaction, schema resolution, parsing correctness, table-write idempotency, and failure handling |
| Runs where | Locally and on a Databricks cluster. **No test connects to Kafka, reads a secret, or writes to a table** |
| Configuration validation | A pre-merge test resolves every shipped topic against **every environment** - catching wrong catalog names, invalid cluster references, non-compliant certificate paths, unresolved placeholders and colliding checkpoints before deployment |
| Runtime self-check | The job verifies a critical schema-resolution assumption at startup and refuses to run if the runtime behaves unexpectedly, rather than risking silently incorrect parsing |
| Safety guard | The job refuses to start in the one state that would cause silent data loss (a deleted checkpoint over existing data) |

---

## 11. Deliberate limitations

Stated so they are decisions rather than surprises:

| Limitation | Rationale |
|---|---|
| Avro via Confluent Schema Registry only | The only format in scope. Other formats fail with a clear error rather than being mis-parsed |
| Curated stops short of business logic | Modelling and aggregation are downstream concerns |
| Records are not split or fanned out | Curated is strictly one row per Kafka message, which is what makes reprocessing repeatable |
| Landing retention is defined (20 years) but **not enforced** | The `DELETE` exists and is parameterised; switching it on is a data-owner decision. See "Data protection" above |
| Daily schedule | Configurable per topic if a feed needs to be more frequent |

---

## 12. Glossary

| Term | Meaning |
|---|---|
| **Landing** | Raw layer. Message bytes exactly as they arrived |
| **Curated** | Parsed layer. One table per topic |
| **Quarantine** | Records that could not be parsed, raw bytes retained |
| **Audit** | Operational record of every batch and layer |
| **Topic** | A Kafka feed |
| **Offset** | A message's position in a Kafka topic partition |
| **Checkpoint** | Where the job records how far it has read. Deleting one is dangerous |
| **Replay** | Deliberate reprocessing, either from Kafka or from landing |
| **Schema Registry** | Confluent service holding the schemas messages are encoded with |
| **CloudEvents** | An industry standard for event metadata, carried in Kafka headers |
| **Unity Catalog** | Databricks governance layer for data, files and access |
