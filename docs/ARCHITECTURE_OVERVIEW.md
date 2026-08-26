# Architecture and Governance Overview

For the client IT and architecture teams. Explains what this framework is, how it is built,
what it needs from your platform, and what it does with your data.

No code reading required. Engineers wanting implementation depth should read
[DESIGN.md](DESIGN.md) and the per-source [DESIGN_KAFKA.md](DESIGN_KAFKA.md) /
[DESIGN_ORACLE.md](DESIGN_ORACLE.md) / [DESIGN_FILES.md](DESIGN_FILES.md);
[NAVIGATION.md](NAVIGATION.md) maps every file in the package.

---

## 1. What it does, in one paragraph

It moves data from three kinds of source — Confluent Kafka topics, Oracle database tables,
and file drops in ADLS or a Unity Catalog Volume — into Databricks Delta tables on a
schedule, through one shared configuration, control, audit and state mechanism. Kafka
messages are written twice: once as **raw bytes exactly as they arrived** (the *landing*
layer, the evidential record), and once **parsed into queryable structure** (the *curated*
layer), from a single read. Oracle and file sources land directly, since a database row or
a file line is already structured. Every run records what it did in a shared *audit* table.
Which sources it processes, and how each behaves, is entirely configuration — onboarding a
source requires no code change.

**Scope boundary:** the framework stops at landing (Oracle, Files) or curated (Kafka).
Business logic, aggregation and data marts are downstream and out of scope.

---

## 2. Data flow

```
  Confluent Kafka topic          Oracle table                ADLS / UC Volume file drop
          |                           |                              |
          |  daily, drains         |  daily, a bounded          |  daily, drains what
          |  what's available      |  cursor/filter window      |  has arrived
          v                           v                              v
  +----------------+          +----------------+           +----------------+
  | write LANDING  |          | read a closed  |           | write LANDING  |
  | (raw bytes)    |          | interval, then |           | (source cols + |
  | parse the same |          | write LANDING  |           | file metadata) |
  | batch, write   |          | then advance   |           +----------------+
  | CURATED        |          | the watermark  |
  +----------------+          +----------------+
          |                           |                              |
          v                           v                              v
     landing + curated           landing table               landing table
     tables (one per topic)   (one per Oracle table)      (one per drop zone)
          \___________________________|______________________________/
                                       v
                          ONE shared audit table, every source
```

Kafka records that cannot be parsed go to a **quarantine** table with their raw bytes
retained, rather than being dropped or silently nulled. The file source has no separate
quarantine table — a row that does not fit the configured schema still lands, with whatever
did not fit captured in a `_rescued_data` column, and its count is reported the same way a
quarantine count is.

### The layers, by source

| Source | Layers | Contains | Who reads it |
|---|---|---|---|
| **Kafka** | landing, curated, quarantine | Landing: message bytes verbatim + Kafka/CloudEvent metadata. Curated: the same metadata plus the parsed message as a nested structure. Quarantine: unparseable records, raw bytes retained. | Landing/quarantine: engineering, recovery and audit only. Curated: downstream data products, analysts. |
| **Oracle** | landing only | Every extracted column plus provenance (when it was read, which run). One table per Oracle table extracted. | Downstream data products, analysts — landing here is the queryable layer, since a database row needs no further parsing. |
| **Files** | landing only | Source file columns verbatim plus file metadata (path, size, modification time) and a rescued-data column for anything that did not fit. | Downstream data products, analysts. |
| **Audit** (shared) | — | One row per run per layer per source: status, row counts, read boundaries, errors. | Support and monitoring, across every source at once. |

---

## 3. Why Kafka gets two layers, and the others get one

The landing layer exists so that **the raw evidence of what arrived is never lost**.

Kafka retains messages for a limited window — typically days. Once that window passes, a
message that was mis-parsed is gone forever. By storing the original bytes on first contact,
the framework can re-parse historical data **years later** against a corrected schema,
without going back to Kafka and without the producing system being involved. In practice
this converts a class of incident that would otherwise mean permanent data loss into a
routine reprocessing job. The curated layer is Kafka's consumable one: same records, parsed
and typed, one table per topic, partitioned by event date.

**Oracle and file sources have no equivalent parsing step to protect against**, which is
why they land only. A JDBC read returns already-typed columns; a structured file format
(CSV/JSON/Parquet/Avro) is read directly into its own schema. There is nothing to "re-parse"
independently of re-reading the source itself — and for Oracle, the source system retains
its own history, which is not true of a Kafka broker's short retention window. This is a
deliberate scope decision (`docs/build_log/DECISIONS.md` D-07), not an oversight: a Oracle
curated layer, or CDC-style transformation, is downstream work this framework does not do.

---

## 4. Environments

The same code and the same source definitions serve dev, preprod and prod. Configuration is
merged from layers, so a source is defined **once** and the environment supplies only what
differs:

| Layer | Holds | Example |
|---|---|---|
| Platform defaults (every source type) | Common to everything | The three shared table names, table properties |
| Platform defaults (one source type) | Common to Kafka, or to Oracle, or to Files | Default fetch size, trigger cadence |
| **Per environment** | **Catalogs, broker/JDBC/storage endpoints, secret scopes** | `platform_dev` vs `platform_prod` |
| Per source | Unique to one feed | Topic name / Oracle table / file path, schema, dedup or merge keys |

Consequences worth noting:

- **No environment-specific branches or forks.** Promotion is a deployment, not a merge.
- **Environments cannot collide.** Each has its own catalog, tables and checkpoints; an
  automated test asserts no two sources — across all three types — share a table or a
  checkpoint path.
- A configuration mistake that would only appear in one environment is caught before
  deployment, because validation resolves every source against every environment.

---

## 5. Security model

### Credentials

**No credential value appears anywhere in the codebase or configuration.** Configuration
files reference secrets *by name only* — a Databricks secret scope and a key within it.
Values are resolved at runtime from **Azure Key Vault** through the Databricks secrets API.
This is uniform across all three source types: Kafka broker and registry credentials,
Oracle database credentials, and ADLS storage credentials are all resolved the same way.

Configuration in Git therefore contains no sensitive material and can be reviewed by anyone
with repository access. Credential-bearing values are masked, by option-name pattern rather
than by a per-system list, before they reach any log line or audit record — one redaction
mechanism covers Kafka options, JDBC connection properties and ADLS session configuration
alike.

Each environment uses its own secret scope, so a dev credential cannot reach production
systems.

### Certificates and connection surfaces

| Source | Credential surface | Notes |
|---|---|---|
| **Kafka** | SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512, mutual TLS. TLS material (truststores, keystores, client certificates) read from **Unity Catalog Volumes only** — any other path (DBFS, workspace files) is a configuration error at startup, not a warning. | Different domains may use entirely different clusters, registries and authentication modes simultaneously; Kafka and Schema Registry credentials are configured, and can differ, independently. |
| **Oracle** | Username/password (`auth_mode: basic`) is the only supported mode today. The account should be read-only on the extracted schemas — this framework only ever issues `SELECT`. | Oracle wallets and Kerberos are deliberately not implemented: each needs a file or a ticket staged on the compute, which is a platform decision, not a configuration guess. |
| **Files** | Either Unity Catalog governance directly (a Volume path — no credential of this framework's own at all), or an ADLS account key / service-principal OAuth credential, resolved the same way as every other secret. | SAS tokens, managed identity and Unity Catalog credential passthrough for the non-Volume path are deliberately not implemented, for the same "verify before building" reason as Oracle's auth modes. |

### Access control (Unity Catalog)

Privileges are deliberately split so that no single identity can both run the pipeline and
change its rules — see the full table in section 6:

| Identity | Data tables (landing / curated) | Audit | Control table | State table | Secrets |
|---|---|---|---|---|---|
| Ingestion service principal | read + write | read + write | **read only** | read + write | read |
| Support team | read | read | **read + write** | **read only** | none |
| Data consumers | read | — | — | — | none |

The ingestion job **cannot disable itself or alter its own replay parameters** (it only
reads the control table). The support team **cannot hand-move a watermark** (it only reads
the state table — a hand-edited watermark is a silent data-loss incident) and **cannot
change table targets or authentication**. Structural changes require a reviewed pull
request.

---

## 6. What the framework needs from your platform

Provisioned once per environment, then once per new Kafka cluster, Oracle database or
storage account.

| Requirement | Detail |
|---|---|
| **Databricks workspace** | Unity Catalog enabled; runtime DBR 16.4 LTS or later |
| **Azure Key Vault** | One per environment, surfaced as Databricks secret scopes |
| **Service principal** | Runs the jobs. Needs READ on secret scopes, READ/WRITE on the checkpoint Volume(s), READ on cert Volumes, MODIFY on target tables, SELECT on the control table, SELECT/MODIFY on the state table |
| **UC Volumes** | Checkpoints (Kafka, Files — read/write), certificates (read), Auto Loader schema locations (Files — read/write) |
| **Network connectivity** | From Databricks compute to every Kafka bootstrap endpoint, every Schema Registry, every Oracle listener, and every ADLS account a `storage_ref`-governed file source uses. On serverless compute this means NCC private endpoints or firewall allowlisting of stable egress IPs. A Unity Catalog Volume file source needs no ADLS network egress at all. |
| **Kafka credentials** | One consumer credential per cluster per environment |
| **Oracle credentials** | One read-only database account per database per environment |
| **Oracle JDBC driver** | **Not bundled with Databricks Runtime, and not installed by this repository.** Must be added as a cluster library or via an init script before the first Oracle run. |
| **Storage credentials** | An ADLS account key or service-principal credential per storage account a non-Volume file source uses |

Storage accounts, external locations and metastore wiring are handled by your Databricks
deployment; the code never references them directly except through the register/environment
mechanism described in [CONFIGURATION.md](CONFIGURATION.md).

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
| `{data_catalog}.landing`, `{data_catalog}.curated` (Kafka; one schema per layer, one table per topic), `{data_catalog}.oracle_<schema>` (Oracle; one schema per source database schema), `{data_catalog}.<target_schema>` (Files; one schema per onboarded drop zone) | Ingestion service principal | `USE SCHEMA`, `CREATE TABLE`, `SELECT, MODIFY` per table |
| The same data-catalog schemas | Support group / data consumers | `SELECT` |

The split within `{control_schema}` is the load-bearing one: the ingestion job can never
disable itself (it only reads `ingest_control`), and support can never hand-move a
watermark (it only reads `ingest_state`). See VB-16 for what remains to confirm once a
workspace exists to check it against.

### Two prerequisites that are commonly missed

**Executor visibility of certificates (mTLS Kafka clusters only).** Keystores and
truststores are opened by the Kafka client running on Spark *executors*, not the driver.
Volume paths must be readable from executors on the chosen compute configuration. A
supplied notebook verifies this in about thirty seconds; it should be run before onboarding
any mTLS topic.

**Serverless network reachability is not assumed** for on-prem Kafka clusters, on-premise
Oracle databases, or any storage account a `storage_ref`-governed file source uses. For
systems reachable only over private networking, connectivity must be confirmed per system,
or those sources run on classic compute instead. This is called out explicitly rather than
discovered at go-live.

---

## 7. Operating model

| Aspect | Detail |
|---|---|
| Schedule | Daily per source, via Databricks Workflows. Kafka, Oracle and Files each run through their own job (different schedule negotiation, different blast radius of a stuck run — a stuck Oracle run holds sessions open on someone else's production database; a stuck Kafka run only costs lag) |
| Execution | Kafka and Files: drain everything currently available, then stop. Oracle: read a bounded window since the last successful run. All three have a natural end. |
| Retries | Automatic — Kafka/Files up to 3, Oracle up to 2 (a JDBC failure is often the source database being busy, so Oracle retries more slowly). Safe by design (section 8) |
| Failure alerting | Email on job failure, plus a `FAILED` row in the audit table |
| Monitoring | The shared audit table. One SQL query answers "did everything run last night?" across every source type |
| Manual intervention | Support has parameterised jobs and a single shared control table for recovery — no code editing during an incident |

### Change management

| Change type | Route | Effect |
|---|---|---|
| New source; new cluster/database/storage account; table, partition or environment changes | Pull request, review, deployment | Next deployment |
| Enable/disable a source; failure-mode toggle; batch/fetch sizing; the Oracle full/delta switch; a replay | SQL update, or a job parameter | **Next run - no deployment** |

This split is deliberate. Changes with production blast radius get code review. Changes
needed during an incident at 3am do not require a release.

---

## 8. Data integrity guarantees

**Each record is written exactly once**, even when a run fails midway or is retried, for
every source type — by two different mechanisms suited to each source's shape.

**Kafka (streaming).** Spark records which offsets a batch covers *before* processing it,
and marks the batch complete only *after* both writes succeed. A retry therefore reprocesses
**the same messages**, not new ones, and Delta's transactional markers cause an
already-completed write to be skipped rather than repeated.

**Oracle (batch, watermark-based).** The read window is bounded before anything is read
(`cursor > last_watermark AND cursor <= this_run's_high_water_mark`); the watermark is
advanced **only after** the write has committed. A run that fails before that point leaves
the watermark exactly where it was, so the next run re-reads the identical window — a table
declared with a stable key absorbs that safely as an update; one without loses that
guarantee, which is a deliberate, documented per-table decision, never a default.

**Files (streaming, landing-only).** The same checkpoint-and-commit mechanism as Kafka.

Practical consequence: if a Kafka run's landing write succeeds and its curated write fails,
re-running the job completes the curated write **without duplicating the landing data**. No
manual cleanup, no reconciliation job.

Recovery operations are separated by cause, and vary by source type — see each source's own
failure-scenario table for the complete list: [DESIGN_KAFKA.md](DESIGN_KAFKA.md),
[DESIGN_ORACLE.md](DESIGN_ORACLE.md), [DESIGN_FILES.md](DESIGN_FILES.md). In summary:

| Situation | Kafka | Oracle | Files |
|---|---|---|---|
| Data never arrived / a gap | Bounded replay from an offset or timestamp | Re-extract an explicit window (never disturbs the scheduled position) | Re-read via a checkpoint reset (no dedicated replay job — see below) |
| Data arrived but was parsed wrong | A separate replay that re-reads only the retained raw bytes | Not applicable — no separate parse step | Not applicable — no separate parse step |

Replayed or re-extracted records are tagged, so reprocessed data is distinguishable from the
primary run's data by any downstream consumer.

**One item pending verification, common to every source type:** the Delta idempotent-write
mechanism, and the durable run-sequence store batch sources use for the same guarantee, are
taken from Delta's documented contract but have not been measured on the target runtime.
Both are listed as go-live verification steps with defined tests — see
[VERIFICATION_BACKLOG.md](VERIFICATION_BACKLOG.md), entries VB-15 and VB-09.

---

## 9. Data protection considerations

Points worth review by data protection / compliance, per source type.

**Kafka landing retains raw message payloads indefinitely.** If topics carry personal or
sensitive data, that data is stored verbatim, including any fields the curated layer does
not expose. This is intentional — it is what makes historical reprocessing possible — but
it means:

- Landing should be governed at the sensitivity of the **most sensitive topic** it holds
- Erasure requests (e.g. GDPR Article 17) must consider landing, curated **and** the
  quarantine tables, **in every environment**

**Oracle landing retains every extracted version of a row** for a source configured with a
stable key (the merge key includes the cursor column, so a claim's history is preserved as
distinct versions rather than overwritten) — this is a deliberate mirror of what the source
held at each point in time, not an accumulation of duplicates. A source without a stable key
(an explicit, logged waiver) appends instead, which can retain more copies than intended if
it is re-run or switched to a full load.

**File landing retains the source columns verbatim**, plus whatever did not fit the expected
schema in a `_rescued_data` column — a rescued row can therefore contain unexpected or
unvalidated content by construction.

**Retention: the policy is defined, the enforcement is not switched on**, for every source
type sharing the landing pattern. Both halves matter:

| | Status |
|---|---|
| **Policy** | **20 years** for landing rows, set as `landing_retention_days` in `databricks.yml` and overridable per environment. Changing it is a compliance decision, not tuning. |
| **Enforcement** | The `DELETE` that applies it is written and parameterised in `sql/04_maintenance.sql` but is **deliberately commented out**. Nothing deletes landing data today, for any source. |

Enabling automatic deletion of raw payloads was left as an explicit decision for the data
owner rather than shipped as a default. At the 20-year setting the statement would remove
nothing for two decades in any case, so there is time to make that decision deliberately.
Every landing table is partitioned by ingestion date specifically so the deletion is cheap
when it runs.

**Two questions remain open**, and both should be answered before enforcement is enabled:

- Does the 20 years apply to **Kafka's quarantine** tables? They retain full raw payloads,
  so the same reasoning applies, but the requirement was stated for landing only.
- Does it apply to **curated** (Kafka)? Curated is derived and can be rebuilt from landing,
  so it may warrant a shorter window rather than the same one.

**Kafka quarantine tables hold full raw payloads** for records that failed to parse, and
should carry the same access controls as landing.

**The audit table holds no message or row content** — only counts, read boundaries,
timestamps, status and error text, for every source type. Error messages may include Kafka
offsets, Oracle cursor values, file paths and schema identifiers, never payload values.
`source_detail` on an Oracle run's audit row does carry the **exact SQL statement sent to
the database** — useful for incident diagnosis, and worth knowing if that statement's
literal predicates could themselves be considered sensitive in your environment.

**Every source's landing table is scoped to one feed.** Access control can therefore be set
per source, at the table level. If a source requires stricter isolation than its peers,
that is a design conversation before onboarding, not after.

---

## 10. Assurance

| | |
|---|---|
| Automated tests | Run `pytest -m "not spark" -q` to see the exact, current count (it changes as the suite grows) — covering configuration validation for every source type, authentication construction, credential redaction, control-table rules, state-write and audit-write behaviour, table-write idempotency mechanics, and every source's own parsing/extraction/read logic |
| Runs where | Locally and on a Databricks cluster. **No test connects to Kafka, Oracle or ADLS, reads a secret, or writes to a real table** |
| Configuration validation | A pre-merge test resolves every shipped source against **every environment**, for every source type — catching wrong catalog names, invalid connection references, non-compliant certificate paths, unresolved placeholders and colliding checkpoints or tables before deployment |
| Runtime self-check | Kafka's job verifies a critical schema-resolution assumption at startup and refuses to run if the runtime behaves unexpectedly, rather than risking silently incorrect parsing |
| Safety guard | Kafka and File jobs refuse to start in the one state that would cause silent data loss (a deleted checkpoint over existing data). Oracle's watermark-then-write ordering gives the equivalent guarantee for a batch source. |
| Extensibility gate | A CI check fails the build if the shared framework code ever comes to name a specific source type outside its one dispatch point — the mechanical proof that a fourth source type can be added without touching the spine every other source depends on. |

---

## 11. Deliberate limitations

Stated so they are decisions rather than surprises:

| Limitation | Rationale |
|---|---|
| Kafka: Avro via Confluent Schema Registry only | The only format in scope. Other formats fail with a clear error rather than being mis-parsed |
| Oracle: no curated layer, no CDC | A JDBC extract already returns typed columns; modelling and change-data-capture are downstream concerns (`docs/build_log/DECISIONS.md` D-07) |
| Oracle: username/password authentication only | Wallets and Kerberos each need platform-level staging this project cannot verify exists on the target runtime — a future code change, not a config guess |
| Files: no separate quarantine table | A row that does not fit lands with its unmatched fields captured, rather than being routed elsewhere — the same information, a smaller design |
| Files: no replay job | A missing checkpoint already causes a full re-read on its own, files persist in the source zone (unlike Kafka's broker retention), and there is no separate parse step to re-run — the checkpoint-reset procedure is the recovery path instead (`docs/build_log/DECISIONS.md` D-10) |
| Files: only account-key and service-principal storage authentication (or full Unity Catalog governance via a Volume path) | SAS tokens and managed identity need platform-level wiring this project cannot verify — a future code change, not a config guess (`docs/build_log/DECISIONS.md` D-12) |
| Records are not split or fanned out (Kafka) | Curated is strictly one row per Kafka message, which is what makes reprocessing repeatable |
| Landing retention is defined (20 years) but **not enforced**, for every source | The `DELETE` exists and is parameterised; switching it on is a data-owner decision. See "Data protection" above |
| Daily schedule | Configurable per source if a feed needs to be more frequent |

---

## 12. Glossary

| Term | Meaning |
|---|---|
| **Source** | One configured feed — a Kafka topic, an Oracle table, or a file drop zone. Identified by its `source_key`. |
| **Landing** | Raw/first-contact layer. Kafka: message bytes exactly as they arrived. Oracle/Files: the extracted rows or file rows, largely as read, plus provenance columns |
| **Curated** | Kafka only. Parsed layer, one table per topic |
| **Quarantine** | Kafka only. Records that could not be parsed, raw bytes retained |
| **Rescued data** | Files only. A column holding whatever did not fit the configured schema for a landed row — this source's equivalent of quarantine |
| **Audit** | The one shared operational record of every run, every layer, every source type |
| **Control table** | The one shared table support edits at runtime to change a source's behaviour, no deploy |
| **State / watermark** | Oracle only (and the durable run-sequence every batch-style run allocates). Where an incremental extract last got to; advanced only after a committed write |
| **Topic** | A Kafka feed |
| **Offset** | A Kafka message's position in a topic partition |
| **Cursor** | Oracle: the column (usually a timestamp) an incremental extract reads forward from |
| **Checkpoint** | Kafka and Files. Where the job records how far it has read. Deleting one is dangerous |
| **Replay** | Deliberate reprocessing — from Kafka, from Kafka's own landing table, or a bounded Oracle re-extraction. Files has no replay job; recovery uses the checkpoint-reset procedure instead |
| **Schema Registry** | Confluent service holding the schemas Kafka messages are encoded with |
| **CloudEvents** | An industry standard for event metadata, carried in Kafka headers |
| **Auto Loader** | The Databricks mechanism the file source uses to incrementally discover and read new files |
| **Unity Catalog** | Databricks governance layer for data, files and access |
