# Configuration Reference

Every setting this framework reads, what it does, and what happens if you get it wrong.
Sections 1-8 describe what is common to every source type and the Kafka source's own
settings; §10 is Oracle, §11 is Files.

**Tier markers used throughout (and mirrored in the YAML files themselves):**

| Marker | Meaning |
|---|---|
| 🔴 **MUST CHANGE** | Environment- or source-specific. The shipped value is a placeholder and **will not work**. |
| 🟡 **NICE TO CHANGE** | Has a sensible default. Tune only with a concrete reason. |
| 🟢 **NO CHANGE REQUIRED** | Framework contract or platform standard. Changing it needs a design discussion, not a config edit. |

---

## 1. The five layers

Configuration is merged from five layers, one of which has a rare optional sub-layer.
**Later always wins, per key. Absent keys fall through to the layer below.**

```
1.  conf/defaults.yaml                 common to every source, of every TYPE, in every environment
1b. conf/defaults/<source_type>.yaml   common to every source of ONE type (kafka | oracle | file)
2.  conf/environments/<env>.yaml       what differs between dev / preprod / prod
      `defaults:`            for every type      `defaults_by_type: <type>:`   for one type
3.  conf/sources/<source_key>.yaml     what is unique to one source
     3a. source:.environments.<env>   RARE - unique to one source IN ONE environment, nested
                                       in the same file. Working example:
                                       conf/sources/vector_patient_events.yaml. See §4.
        ^-- STRUCTURAL: Git, PR-reviewed, takes effect on the next deploy
4. operational control table     support-team overrides
5. Workflows job parameters      one-off overrides
        ^-- OPERATIONAL: takes effect on the NEXT RUN, no deploy
```

**The rule:** structural change = PR. Operational change = SQL `UPDATE`. Adding a source =
neither, beyond one config file.

**The filename is the `source_key`.** It is globally unique across every source type (a
Kafka topic and an Oracle table cannot share one), and it appears in job parameters, the
control table, checkpoint/state keys and every audit row.

### Which layer does a setting belong in?

| Is the value... | Put it in |
|---|---|
| the same for every source of every type | `conf/defaults.yaml` |
| the same for every source of ONE type (e.g. a trigger, a fetch size default) | `conf/defaults/<source_type>.yaml` |
| different per environment (catalog, broker, secret scope, JDBC host, storage account) | `conf/environments/<env>.yaml` |
| unique to one source (topic name, Oracle table, file path, dedup keys) | `conf/sources/<key>.yaml` |
| unique to one source **AND** one environment (rare) | `conf/sources/<key>.yaml`, nested under `environments:` — see §4 |
| something support must change during an incident | the control table |

A setting only one source *type* understands must never go into `conf/defaults.yaml`: every
source of every type is validated against it, so a Kafka-only key there is an unknown key
for every Oracle and file source — exactly the error you want, at the wrong moment.

### Placeholders

Values in layers 1-3 may contain `{placeholder}` tokens:

| Token | Source | Available in |
|---|---|---|
| `{catalog}`, `{ops_catalog}`, `{audit_schema}`, `{control_schema}`, `{logs_schema}`, and any other key under `vars:` | the environment file | source settings, cluster/registry/jdbc/storage profiles |
| `{source_key}` | the source's config filename without `.yaml` | source settings only |
| `{domain}` | the source's `domain:` | source settings only |

An unresolved placeholder is a hard error naming the setting and the token — it never
reaches a cluster. The one declared exception: a source type's `target_tokens`
(`{topic_table}` for Kafka, `{source_schema}`/`{source_table}` for Oracle,
`{target_schema}`/`{target_table}` for Files) are left for the source to fill at the top of
its own `run()`, because only the source knows those values — see `framework/tables.py`.

`{source_key}` and `{domain}` are deliberately **not** available in cluster/registry/jdbc/
storage profiles — those are shared by many sources, so substituting a source name into a
cert path or a JDBC host would silently produce a per-source path where none was intended.

### Table creation — nothing is created by hand

Every table this framework owns is created by the **first run**, with explicit
`CREATE TABLE IF NOT EXISTS` DDL and the configured `table_properties`. Onboarding a source
never needs a DDL step, and no table is ever created with a shape Spark inferred from
whatever the first batch happened to contain.

| Table | Column list comes from |
|---|---|
| Kafka landing, quarantine; Oracle landing; File landing | constants/derivation in that source's own `tables.py` |
| **Kafka curated** | derived on the driver from the Avro **reader schema**, before any row is read — `sources/kafka/curated.py::curated_schema()` runs the real projection over an empty frame, so the table cannot disagree with what the writer produces |
| the shared `ingest_audit` / `ingest_state` / `ingest_control` | `framework/audit.py` / `framework/state.py` / provisioned by `sql/01_operational_config.sql` |

`sql/01_operational_config.sql` and `sql/02_layer_tables.sql` are therefore **optional**.
They exist for provisioning and pre-review, not as an onboarding step.

### Table properties — `table_properties`

One template in `conf/defaults.yaml`, applied to every table the framework creates. Two are
**on by default**:

```yaml
defaults:
  table_properties:
    delta.autoOptimize.optimizeWrite: "true"
    delta.autoOptimize.autoCompact: "true"
```

**Applied at CREATE time only, always.** Editing this file does not alter a table that
already exists, for any property below — use `ALTER TABLE ... SET TBLPROPERTIES` for that.

**Four more ship commented out** in the same block, each with its Delta default and why it
is not on platform-wide. Uncomment inside a **source's own** `table_properties` to opt one
source in — a source overrides the whole map, so copy the block rather than editing the
shared default.

| Property | Off means | Turn it on when |
|---|---|---|
| `delta.enableChangeDataFeed` | No `table_changes()` support | A downstream consumer needs only what changed since a version. Costs storage per write — `ingest_control` already turns this on (it is small); a high-volume landing table usually cannot afford it. |
| `delta.deletedFileRetentionDuration` | Delta's built-in 7 days | An **ad-hoc** `VACUUM` from a notebook needs a non-default floor. The scheduled maintenance job always passes an explicit `RETAIN`, so this is redundant for anything scheduled. **Never** set this near the 20-year `landing_retention_days` — that is row retention, this is file retention. |
| `delta.logRetentionDuration` | Delta's built-in 30 days | `DESCRIBE HISTORY` / time-travel-by-version needs to reach further back than a month. |
| `delta.appendOnly` | Updates/deletes/merges allowed | Almost never, on tables this framework creates. **Do not set it on any layer table** — every one is written via `MERGE` during a replay or a delta load, and this property makes every such write fail outright. |

### Where each file lives

| Surface | File / table | Owner | Effect |
|---|---|---|---|
| Platform defaults, every type | `conf/defaults.yaml` | Platform eng | Next deploy |
| Platform defaults, one type | `conf/defaults/<type>.yaml` | Platform eng | Next deploy |
| Per-environment values | `conf/environments/<env>.yaml` | Platform eng | Next deploy |
| Connection registers | `conf/clusters.yaml` / `registries.yaml` / `jdbc.yaml` / `storage.yaml` | Platform eng | Next deploy |
| Per-source structure | `conf/sources/<source_key>.yaml` | Domain + platform eng | Next deploy |
| Per-source behaviour | `{ops_catalog}.{control_schema}.ingest_control` | **Support team** | **Next run** |
| Deployment targets | `databricks.yml` | Platform eng | Next deploy |
| Job shape / schedule | `resources/*.yml` | Platform eng | Next deploy |
| One-off replay | Workflows UI job parameters | **Support team** | That run only |

### How the environment is chosen

Each job passes `--environment ${bundle.target}`, so the DAB target name **must match** a
`conf/environments/<name>.yaml` file. The shipped set is `dev`, `preprod`, `prod`.

Renaming a bundle target without renaming its environment file makes every job in that
target fail at startup with `unknown environment` — which is the intended behaviour, but
rename both together. `tests/test_shipped_config.py` asserts all three files exist.

> **Note:** the data `catalog` is **not** a bundle variable. It lives in
> `conf/environments/<env>.yaml` under `vars.catalog`, because the *code* needs it to build
> table names. DAB variable substitution does not apply to files under `sync.include` —
> `conf/` is copied to the workspace verbatim.

### "I need to change X" — decision tree

```
Is it an emergency, or does it need to land right now?
├── YES → Can it be expressed as: enable/disable, a standing tuning knob
│         (failure mode, batch/file size, Oracle fetch/partition count), or a replay?
│         ├── YES → UPDATE the operational control table. Done, no deploy.
│         └── NO  → It is structural. PR it. If truly urgent, a Workflows
│                   job parameter overrides the control table for one run.
└── NO  → PR against conf/. Promote dev → preprod → prod.
```

---

## 2. Confluent Kafka cluster config — `conf/clusters.yaml`

One block per **physical cluster**. Multiple topics share a block.

### 🔴 MUST CHANGE

| Key | What it is | If you get it wrong |
|---|---|---|
| `bootstrap_servers` | Broker endpoint(s), comma-separated. Confluent Cloud: *Cluster Settings → Endpoints*. | Job hangs then fails on timeout. Most common onboarding error. |
| `secret_scope` | Databricks secret scope backed by your Azure Key Vault. Not needed for pure mTLS. | `ConfigError` naming the scope at startup. |
| `sasl_username_key` | **Key name** in that scope holding the API key / username. SASL only. | Broker rejects auth; run fails immediately. |
| `sasl_password_key` | **Key name** holding the API secret / password. SASL only. | As above. |
| `truststore_path` | UC Volume path to the JKS/PKCS12 truststore. Required for mTLS; **also required for SASL against a private CA**. | TLS handshake failure. |
| `truststore_password_key` | Key name for the truststore password. | Keystore load error on the executors. |
| `keystore_path` | UC Volume path to the **client** keystore. mTLS only. | mTLS handshake failure. |
| `keystore_password_key` | Key name for the keystore password. mTLS only. | As above. |

> **Nothing in this file is secret.** Only scope and key *names* appear. Values are read at
> runtime via `dbutils.secrets.get()`. If you are pasting a password into a YAML file, stop.

### 🟡 NICE TO CHANGE

| Key | Default | When to change |
|---|---|---|
| `key_password_key` | *unset* | Only when the private key inside the keystore has its own password, distinct from the keystore password. Omit the line otherwise. |
| `truststore_type` / `keystore_type` | `JKS` | Set `PKCS12` if your security team standardised on it. |
| `extra_options` | *empty* | Passed to the consumer with a `kafka.` prefix added. Use for `session.timeout.ms`, `request.timeout.ms`, `client.dns.lookup`. |

### 🟢 NO CHANGE REQUIRED

| Key | Value | Why |
|---|---|---|
| `auth_mode` | `sasl_plain` \| `sasl_scram_sha_256` \| `sasl_scram_sha_512` \| `mtls` | Dictated by how your brokers are configured — **confirm with the Kafka team, do not guess**. The framework derives `security.protocol`, `sasl.mechanism` and the JAAS login module from this one value. |

### Auth mode cheat sheet

| Your cluster is… | `auth_mode` | Needs SASL keys? | Needs truststore? | Needs keystore? |
|---|---|---|---|---|
| Confluent Cloud (API key/secret) | `sasl_plain` | Yes | **No** (public CA) | No |
| Self-managed, SCRAM, public CA | `sasl_scram_sha_512` | Yes | No | No |
| Self-managed, SCRAM, private CA | `sasl_scram_sha_512` | Yes | **Yes** | No |
| Self-managed, mutual TLS | `mtls` | No | Yes | **Yes** |

The third row catches people out: **SASL and a truststore are not mutually exclusive.** If
the broker cert is signed by a corporate CA the JVM does not trust, you need both.

### mTLS prerequisites — verify once per cluster, before onboarding

`ssl.truststore.location` and `ssl.keystore.location` are opened by the Kafka client on the
**executors**, not the driver. UC Volume FUSE paths must therefore be readable from
executors on your compute profile (dedicated access mode, or standard access mode on a
recent DBR). Verify with:

```python
spark.range(4).repartition(4).rdd.map(
    lambda _: __import__("os").path.exists("/Volumes/<cat>/<sch>/certs/<file>.jks")
).collect()
```

All `True` → good. Any `False` → executors cannot see the Volume; do not onboard mTLS
topics on that compute until it is resolved. The documented fallback (`SparkContext.addFile`
staging) is described in `sources/kafka/security.py` but **is not built** — no in-scope
cluster needs it (VB-11).

Schema Registry PEMs are **driver-only** and are not affected by this.

---

## 3. Schema Registry config — `conf/registries.yaml`

Registry auth is **independent** of Kafka auth by design. Same domain, different credential.

### 🔴 MUST CHANGE

| Key | What it is | Notes |
|---|---|---|
| `url` | Registry base URL, **no trailing slash** | Confluent Cloud: *Schema Registry → API endpoint* |
| `secret_scope` | Scope holding the registry credential | May be the same scope as Kafka, but the **keys differ** — an SR API key is a separate credential |
| `username_key` / `password_key` | Key names for the user-info pair | `auth_mode: basic` only |
| `client_cert_path` / `client_key_path` | UC Volume paths to **PEM** files | `auth_mode: mtls` only. PEM, not JKS — this is `requests`, not the JVM |
| `ca_bundle_path` | PEM CA bundle for a private registry | Omit when the registry uses a publicly-trusted cert |

### 🟡 NICE TO CHANGE

| Key | Default | When to change |
|---|---|---|
| `timeout_seconds` | `20` | Raise to `30`+ behind a slow private link |
| `max_retries` | `3` | GETs only, exponential backoff, on 429/5xx |

### 🟢 NO CHANGE REQUIRED

| Key | Value |
|---|---|
| `auth_mode` | `none` \| `basic` \| `mtls` — dictated by the registry's configuration |

---

## 4. Kafka per-source structural config — `conf/sources/<source_key>.yaml`, `source_type: kafka`

Start from [`conf/sources/_TEMPLATE.yaml`](../conf/sources/_TEMPLATE.yaml).

### 🔴 MUST CHANGE

| Key | Notes |
|---|---|
| `topic` | Exact Kafka topic name. Case-sensitive. |
| `domain` | Owning team. Appears in every audit row. |
| `cluster` | Must match a key in `clusters.yaml`. Load fails and lists valid names if wrong. |
| `registry` | Must match a key in `registries.yaml`. Unrelated namespace to `cluster`. |
| `subject` | Usually `<topic>-value` (TopicNameStrategy). **Verify** — RecordNameStrategy subjects look completely different. |
| `min_partitions` | Roughly **4× this topic's partition count**. Confirm the partition count with the producing team at onboarding. The platform default assumes a mid-sized topic; leave it wrong and the topic reads at a fraction of the parallelism it could, silently. Over-setting is cheap — it only ever splits offset ranges, never merges them. |

#### 🔴 MUST-READ — the schedule is not a free choice

**The ingestion schedule must be at most ONE THIRD of this topic's Kafka retention, and you
must confirm that retention with the producing team at onboarding.**

The cadence lives in `resources/job_ingest_primary.yml`, not in this file, which is exactly
why it gets forgotten. A daily job against a 24-hour retention has no margin at all: one
failed run, one long weekend, one paused schedule during a change freeze, and records age
out before they are ever read. What you get then is not an error — with
`fail_on_data_loss: true` the run fails loudly, which is the good case; with it set to
`false` the gap is silent.

One third is the working rule because it survives two consecutive missed runs. If the
producing team cannot state a retention, that is the finding, not a reason to guess.

Table names are **not** set here — see "Table naming" below. `table_name:` is the only
sanctioned per-topic override; `landing_table` / `curated_table` / `quarantine_table` /
`checkpoint_root` all come from the pattern in `conf/defaults/kafka.yaml`.

### 🟡 NICE TO CHANGE

| Key | Default | Notes |
|---|---|---|
| `consumer_group_prefix` | `dbx-{source_key}` | Spark manages its own group; this is the identifiable prefix. Replays get a distinct suffix automatically. |
| `starting_offsets` | `earliest` | **First run only** — the checkpoint wins afterwards. `latest` when history would wrongly re-trigger consumers. |
| `max_offsets_per_trigger` | `1000000` | Splits a backlog into several bounded microbatches under `availableNow` — it does **not** cap what a run consumes. It always has a value: unset would mean the entire backlog arrives as ONE microbatch, so a first run on a retained topic becomes a single enormous batch whose failure costs the whole run. Operationally overridable: `ingest_control.kafka_max_offsets_per_trigger`. |
| `curated_dedup_keys` | *empty* | Collapses duplicates **within one microbatch**. Business fields are nested, so write them as **`payload.<field>`** — a bare name is rejected with a clear error. |
| `reader_schema_id` | — | **Required** when `reader_schema_mode: pinned_id`. |
| `table_name` | *derived* | Override the derived landing/curated/quarantine name only when it collides with another topic's, or reads badly. |
| `table_properties` | *(inherited from `conf/defaults.yaml`)* | Overrides the whole map for this topic. See "Table properties" above. |

#### Partitioning — structural, deliberately NOT operationally overridable

| Key | Default | Notes |
|---|---|---|
| `landing_partition_by` | `[ingest_date]` | Must name at least one column (validated). `topic` is constant inside a per-topic table, so it is not a useful key. |
| `curated_partition_by` | `[event_date]` | `event_date` = date of `ce_time`, falling back to `kafka_timestamp`. |

Delta allows `PARTITIONED BY` **or** `CLUSTER BY`, never both — these tables use
partitioning. Changing a partition column on an existing table requires rewriting it, which
is why this is a PR-only field: the control table and job parameters cannot override it.

### 🟢 NO CHANGE REQUIRED

| Key | Default | Why it is the default |
|---|---|---|
| `trigger` | `availableNow` | Drains what is available then stops — the only trigger giving a scheduled run a natural end. |
| `fail_on_data_loss` | `true` | `false` means silently accepting gaps; needs explicit domain sign-off. |
| `reader_schema_mode` | `registry_latest` | The curated **`payload` struct's shape** follows the subject's latest version; each record is still **decoded** with its own writer schema. |
| `failure_mode` | `FAILFAST` | Loud failure beats silently NULLed fields. Use `QUARANTINE` only for a known, accepted source of bad records. The two values are the same two the CHECK constraint on `ingest_control.kafka_failure_mode` allows — the column and the setting are one lever. |
| `curated_dedup_order_by` | `kafka_timestamp` | Change only if the producer sets no timestamp. |

### Environment-specific overrides for one source — `environments:`

Rare. A fourth structural sub-layer, nested inside the same source file.

**Working example, not a hypothetical** — [`conf/sources/vector_patient_events.yaml`](../conf/sources/vector_patient_events.yaml)
resolves differently in all three shipped environments, and a test in
`tests/test_shipped_config.py` asserts the exact numbers below on every test run, so this
table cannot drift from the file:

| Environment | `max_offsets_per_trigger` | Where it comes from |
|---|---:|---|
| `dev` | `100000` | No override in the source file → falls through to `dev.yaml`'s `defaults_by_type.kafka` |
| `preprod` | `2000000` | Set under `environments: preprod:` in the source file |
| `prod` | `5000000` | Set under `environments: prod:` in the source file |

The shape, inside the source file:

```yaml
source:
  topic: vector.patient.events.v1
  ...
  environments:
    preprod:
      max_offsets_per_trigger: 2000000
    prod:
      max_offsets_per_trigger: 5000000
```

**Full precedence, later always wins:**

```
defaults.yaml -> defaults/kafka.yaml -> environments/<env>.yaml -> sources/<key>.yaml
    -> sources/<key>.yaml: environments.<env> -> operational control table -> job parameters
```

Still layer 3 — still Git, still PR-reviewed, still deployed by DAB. It exists only for a
value specific to **both** one source **and** one environment — a platform-wide environment
difference belongs in `conf/environments/<env>.yaml`, and a runtime toggle belongs in the
control table.

An environment named in the block that does not exist (a typo) is a **hard error at load**,
listing the environments that do. Any key placed inside it is validated exactly like a
normal source setting — a typo there fails the same "unknown keys" check.

### Table naming — Kafka

Landing, curated and quarantine are **one table per topic**, and all three are named from
`{topic_table}` — the Kafka topic name with dots and hyphens turned into underscores,
because neither is legal in an unquoted Unity Catalog identifier:

```
vector.patient.events.v1  ->  <catalog>.landing.vector_patient_events_v1
                              <catalog>.curated.vector_patient_events_v1
                              <catalog>.landing.vector_patient_events_v1_quarantine
```

The patterns live in `conf/defaults/kafka.yaml`. A source file never names a table.

**To override one source's name**, set `table_name:` in that source's file:

```yaml
source:
  topic: vector.patient.events.v1
  table_name: patient_events        # -> <catalog>.landing.patient_events, .curated.patient_events
```

It replaces the derived name in **both** layers and in every environment. It overrides the
NAME only — catalog and schema still come from `conf/defaults/kafka.yaml`. Use it when the
derived name collides with another topic's, or reads badly.

A topic name that cannot become a legal identifier (a leading digit, other punctuation) is a
**hard error at config load**, naming the topic and telling you to set `table_name:`.

### `reader_schema_mode` — pick one

| Mode | The curated `payload` struct | Use when |
|---|---|---|
| `registry_latest` *(default)* | Follow the subject's latest version | Normal operation |
| `pinned_id` | Frozen at `reader_schema_id` | A **contract freeze** while downstream is rebuilt (see `rcm_claim_status.yaml`) |

There is deliberately **no third "use each writer schema as-is" mode**. Curated stores the
payload as one STRUCT column, and two writer versions decoded without a common reader
schema produce two incompatible struct types that cannot share a table.

In **all** modes each record is decoded with its own writer schema from the wire header.
The reader schema controls the output *shape* only, never how bytes are read.

---

## 5. Operational control table — `{ops_catalog}.{control_schema}.ingest_control`

**One table, every source type.** DDL: [`sql/01_operational_config.sql`](../sql/01_operational_config.sql).
Ready-made statements: [`sql/03_support_queries.sql`](../sql/03_support_queries.sql).

A **missing row is not an error** — it means "no overrides, run as coded", so a newly
onboarded source works the moment its YAML merges. Duplicate rows **are** an error.

### Framework-owned columns — mean the same thing for every source type

| Column | Overrides | Typical use |
|---|---|---|
| `enabled` | — | `false` = **emergency stop**. The job runs, reads/consumes nothing, writes a `SKIPPED` audit row so silence is never ambiguous. |
| `replay_rerun_id` | — | **Required for any replay.** Isolates the checkpoint/state (where the source has one) and tags every written row. Also — on a **primary** Kafka/File run — the single-use checkpoint-reset id. |
| `replay_controls` | — | JSON object of structured, source-specific replay parameters, e.g. `{"replay_starting_offsets": "{...}"}`. Validated against that source's own spec on the next run — a typo produces the same error a YAML typo would. |

### Source-type-owned columns, named `<source_type>_<setting>` (D-01)

Because this ONE table is shared by every source type, a column specific to one type is
prefixed rather than bare: `kafka_failure_mode`, not `failure_mode` — a bare name on a
table with an Oracle row and a file row beside it would not say which mechanism it turns.
**Setting a column for the wrong source type is a run-time error naming both**, not a
silent no-op.

| Column | Source type | Overrides | Typical use |
|---|---|---|---|
| `kafka_failure_mode` | kafka | `failure_mode` | Flip to `QUARANTINE` to get a failing topic moving. §9 |
| `kafka_max_offsets_per_trigger` | kafka | `max_offsets_per_trigger` | Lower it to get a huge backlog through in survivable chunks |
| `kafka_checkpoint_reset_id` | kafka | — | Incident-only, single-use. Bypasses the checkpoint-reset guard and forks the Delta transaction identity. §9 |
| `oracle_fetch_size` | oracle | `fetch_size` | Rows per JDBC round trip. §10 |
| `oracle_num_partitions` | oracle | `num_partitions` | Parallel JDBC connections. §10 |
| `oracle_incremental_mode` | oracle | `incremental_mode` | The full-vs-delta switch — the one operational lever that changes *which rows* are extracted. §10 |
| `file_failure_mode` | file | `failure_mode` | Flip to `QUARANTINE` to land a batch holding rescued rows. §11 |
| `file_max_files_per_trigger` | file | `max_files_per_trigger` | Lower it to get a huge backlog through in survivable chunks |
| `file_checkpoint_reset_id` | file | — | Incident-only, single-use. Mirrors `kafka_checkpoint_reset_id` exactly — this source shares Kafka's checkpoint-reset guard. §11 |

**There is no free-form JSON escape hatch for a standing, source-specific setting any more**
(D-01 removed the old `source_overrides` column): a setting either has a dedicated column,
or it is not operationally overridable from this table at all. Adding a source type's
levers is a deploy already (new package, job template, conf files), so it may also need an
`ALTER TABLE ADD COLUMNS` — that is honest, not a regression.

### The checkpoint-reset columns — incident use only, and SINGLE-USE

Kafka's and the File source's checkpoint-reset columns behave identically (they call the
same shared guard, `framework/checkpoint.py`). Unlike every other column in this table,
setting one is **not a toggle** — it is a one-way fork of the source's Delta transaction
identity, which is what makes the restart safe rather than merely permitted: a fresh
identity has no prior committed versions for Delta to silently skip against.

**It is single-use.** Reusing an id keeps the identity the last reset created, against which
Delta already holds high versions, so every write would be skipped as a duplicate and the
run would report success having ingested nothing. The job **refuses to start** in that state
and names the spent id.

**Never blank it back out once set.** Once the checkpoint exists again the field is inert
for the guard — it only forks the app id, which must stay forked. Reverting to `NULL`
reverts to the pre-incident identity and its stale watermark/position.

It is rejected outright in source YAML — the spec declares it operational-only, so a value
in Git is a startup error rather than a bypass that silently re-applies on every future
deploy with no incident behind it.

Full procedures: [`RUNBOOK_SUPPORT.md` §5.4a](RUNBOOK_SUPPORT.md) (Kafka) and
[`RUNBOOK_SUPPORT.md` §9.4](RUNBOOK_SUPPORT.md) (Files).

### Attribution

`notes`, `updated_by`, `updated_at` are set by the support `UPDATE` templates. Delta's own
`DESCRIBE HISTORY` is the backstop if they are not maintained.

---

## 6. Deployment config — `databricks.yml`

### 🔴 MUST CHANGE

| Variable | Notes |
|---|---|
| `workspace.host` (per target) | Your workspace URL. Three placeholders shipped. |
| `ops_catalog` (per target) | Where the control/state/audit tables live. |
| `data_catalog` (per target) | The data catalog — **MUST match `vars.catalog`** in that target's `conf/environments/<env>.yaml`; a test asserts they agree. Needed here only because the maintenance job is pure SQL and cannot read `conf/`. |
| `service_principal` | The principal jobs run as. Needs `READ` on secret scopes, `READ VOLUME` on cert/checkpoint Volumes, `MODIFY` on target tables. |
| `notification_email` | Failure alerts. |
| `sql_warehouse_id` | SQL warehouse that runs the maintenance job. The shipped value is a placeholder and **will not resolve** — set it before deploying `resources/job_maintenance.yml`. |

### 🟡 NICE TO CHANGE — retention

**Two different numbers. Do not make them consistent.** They govern unrelated things and
merging them is expensive in opposite directions.

| Variable | Governs | Default | When to change it |
|---|---|---|---|
| `landing_retention_days` | How long a landing **row** is kept before it is eligible for deletion | `7300` (20 years) | Per environment. Dev has no reason to keep two decades of data. In prod this is a **compliance decision**, not tuning. |
| `vacuum_retention_hours` | How long Delta keeps **superseded data files** — i.e. how far back time travel and recovery still work | `168` (7 days) | Rarely. Raising it costs storage; 7 days is Delta's floor without an explicit override. |

Setting `vacuum_retention_hours` to twenty years would retain every version of every file for
two decades. It is not the retention policy — `landing_retention_days` is.

Note that the retention `DELETE` in `sql/04_maintenance.sql` is **commented out**. The policy
is expressed and parameterised; enabling automatic deletion of raw payloads is the data
owner's decision. See [`docs/ARCHITECTURE_OVERVIEW.md`](ARCHITECTURE_OVERVIEW.md) §"Data protection considerations".

### 🟢 NO CHANGE REQUIRED

`bundle.name`, `artifacts`, `sync.include`, `targets.*.mode`.

The `run_as` / `permissions` split is deliberate: support gets `CAN_MANAGE_RUN` and not
`CAN_MANAGE`, so they can trigger and cancel jobs but cannot edit a job definition. The
permissions are declared at **target** level, so they cover every job in the bundle —
including primary ingestion, which is intended so support can re-run it during an incident.

---

## 7. Job parameters

### Ingestion jobs — `run-ingest`

One job per source *type* (`ingest_primary` for Kafka, `ingest_oracle`, `ingest_file`), one
task per source. Every task passes the same four named parameters:

| Parameter | Default | Notes |
|---|---|---|
| `config_root` | `${workspace.file_path}/conf` | 🟢 |
| `environment` | `${bundle.target}` | 🟢 Never typed twice |
| `source-key` | per task | 🔴 One task per source |
| `job-run-id` | `{{job.run_id}}` | 🟢 For audit correlation |

### Replay jobs — `run-replay`

`--run-type` is **required with no default** — choosing the wrong replay shape during an
incident is the mistake this prevents.

| Parameter | `kafka_replay` | `curated_replay` | `oracle_replay` |
|---|---|---|---|
| `source_key` | required | required | required |
| `rerun_id` | required | required | required |
| `replay_starting_offsets` | one of these two | — | — |
| `replay_starting_timestamp` | one of these two | — | — |
| `replay_ending_offsets` / `replay_ending_timestamp` | optional ⇒ bounded batch | — | — |
| `replay_landing_filter` | — | **required** | — |
| `replay_cursor_start` | — | — | **required** |
| `replay_cursor_end` | — | — | optional ⇒ "from there to now" |

`replay_landing_filter` and `replay_cursor_start` are required deliberately: re-processing
all history by accident is an expensive way to discover you meant one day. There is no
`file_replay` run type — see §11.

### Maintenance job — `maintenance`

| Parameter | Default | Notes |
|---|---|---|
| `catalog` | `${var.data_catalog}` | 🟡 The data catalog the tables live in |
| `ops_catalog` | `${var.ops_catalog}` | 🟡 Where the shared audit table lives |
| `landing_retention_days` | `${var.landing_retention_days}` | 🟡 See §6. Only used by the reporting query today — the `DELETE` is commented out. |
| `vacuum_retention_hours` | `${var.vacuum_retention_hours}` | 🟢 Leave at Delta's default unless storage pressure says otherwise. |

Runs weekly. It compacts and reclaims; it does **not** delete rows. **Oracle's landing table
is not covered** by this job — a known gap, see `resources/job_maintenance.yml`'s own header
comment.

---

## 8. Prerequisites inventory

### Secrets (Azure Key Vault, via a Databricks secret scope)

For every cluster, registry, JDBC and storage profile in `conf/`:

| Scope | Keys | Used by |
|---|---|---|
| `kv-kafka-prod` | `cc-shared-api-key`, `cc-shared-api-secret`, `sr-shared-api-key`, `sr-shared-api-secret` | Confluent Cloud + its registry |
| `kv-kafka-rcm` | `rcm-consumer-username`, `rcm-consumer-password`, `rcm-truststore-password`, `rcm-sr-username`, `rcm-sr-password` | RCM cluster + registry |
| `kv-kafka-antifraud` | `af-truststore-password`, `af-keystore-password`, `af-private-key-password` | Anti-Fraud mTLS |
| `kv-oracle-prod` | `oracle-core-username`, `oracle-core-password` (per `jdbc.yaml` profile) | Oracle JDBC |
| `kv-adls-prod` | `adls-landing-account-key`, or the service-principal client id/secret | File source, `storage_ref`-governed sources only |

The job's service principal needs `READ` on each scope. Verify:

```python
dbutils.secrets.list("kv-kafka-prod")   # lists key NAMES only, never values
```

### Certificates (Unity Catalog Volumes)

Config validation **rejects** any cert path that is not `/Volumes/...` — DBFS and
workspace files are not accepted.

| Path | Format | Read by |
|---|---|---|
| `.../certs/rcm/corp-truststore.jks` | JKS | Kafka client — **executors** |
| `.../certs/rcm/corp-ca-bundle.pem` | PEM | `requests` — driver only |
| `.../certs/antifraud/truststore.jks`, `client-keystore.jks` | JKS | Kafka client — **executors** |
| `.../certs/antifraud/sr-client.pem`, `sr-client-key.pem`, `fraud-ca-bundle.pem` | PEM | `requests` — driver only |

### Checkpoint and schema-location Volumes

One Volume root per checkpoint-based source type (Kafka, Files), each with per-source
sub-paths the framework appends itself. The service principal needs `READ VOLUME` +
`WRITE VOLUME`. The file source's `cloudFiles.schemaLocation` is a **separate** Volume-backed
resource with its own lifecycle — see §11.

### Network (serverless compute only)

Serverless egress must reach **every** bootstrap endpoint, **every** Schema Registry,
**every** Oracle listener, and **every** storage account named by a `storage_ref` (a Unity
Catalog Volume `source_path` needs no ADLS network egress at all), via NCC private
endpoints or firewall allowlisting of the NCC's stable egress IPs. **This is not assumed**
for the on-prem Kafka clusters — confirm per cluster, or run those tasks on classic compute
by swapping `environment_key` for a `job_cluster_key` in the relevant job template.

A registry that is unreachable produces a `SchemaResolutionError` that says so explicitly.

### Oracle JDBC driver

**Not installed by anything in this repository, and not bundled with Databricks Runtime by
default** (VB-22). Install it as a cluster library or via an init script before the first
Oracle run — `resources/job_ingest_oracle.yml`'s header comment says so explicitly.

---

## 9. Validating configuration before you deploy

```bash
pytest tests/test_shipped_config.py -q     # every conf/sources/*.yaml resolves, every environment
pytest tests/test_offline_validation.py -q # YAML parses; every job entrypoint, source_type and
                                            # register reference resolves; required ⊆ structural
databricks bundle validate -t dev
```

`test_shipped_config.py` loads and validates every shipped source file against every
environment, for every source type. Run it in CI on every PR — it catches a typo'd catalog
name, an unknown cluster/registry/jdbc/storage reference, a non-3-tier table name and a DBFS
cert path before any of them reach a cluster.

---

## 10. Oracle sources — `conf/sources/<source_key>.yaml`, `source_type: oracle`

Copy `conf/sources/_TEMPLATE_oracle.yaml` to onboard a table. It carries the four questions
the SOURCE TEAM has to answer first — which column is the cursor and when it is stamped,
what the stable key is, which column the read can be split on, and whether there are LOB /
RAW / INTERVAL / TZ columns. None of them can be answered from the Databricks side.

### 🔴 MUST-READ — `merge_keys` decides whether rows can be silently lost

A cursor extract reads `cursor > last_watermark AND cursor <= high_water`. The upper bound
is always captured at the start of the run, so rows committed *during* the extract are not
half-read. The remaining hazard is the LOWER bound, and `merge_keys` decides it:

| `merge_keys` | Predicate | Write | Property |
|---|---|---|---|
| **set** (recommended) | `>= last_watermark` | MERGE | **Tie-safe.** The boundary is re-read and de-duplicated, so rows sharing a cursor value cannot be lost. |
| **`[]`** (explicit waiver) | `> last_watermark` | append | Faster. **A row committed with exactly the last watermark value, after the previous run passed it, is never extracted.** |

Omitting `merge_keys` on a cursor source is a **startup error**, not a default — silence is
not a decision anybody made. A waived source WARNs on every run, naming the risk.

The merge key is `merge_keys + cursor_column`, so it identifies a **version** of a row, not
the row. That is what keeps landing a retained mirror rather than a current-state table, and
it is why switching between full and delta loads is safe (see `oracle_incremental_mode`).

### 🔴 MUST CHANGE

| Key | What it is |
|---|---|
| `source_schema` | The Oracle schema, e.g. `CLAIMS`. Write it as Oracle holds it (upper case); the target is lower-cased. |
| `source_table` | The Oracle table. **The only key with no platform default.** |
| `jdbc_ref` | A profile name from `conf/jdbc.yaml`. Never a host or a URL. |
| `domain` | Owning team. Appears in every audit row. |

**The target is derived, never configured:** `CLAIMS.CLAIM_HEADER` →
`{catalog}.oracle_claims.claim_header`. The schema must already exist —
`CREATE SCHEMA IF NOT EXISTS <catalog>.oracle_claims;` in every environment, before the
first run. The framework creates tables, never schemas.

### 🟡 NICE TO CHANGE

| Key | Default | Change it when |
|---|---|---|
| `incremental_mode` | `full` | The table is large enough that a full read is not affordable. `cursor` needs `cursor_column` + `cursor_type` + a `merge_keys` decision; `filter` needs `filter_column` + `filter_criteria`. |
| `cursor_column` / `cursor_type` | — | Required by `cursor`. `timestamp` or `number`. |
| `merge_keys` | — | See the MUST-READ block above. |
| `partition_column` + `num_partitions` | `1` (serial) | Always, once the source team names a column. Both or neither: a count without a column reads on ONE executor whatever the cluster size, and the run WARNs about it. |
| `fetch_size` | `10000` | Rows are wide (lower it) or very narrow (raise it). **Never leave it unset — the Oracle driver's own default is TEN rows per round trip.** |
| `columns` | all | Projecting away a LOB / RAW / INTERVAL / TZ column is the usual reason. |
| `filter_column` + `filter_criteria` | — | A standing predicate, e.g. open claims only. **Structural**: it reaches Oracle's parser verbatim, so it can never be overridden from the control table. |
| `dynamic_date_filter` | — | A rolling window, evaluated by **Oracle's** clock. `P<n>D` or `PT<n>H` only — a month is not thirty days to everyone. |
| `sql_query` | — | A join or an expression the keys above cannot express. Mutually exclusive with `columns` / `filter_*` / `dynamic_date_filter`. One `SELECT` (or `WITH … SELECT`); the cursor predicate is still appended, so the cursor column must be in its select list. |
| `query_timeout` | `0` (none) | Once the table's normal run time is known. |
| `session_init` | — | An `ALTER SESSION` this table needs. **Runs once per JDBC connection, i.e. per partition** — keep it cheap and idempotent. |
| `column_types` | — | A column the driver maps wrongly. Read VB-02 / VB-03 first: the default mapping is usually right. |

### 🟢 NO CHANGE REQUIRED

| Key | Why |
|---|---|
| `landing_table` | Derived from `source_schema` / `source_table`. Set it only if the derived name collides. |
| `landing_partition_by` | `ingest_date` — the date a row was WRITTEN. Bounds partition growth and makes retention a partition drop. |
| `table_properties` | Platform-wide, from `conf/defaults.yaml`. |

### Operational overrides — `ingest_control`

Three columns, and one of them is different in kind from every other operational lever in
this framework:

| Column | Setting | Effect |
|---|---|---|
| `oracle_fetch_size` | `fetch_size` | Rows per round trip. Lower it when a run is straining the source. |
| `oracle_num_partitions` | `num_partitions` | Parallel connections. Lower it when a DBA asks for less load. Needs `partition_column` in the source file. |
| `oracle_incremental_mode` | `incremental_mode` | **The full-vs-delta switch.** A recovery lever: a delta load that has been skipping rows is repaired by one full load, without waiting for a PR. |

**Switching to `full` duplicates rows on a source that waived `merge_keys`**, because that
source appends. Where `merge_keys` are set the merge absorbs the re-read and the switch
costs only time. A full run does **not** advance or clear the watermark, so switching back
to `cursor` resumes from the last genuine delta boundary.

Everything that decides what the increment *means* — `cursor_column`, `cursor_type`,
`merge_keys`, `filter_criteria`, `source_schema`, `source_table` — is structural, and an
override of one is ignored and logged.

### Replay parameters — `oracle_replay`

| Parameter | Required | What it is |
|---|---|---|
| `rerun-id` | yes | Tags every row the replay writes, and identifies it in the audit table. |
| `replay-cursor-start` | yes | Inclusive lower bound, **always** — a replay's start is a boundary a human typed. |
| `replay-cursor-end` | no | Upper bound. Omit for "from there to now", which still captures a real high-water mark. |

A replay **never writes `ingest_state`**, so the scheduled delta load keeps its own position
and an incident cannot strand production state at a bound somebody typed once. Both bounds
are operational-only: a bound checked into Git would re-extract the same window forever.

---

## 11. File sources — `conf/sources/<source_key>.yaml`, `source_type: file`

Copy `conf/sources/_TEMPLATE_file.yaml` to onboard a drop zone. It carries the four
questions whoever owns the landing zone has to answer first — the full schema and whether
it drifts, whether files are ever rewritten in place, whether the file name carries data
that belongs in a column, and roughly how many files land per day.

**Use Auto Loader (`cloudFiles`), always `availableNow`** (CORE section 10 — already made,
not a per-source choice). **This source is checkpoint-based**, exactly like Kafka's primary
stream, and reuses the *same* checkpoint-reset mechanism — see the MUST-READ block below
rather than a second design.

### 🔴 MUST-READ — this source shares Kafka's checkpoint-reset guard

Deleting or losing this source's checkpoint restarts Auto Loader's internal batch
numbering from zero. Delta has already recorded higher `txnVersion`s against this source's
app id, so every write is silently **skipped as a duplicate** — the job reports success and
ingests nothing. The framework refuses to start in that state with a message beginning
`REFUSING TO RUN`, exactly Kafka's guard, applied to this source's own checkpoint and
landing table. See [`docs/RUNBOOK_SUPPORT.md` §9](RUNBOOK_SUPPORT.md) for the restart
procedure — it is the same five-step procedure as Kafka's §5.4a, with
`file_checkpoint_reset_id` in place of `kafka_checkpoint_reset_id`.

`cloudFiles.schemaLocation` is a **separate** checkpoint-like resource, covered by the same
guard indirectly rather than directly — see `docs/DESIGN_FILES.md` for the reasoning.

### 🔴 MUST-READ — there is no replay job for this source, and none is planned

`docs/build_log/DECISIONS.md` D-10: decided and settled. A fresh (missing) checkpoint
already makes Auto Loader re-read the whole path on its own
(`cloudFiles.includeExistingFiles` defaults to `true`), files persist in ADLS so there is no
retention window to race against the way a Kafka replay races broker retention, and this
source is landing-only, so there is no re-parse-from-landing shape either.

**The generic `replay_rerun_id` control column does *not*, by itself, cause a re-read for
this source.** It is read in exactly one place — `framework/audit.py` seeds the audit row's
`rerun_id` label from it — and this source has no replay `run_type` to fork a separate
checkpoint namespace the way Kafka's replay does. The lever that actually matters is
**`file_checkpoint_reset_id`**, used together with the checkpoint being genuinely absent
(deleted, or a source_key that has never run). That combination is exactly
`docs/RUNBOOK_SUPPORT.md` §9.3/§9.4's checkpoint-loss procedure — reused deliberately here as
the recovery path, not a second mechanism.

**Recovery, in order — step 1 is not optional:**

1. **Delete the affected landing partition(s) first.** Landing is append-only: a re-read
   without this step appends everything again and silently duplicates the data.
2. Set a fresh, previously unused `file_checkpoint_reset_id`.
3. Run the normal file job.

A **bounded** re-read narrows `source_path` or `path_glob` for that one run — there is no
offset or timestamp window for this source, unlike Kafka's or Oracle's replay bounds.

### 🔴 MUST CHANGE

| Key | What it is |
|---|---|
| `access_mode` | `volume` \| `adls` (`docs/build_log/DECISIONS.md` D-15) — an **explicit** choice, not inferred from `source_path`'s shape. Decides which of the two rows below apply; setting a key that belongs to the other mode is a config error naming the key and the mode. **Prefer `volume` when a Volume is available** — see the onboarding template. |
| `volume_path` | **`access_mode: volume` only.** A Unity Catalog Volume path, `/Volumes/<catalog>/<schema>/<volume>/...` — UC-governed, no `storage_ref`, no credentials this framework applies at all. `storage_ref` and `source_path` are **rejected** in this mode. |
| `storage_ref` / `source_path` | **`access_mode: adls` only.** `storage_ref` is a profile name from `conf/storage.yaml`, never an account URL. `source_path` is the path **within** the container only — never a full `abfss://` URL — resolved against `storage_ref`, which is what lets the same source file resolve to a different account per environment. `volume_path` is **rejected** in this mode. |
| `file_format` | `csv` \| `json` \| `parquet` \| `avro`. |
| `target_schema` / `target_table` | Where this lands. A file feed has no source-side schema/table the way Oracle's does, so these are your own choice, not a derivation — `{catalog}.<target_schema>.<target_table>`. The target schema must already exist: `CREATE SCHEMA IF NOT EXISTS <catalog>.<target_schema>;` in every environment, before the first run. |
| `domain` | Owning team. Appears in every audit row. |

### 🟡 NICE TO CHANGE

| Key | Default | Change it when |
|---|---|---|
| `schema_mode` | `provided` | `provided` needs `schema:` — a DDL column-list string. **`infer` is a source of silent type drift in prod**: a new column, or a value that happens to look like a date in one file's sample, changes the inferred schema with no review and no error anywhere. `hints` narrows inference towards real types (`cloudFiles.inferColumnTypes`) without naming a schema at all — see the note in `sources/file/reader.py` for why this framework reads `hints` that way rather than as `cloudFiles.schemaHints`. |
| `path_glob` | `*` | The same directory holds more than one file shape. |
| `format_options` | `{}` | Reader options **specific to `file_format`** — an unknown key for the configured format is rejected (Spark would otherwise ignore it silently and produce a table full of wrong columns). Known options: **csv** — `header`, `delimiter`, `encoding`, `quote`, `escape`, `comment`, `nullValue`, `emptyValue`, `dateFormat`, `timestampFormat`, `multiLine`, `ignoreLeadingWhiteSpace`, `ignoreTrailingWhiteSpace`. **json** — `multiLine`, `encoding`, `dateFormat`, `timestampFormat`, `allowComments`, `primitivesAsString`. **parquet** — `mergeSchema`, `datetimeRebaseMode`. **avro** — `avroSchema`, `datetimeRebaseMode`, `ignoreExtension`. |
| `filename_columns` | `{}` | The file name carries data (a business date, a batch id). Each value is a regex with **exactly one capture group**, matched against the full file path — checked at config load, not at the first file that reaches it. |
| `landing_partition_by` | `[ingest_date]` | Rarely — `ingest_date` (the date this framework WROTE the row, not a date in the data) bounds partition growth and makes retention a partition drop. |
| `listing_mode` | `directory` | `directory` needs no extra infrastructure; `notification` needs Event Grid + Queue Storage provisioning this tenancy may not permit — see VB-07. |
| `max_files_per_trigger` | `1000` | Lower it to get a huge backlog through in survivable chunks. Unset is not "no limit" — the whole backlog would arrive as one microbatch on the first run, whose failure costs the whole run. Operationally overridable: `ingest_control.file_max_files_per_trigger`. |
| `failure_mode` | `FAILFAST` | `FAILFAST` refuses a batch that contains a row Auto Loader could not fit the configured schema (its `_rescued_data` column is non-NULL). `QUARANTINE` lands it and only reports the count — flip it to unblock a stuck source, no deploy. There is no separate quarantine TABLE for this source: the rescued row lands in the SAME landing table, with whatever did not fit captured in `_rescued_data` rather than dropped. Operationally overridable: `ingest_control.file_failure_mode`. |

### 🟢 NO CHANGE REQUIRED

| Key | Why |
|---|---|
| `checkpoint_root` / `schema_location_root` | Volume-backed roots, platform-wide, following the same pattern as Kafka's `checkpoint_root`. |
| `landing_table` | Derived from `target_schema` / `target_table`. |
| `table_properties` | Platform-wide, from `conf/defaults.yaml`. |

### Unity Catalog Volume source paths — no register, no credentials (D-15)

When `access_mode` is `volume`, none of the rest of this subsection applies: the register
below is not consulted, `sources/file/security.py` builds no options, and
`sources/file/run.py` applies no session configuration around the read at all. Access is
governed entirely by Unity Catalog grants on the Volume itself, verified with whoever owns
it, not with anything in this repository. **Preferred over the register below when a Volume
path is available** — see the onboarding template. VB-28 tracks which `access_mode` each
environment actually uses and whether Volumes are reachable everywhere this framework
targets; until it is answered, both modes are supported and neither is assumed universal.

### Storage register — `conf/storage.yaml`

For every source with `access_mode: adls`. Same register pattern as
`conf/clusters.yaml` / `conf/registries.yaml` / `conf/jdbc.yaml`: the register records the
auth mode and the secret **key names**; `conf/environments/<env>.yaml` overrides the account,
container and secret **scope** per environment. Two auth modes:

| `auth_mode` | Session options this framework sets | Secret keys the register names |
|---|---|---|
| `account_key` | `fs.azure.account.key.<account>.dfs.core.windows.net` | `account_key_secret_key` |
| `service_principal` | `fs.azure.account.auth.type.*` = OAuth, plus the client id/secret/endpoint options | `client_id_secret_key`, `client_secret_secret_key` (+ register-level `tenant_id`, not a secret) |

**Everything else Azure/ADLS supports — SAS tokens, managed identity, and (for a
`storage_ref`-governed path) Unity Catalog credential passthrough — is deliberately not
implemented** (`docs/build_log/DECISIONS.md` D-12), stated here as a limitation, not an
oversight: each needs either a token-provider class this project cannot verify exists on the
target runtime, or workspace-level UC wiring outside this repository's control, so adding one
is a future code change with its own verification, not a config guess — see
`sources/file/security.py`'s module docstring. **These credentials reach the read as SESSION
configuration (`spark.conf.set()`), not as reader `.option()` calls** — see VB-26 for what is
unverified about that mechanism on the target compute.

**If VB-28 comes back "Volumes everywhere":** this whole register, `sources/file/security.py`,
and `framework/security.py`'s `apply_session_options` become deletable — see
`docs/DESIGN_FILES.md`'s note on the planned simplification. Not attempted now.

### Operational overrides — `ingest_control`

Three columns, mirroring Kafka's three exactly (`docs/build_log/DECISIONS.md` D-01):

| Column | Setting | Effect |
|---|---|---|
| `file_failure_mode` | `failure_mode` | `FAILFAST` ↔ `QUARANTINE` — see the NICE TO CHANGE row above. |
| `file_max_files_per_trigger` | `max_files_per_trigger` | Lower it to get a huge backlog through in survivable chunks. |
| `file_checkpoint_reset_id` | — | Bypasses the checkpoint-reset guard **and** forks the Delta transaction identity — see the MUST-READ block above and `docs/RUNBOOK_SUPPORT.md` §9. Single-use. Never blank it back out once set. |

Everything that decides where this source reads from and where it lands —
`access_mode`, `volume_path`, `storage_ref`, `source_path`, `target_schema`, `target_table`,
`landing_partition_by` — is structural, and an override of one is ignored and logged.
