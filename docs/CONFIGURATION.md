# Configuration Reference

Every setting this framework reads, what it does, and what happens if you get it wrong.

**Tier markers used throughout (and in the YAML files themselves):**

| Marker | Meaning |
|---|---|
| 🔴 **MUST CHANGE** | Environment-specific. The shipped value is a placeholder and **will not work**. |
| 🟡 **NICE TO CHANGE** | Has a sensible default. Tune only with a concrete reason. |
| 🟢 **NO CHANGE REQUIRED** | Framework contract or platform standard. Changing it needs a design discussion, not a config edit. |

---

## 1. The five layers

Configuration is merged from five layers, one of which has a rare optional sub-layer.
**Later always wins, per key. Absent keys fall through to the layer below.**

```
1. conf/defaults.yaml            common to every topic in every environment
2. conf/environments/<env>.yaml  what differs between dev / preprod / prod
3. conf/topics/<key>.yaml        what is unique to one topic
     3a. topic:.environments.<env>  RARE - unique to one topic IN ONE environment, nested
                                   in the same file. Working example:
                                   conf/topics/vector_patient_events.yaml. See §4.
        ^-- STRUCTURAL: Git, PR-reviewed, takes effect on the next deploy
4. operational control table     support-team overrides
5. Workflows job parameters      one-off overrides
        ^-- OPERATIONAL: takes effect on the NEXT RUN, no deploy
```

**The rule:** structural change = PR. Operational change = SQL `UPDATE`. Adding a topic =
neither, beyond one config file.

### Which layer does a setting belong in?

| Is the value... | Put it in |
|---|---|
| the same everywhere | `conf/defaults.yaml` |
| different per environment (catalog, broker, secret scope) | `conf/environments/<env>.yaml` |
| unique to one topic (topic name, subject, dedup keys) | `conf/topics/<key>.yaml` |
| unique to one topic **AND** one environment (rare) | `conf/topics/<key>.yaml`, nested under `environments:` — see §4 |
| something support must change during an incident | the control table |

### Placeholders

Values in layers 1–3 may contain `{placeholder}` tokens:

| Token | Source | Available in |
|---|---|---|
| `{catalog}`, and any other key under `vars:` | the environment file | topic settings, cluster and registry profiles |
| `{topic_key}` | the topic's filename | topic settings only |
| `{topic_table}` | the Kafka topic name with `.` and `-` replaced by `_` | topic settings only |
| `{domain}` | the topic's `domain:` | topic settings only |

### Table creation — nothing is created by hand

Every table this framework owns is created by the **first run**, with explicit
`CREATE TABLE IF NOT EXISTS` DDL and the `table_properties` below. Onboarding a topic never
needs a DDL step, and no table is ever created with a shape Spark inferred from whatever the
first batch happened to contain.

| Table | Column list comes from |
|---|---|
| landing, quarantine, audit | constants in `src/kafka_ingest/tables.py` |
| **curated** | derived on the driver from the Avro **reader schema**, before any row is read — `curated_writer.curated_schema()` runs the real projection over an empty frame, so the table cannot disagree with what the writer produces |

Afterwards the shape is allowed to **grow**: `mergeSchema` on append and
`withSchemaEvolution()` on the replay MERGE both handle an additive reader-schema change.
Explicit creation fixes the *starting* shape; those handle the *changing* shape.

Every field in curated is created **nullable**, including ones the Avro schema declares
required. The decode still enforces the Avro contract at parse time — but a `NOT NULL` in
the table would turn one bad record into a failed batch instead of a quarantined row.

`sql/02_layer_tables.sql` is therefore optional. It exists for provisioning and GRANTing an
environment ahead of the first run, not as an onboarding step.

### Table properties — `table_properties`

One template in `conf/defaults.yaml`, applied to every table the framework creates. Two are
**on by default**:

```yaml
topic_defaults:
  table_properties:
    delta.autoOptimize.optimizeWrite: "true"
    delta.autoOptimize.autoCompact: "true"
```

**Applied at CREATE time only, always.** Editing this file does not alter a table that
already exists, for any property below — use `ALTER TABLE ... SET TBLPROPERTIES` for that.

**Four more ship commented out** in the same block, each with its Delta default and why it
is not on platform-wide. Uncomment inside a **topic's own** `table_properties` to opt one
topic in — a topic overrides the whole map, so copy the block rather than editing the shared
default.

| Property | Off means | Turn it on when |
|---|---|---|
| `delta.enableChangeDataFeed` | No `table_changes()` support | A downstream consumer needs only what changed since a version. Costs storage per write — the control table already turns this on (it is small); a high-volume landing table usually cannot afford it. |
| `delta.deletedFileRetentionDuration` | Delta's built-in 7 days | An **ad-hoc** `VACUUM` from a notebook needs a non-default floor. The scheduled maintenance job always passes an explicit `RETAIN`, so this is redundant for anything scheduled. **Never** set this near the 20-year `landing_retention_days` — that is row retention, this is file retention; see §6. |
| `delta.logRetentionDuration` | Delta's built-in 30 days | `DESCRIBE HISTORY` / time-travel-by-version needs to reach further back than a month. The audit table is the natural candidate if incident retrospectives ever need it. |
| `delta.appendOnly` | Updates/deletes/merges allowed | Almost never, on tables this framework creates. **Do not set it on landing or curated** — both are written via `MERGE` during a replay, and this property makes every replay fail outright. |

A topic overrides the whole map in its own file if one feed genuinely needs different
properties (enabling one of the above, or disabling `autoOptimize`). Most never should. 🟡

### Table naming

Landing, curated and quarantine are **one table per topic**, and all three are named from
`{topic_table}` — the Kafka topic name with dots and hyphens turned into underscores, because
neither is legal in an unquoted Unity Catalog identifier:

```
vector.patient.events.v1  ->  <catalog>.landing.vector_patient_events_v1
                              <catalog>.curated.vector_patient_events_v1
                              <catalog>.landing.vector_patient_events_v1_quarantine
```

The patterns live in `conf/defaults.yaml`. A topic file never names a table.

**To override one topic's name**, set `table_name:` in that topic's file:

```yaml
topic:
  topic: vector.patient.events.v1
  table_name: patient_events        # -> <catalog>.landing.patient_events, .curated.patient_events
```

It replaces the derived name in **both** layers and in every environment. It overrides the
NAME only — catalog and schema still come from `defaults.yaml`, so this is not the same as
hardcoding a table name in a topic file. Use it when the derived name collides with another
topic's, or reads badly. Omit it and the derivation applies.

A topic name that cannot become a legal identifier (a leading digit, other punctuation) is a
**hard error at config load**, naming the topic and telling you to set `table_name:`. It is
never silently mangled.

`{topic_key}` is deliberately **not** available in cluster/registry profiles — those are
shared by many topics, so substituting a topic name into a cert path would silently produce
a per-topic path. An unresolved placeholder is a hard error naming the setting and the
token; it never reaches a cluster.

### Where each file lives

| Surface | File / table | Owner | Effect |
|---|---|---|---|
| Platform defaults | `conf/defaults.yaml` | Platform eng | Next deploy |
| Per-environment values | `conf/environments/<env>.yaml` | Platform eng | Next deploy |
| Kafka cluster register | `conf/clusters.yaml` | Platform eng | Next deploy |
| Schema registry register | `conf/registries.yaml` | Platform eng | Next deploy |
| Per-topic structure | `conf/topics/<key>.yaml` | Domain + platform eng | Next deploy |
| Per-topic behaviour | `ops_*.ingestion.ingestion_topic_control` | **Support team** | **Next run** |
| Deployment targets | `databricks.yml` | Platform eng | Next deploy |
| Job shape / schedule | `resources/*.yml` | Platform eng | Next deploy |
| One-off replay | Workflows UI job parameters | **Support team** | That run only |

### How the environment is chosen

Each job passes `--environment ${bundle.target}`, so the DAB target name **must match** an
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
├── YES → Can it be expressed as: enable/disable, trigger, batch size,
│         failure mode, reader schema, or a replay?
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
staging) is described in `security.py` but **is not built** — no in-scope cluster needs it.

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

## 4. Per-topic structural config — `conf/topics/<topic_key>.yaml`

**The filename is the `topic_key`.** It appears in job parameters, the control table,
checkpoint paths and every audit row. Renaming the file orphans the checkpoint.

Start from [`conf/topics/_TEMPLATE.yaml`](../conf/topics/_TEMPLATE.yaml).

### 🔴 MUST CHANGE

| Key | Notes |
|---|---|
| `topic` | Exact Kafka topic name. Case-sensitive. |
| `domain` | `vector` \| `rcm` \| `gma` \| `antifraud` \| `dwh_pes`. Appears in every audit row. |
| `cluster` | Must match a key in `clusters.yaml`. Load fails and lists valid names if wrong. |
| `registry` | Must match a key in `registries.yaml`. Unrelated namespace to `cluster`. |
| `subject` | Usually `<topic>-value` (TopicNameStrategy). **Verify** — RecordNameStrategy subjects look completely different. |
| `min_partitions` | Roughly **4× this topic's partition count**. Confirm the partition count with the producing team at onboarding. The platform default assumes a mid-sized topic; leave it wrong and the topic reads at a fraction of the parallelism it could, silently. Over-setting is cheap — it only ever splits offset ranges, never merges them. |

#### 🔴 MUST READ — the schedule is not a free choice

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

Table names are **not** set here — see "Table naming" above. `table_name:` is the only
sanctioned per-topic override; `landing_table` / `curated_table` / `quarantine_table` /
`audit_table` / `checkpoint_root` all come from the pattern in `conf/defaults.yaml`.

### 🟡 NICE TO CHANGE

| Key | Default | Notes |
|---|---|---|
| `consumer_group_prefix` | — | Spark manages its own group; this is the identifiable prefix. Replays get a distinct suffix automatically. |
| `starting_offsets` | `earliest` | **First run only** — the checkpoint wins afterwards. `latest` when history would wrongly re-trigger consumers. |
| `max_offsets_per_trigger` | `1000000` | Splits a backlog into several bounded microbatches under `availableNow` — it does **not** cap what a run consumes. It always has a value: unset would mean the entire backlog arrives as ONE microbatch, so a first run on a retained topic becomes a single enormous batch whose failure costs the whole run. |
| `curated_dedup_keys` | *empty* | Collapses duplicates **within one microbatch**. Business fields are nested, so write them as **`payload.<field>`** — a bare name is rejected with a clear error. |
| `reader_schema_id` | — | **Required** when `reader_schema_mode: pinned_id`. |
| `table_properties` | *(inherited from `conf/defaults.yaml`)* | Overrides the whole map for this topic. See "Table properties" above. |

#### Partitioning — structural, deliberately NOT operationally overridable

| Key | Default | Notes |
|---|---|---|
| `landing_partition_by` | `[ingest_date]` | Must name at least one column (validated). `topic` is constant inside a per-topic table, so it is not a useful key. |
| `curated_partition_by` | `[event_date]` | `event_date` = date of `ce_time`, falling back to `kafka_timestamp`. |

Delta allows `PARTITIONED BY` **or** `CLUSTER BY`, never both — these tables use
partitioning, so Liquid Clustering is not available on them. Changing a partition column on
an existing table requires rewriting it, which is why this is a PR-only field: the control
table and job parameters cannot override it.

### 🟢 NO CHANGE REQUIRED

| Key | Default | Why it is the default |
|---|---|---|
| `trigger` | `availableNow` | Drains what is available then stops — the only trigger giving a scheduled run a natural end. |
| `fail_on_data_loss` | `true` | `false` means silently accepting gaps; needs explicit domain sign-off. |
| `reader_schema_mode` | `registry_latest` | The curated **`payload` struct's shape** follows the subject's latest version; each record is still **decoded** with its own writer schema. |
| `failure_mode` | `FAILFAST` | Loud failure beats silently NULLed fields. Use `QUARANTINE` only for a known, accepted source of bad records. The two values are the same two the CHECK constraint on `ingest_control.kafka_failure_mode` allows — the column and the setting are one lever. |
| `curated_dedup_order_by` | `kafka_timestamp` | Change only if the producer sets no timestamp. |

### Environment-specific overrides for one topic — `environments:`

Rare. A fourth structural sub-layer, nested inside the same topic file.

**Working example, not a hypothetical** — [`conf/topics/vector_patient_events.yaml`](../conf/topics/vector_patient_events.yaml)
resolves differently in all three shipped environments, and
`tests/test_shipped_config.py::test_vector_patient_events_environment_override_resolves_as_documented`
asserts the exact numbers below on every test run, so this table cannot drift from the file:

| Environment | `max_offsets_per_trigger` | Where it comes from |
|---|---:|---|
| `dev` | `100000` | No override in the topic file → falls through to `dev.yaml`'s environment-level default |
| `preprod` | `2000000` | Set under `environments: preprod:` in the topic file |
| `prod` | `5000000` | Set under `environments: prod:` in the topic file |

The shape, inside the topic file:

```yaml
topic:
  topic: vector.patient.events.v1
  ...
  # No blanket max_offsets_per_trigger here - this topic wants a genuinely different value
  # per environment, so the whole setting lives in the sub-layer below instead.
  environments:
    preprod:
      max_offsets_per_trigger: 2000000
    prod:
      max_offsets_per_trigger: 5000000
```

**Full precedence, later always wins:**

```
defaults.yaml -> environments/<env>.yaml -> topics/<key>.yaml -> topics/<key>.yaml: environments.<env>
    -> operational control table -> job parameters
```

Still layer 3 — still Git, still PR-reviewed, still deployed by DAB. It exists for a value
that is specific to **both** one topic **and** one environment. Two more common cases have
better homes:

| Need | Use instead |
|---|---|
| Same value for every topic in one environment | `conf/environments/<env>.yaml` `topic_defaults` |
| A value that changes at runtime, no deploy | The operational control table (§5) |

An environment named in the block that does not exist (a typo) is a **hard error at load**,
listing the environments that do. Any key placed inside it is validated exactly like a
normal topic setting — a typo there fails the same "unknown keys" check.

### `reader_schema_mode` — pick one

| Mode | The curated `payload` struct | Use when |
|---|---|---|
| `registry_latest` *(default)* | Follow the subject's latest version | Normal operation |
| `pinned_id` | Frozen at `reader_schema_id` | A **contract freeze** while downstream is rebuilt (see `rcm_claim_status.yaml`) |

There is deliberately **no third "use each writer schema as-is" mode**. Curated stores the
payload as one STRUCT column, and two writer versions decoded without a common reader
schema produce two incompatible struct types that cannot share a table.

In **all three** modes each record is decoded with its own writer schema from the wire
header. The reader schema controls the output *shape* only, never how bytes are read.

---

## 5. Operational control table — support-team editable

`<ops_catalog>.ingestion.ingestion_topic_control`, one row per `topic_key`.
DDL and grants: [`sql/01_operational_config.sql`](../sql/01_operational_config.sql).
Ready-made statements: [`sql/03_support_queries.sql`](../sql/03_support_queries.sql).

A **missing row is not an error** — it means "no overrides, run as coded", so a newly
onboarded topic works the moment its YAML merges. Duplicate rows **are** an error.

### Behaviour overrides

| Column | Overrides | Typical use |
|---|---|---|
| `enabled` | — | `false` = **emergency stop**. Job runs, consumes nothing, writes a `skipped_disabled` audit row so silence is never ambiguous. |
| `kafka_max_offsets_per_trigger` | YAML `max_offsets_per_trigger` | Lower it to get a huge backlog through in survivable chunks. |
| `kafka_failure_mode` | YAML `failure_mode` | Flip to `QUARANTINE` to get a failing topic moving, then investigate. Records keep their raw bytes and are recoverable by a curated replay. |

`fail_on_data_loss`, `reader_schema_mode` and `trigger` are **not** in this table any more.
They are structural: accepting silent gaps, or changing which schema fixes the payload
struct's shape, needs domain sign-off and a PR, not a 3am `UPDATE`. **Q14** in
`sql/03_support_queries.sql` lists every source currently running with data-loss protection
disabled, precisely because that setting cannot be seen from the control table.

There is no free-form JSON escape hatch for a source-specific setting
(`docs/build_log/DECISIONS.md` D-01 removed it): a setting either has a dedicated column
here, or it is not operationally overridable at all.

### Checkpoint-reset override — incident use only, and SINGLE-USE

| Column | Overrides | Typical use |
|---|---|---|
| `kafka_checkpoint_reset_id` | Bypasses the checkpoint-reset guard | Set **only** after confirming a topic's primary checkpoint is genuinely gone, and **only to an id this source has never used**. See [`RUNBOOK_SUPPORT.md` §5.4a](RUNBOOK_SUPPORT.md#5-4a-restarting-the-primary-after-a-genuine-checkpoint-loss). |

Unlike every other row in this table, this is **not a toggle** — it is a one-way fork of the
source's Delta transaction identity (`KafkaConfig.txn_app_id`), which is what makes the
restart safe rather than merely permitted: a fresh identity has no prior committed versions
for Delta to silently skip against.

**It is single-use.** Reusing an id keeps the identity the last reset created, against which
Delta already holds high versions, so every write would be skipped as a duplicate and the run
would report success having ingested nothing — the very failure the guard exists to catch.
The job **refuses to start** in that state and names the spent id; run **Q6d** in
`sql/03_support_queries.sql` first rather than finding out from a failed run.

**Never blank it back out once set.** Once the checkpoint exists again the field is inert for
the guard — it only forks the app id, which must stay forked. Reverting to `NULL` reverts to
the pre-incident identity and its stale watermark.

It is rejected outright in source YAML — the spec declares it operational-only, so a value
in Git is a startup error rather than a bypass that silently re-applies on every future
deploy with no incident behind it.

This does **not** replace a Kafka replay for the data missed during the outage — the two are
independent; see RUNBOOK_SUPPORT.md §5.4a for the full sequence.

### Replay controls

| Column | Notes |
|---|---|
| `rerun_id` | **Required for any replay.** Isolates the checkpoint *and* tags every written row. `[A-Za-z0-9_.-]{1,64}`. |
| `rerun_starting_offsets` | Spark `startingOffsets` JSON: `{"topic":{"0":45231}}`. `-2`=earliest, `-1`=latest. |
| `rerun_starting_timestamp` | ISO-8601 or epoch millis. **Mutually exclusive** with offsets — a DB constraint enforces this. |
| `rerun_ending_offsets` / `rerun_ending_timestamp` | Optional. Present ⇒ bounded **batch** replay instead of streaming. |
| `curated_replay_landing_filter` | SQL predicate over **landing** for a curated-only replay. |

**Job parameters typed into the Workflows UI win over this table**, so an urgent one-off
replay needs no `UPDATE` first. Use the table to park an intent that must survive re-triggering.
Clear the columns when the replay is done.

### Attribution

`change_reason`, `updated_by`, `updated_at` are set by the support `UPDATE` templates.
Delta's own `DESCRIBE HISTORY` is the backstop if they are not maintained.

---

## 6. Deployment config — `databricks.yml`

### 🔴 MUST CHANGE

| Variable | Notes |
|---|---|
| `workspace.host` (per target) | Your workspace URL. Three placeholders shipped. |
| `catalog` | Data catalog per environment. |
| `ops_catalog` / `control_table` | Where the control table lives. |
| `service_principal` | The principal jobs run as. Needs `READ` on secret scopes, `READ VOLUME` on cert Volumes, `MODIFY` on target tables. |
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
owner's decision. See `docs/RUNBOOK_CLIENT_IT.md` §"Data protection".

### 🟢 NO CHANGE REQUIRED

`bundle.name`, `artifacts`, `sync.include`, `targets.*.mode`.

The `run_as` / `permissions` split is deliberate: support gets `CAN_MANAGE_RUN` and not
`CAN_MANAGE`, so they can trigger and cancel jobs but cannot edit a job definition. Note the
permissions are declared at **target** level, so they cover every job in the bundle —
including primary ingestion, which is intended so support can re-run it during an incident.

---

## 7. Job parameters

### Primary job — `ingest_primary`

| Parameter | Default | Notes |
|---|---|---|
| `config_root` | `${workspace.file_path}/conf` | 🟢 |
| `control_table` | `${var.control_table}` | 🟢 |
| `topic-key` | per task | 🔴 One task per topic |

### Replay jobs

| Parameter | `replay_kafka` | `replay_curated` |
|---|---|---|
| `topic_key` | required | required |
| `rerun_id` | required | required |
| `starting_offsets` | one of these two | — |
| `starting_timestamp` | one of these two | — |
| `ending_offsets` / `ending_timestamp` | optional ⇒ bounded batch | — |
| `landing_filter` | — | **required** |

`landing_filter` is required deliberately: re-parsing all history by accident is an
expensive way to discover you meant one day.

### Maintenance job — `maintenance`

| Parameter | Default | Notes |
|---|---|---|
| `catalog` | `${var.ops_catalog}` | 🟡 The data catalog the tables live in. Kept in step with `vars.catalog` in the environment file by hand. |
| `landing_retention_days` | `${var.landing_retention_days}` | 🟡 See §6. Only used by the reporting query today — the `DELETE` is commented out. |
| `vacuum_retention_hours` | `${var.vacuum_retention_hours}` | 🟢 Leave at Delta's default unless storage pressure says otherwise. |

Runs weekly. It compacts and reclaims; it does **not** delete rows.

---

## 8. Prerequisites inventory

### Secrets (Azure Key Vault, via a Databricks secret scope)

For every cluster and registry in `conf/`:

| Scope | Keys | Used by |
|---|---|---|
| `kv-kafka-prod` | `cc-shared-api-key`, `cc-shared-api-secret`, `sr-shared-api-key`, `sr-shared-api-secret` | Confluent Cloud + its registry |
| `kv-kafka-rcm` | `rcm-consumer-username`, `rcm-consumer-password`, `rcm-truststore-password`, `rcm-sr-username`, `rcm-sr-password` | RCM cluster + registry |
| `kv-kafka-antifraud` | `af-truststore-password`, `af-keystore-password`, `af-private-key-password` | Anti-Fraud mTLS |

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

### Checkpoint Volume

One Volume, shared by all topics; the framework creates per-topic sub-paths. The service
principal needs `READ VOLUME` + `WRITE VOLUME`.

### Network (serverless compute only)

Serverless egress must reach **every** bootstrap endpoint **and every** Schema Registry,
via NCC private endpoints or firewall allowlisting of the NCC's stable egress IPs. **This
is not assumed** for the on-prem clusters — confirm per cluster, or run those tasks on
classic compute by swapping `environment_key` for a `job_cluster_key` in
`resources/job_ingest_primary.yml`.

A registry that is unreachable produces a `SchemaResolutionError` that says so explicitly.

---

## 9. Validating configuration before you deploy

```bash
pytest tests/test_shipped_config.py -q     # every conf/topics/*.yaml resolves
databricks bundle validate -t dev
```

`test_shipped_config.py` loads and validates every shipped topic file. Run it in CI on
every PR — it catches a typo'd catalog name, an unknown cluster reference, a non-3-tier
table name and a DBFS cert path before any of them reach a cluster.


---

## 9. Oracle sources — `conf/sources/<source_key>.yaml`, `source_type: oracle`

Added in Stage 4. Sections 1–8 above still describe the Kafka source in places where the
wording has not caught up; this section is self-contained for Oracle.

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

## 10. File sources — `conf/sources/<source_key>.yaml`, `source_type: file`

Added in Stage 5. Sections 1–9 describe Kafka and Oracle; this one is self-contained.

Copy `conf/sources/_TEMPLATE_file.yaml` to onboard a drop zone. It carries the four
questions whoever owns the landing zone has to answer first — the full schema and whether
it drifts, whether files are ever rewritten in place, whether the file name carries data
that belongs in a column, and roughly how many files land per day.

**Use Auto Loader (`cloudFiles`), always `availableNow`** (CORE section 10 / the STAGE_5
brief's decision, already made — not a per-source choice). **This source is
checkpoint-based**, exactly like Kafka's primary stream, and reuses the *same*
checkpoint-reset mechanism — see the MUST-READ block below rather than a second design.

### 🔴 MUST-READ — this source shares Kafka's checkpoint-reset guard

Deleting or losing this source's checkpoint restarts Auto Loader's internal batch
numbering from zero. Delta has already recorded higher `txnVersion`s against this source's
app id, so every write is silently **skipped as a duplicate** — the job reports success and
ingests nothing. The framework refuses to start in that state with a message beginning
`REFUSING TO RUN`, exactly Kafka's guard, applied to this source's own checkpoint and
landing table. See `docs/RUNBOOK_SUPPORT.md` §9 for the restart procedure — it is the same
five-step procedure as Kafka's §5.4a, with `file_checkpoint_reset_id` in place of
`kafka_checkpoint_reset_id`.

`cloudFiles.schemaLocation` is a **separate** checkpoint-like resource, covered by the same
guard indirectly rather than directly — see `docs/DESIGN.md` §11 for the reasoning.

### 🔴 MUST CHANGE

| Key | What it is |
|---|---|
| `storage_ref` | A profile name from `conf/storage.yaml`. Never an account URL. |
| `source_path` | The path **within** the container only — never a full `abfss://` URL. The account and container come from `storage_ref`, which is what lets the same source file resolve to a different account per environment. |
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

### Storage register — `conf/storage.yaml`

Same register pattern as `conf/clusters.yaml` / `conf/registries.yaml` / `conf/jdbc.yaml`:
the register records the auth mode and the secret **key names**; `conf/environments/<env>.yaml`
overrides the account, container and secret **scope** per environment. Two auth modes:

| `auth_mode` | Session options this framework sets | Secret keys the register names |
|---|---|---|
| `account_key` | `fs.azure.account.key.<account>.dfs.core.windows.net` | `account_key_secret_key` |
| `service_principal` | `fs.azure.account.auth.type.*` = OAuth, plus the client id/secret/endpoint options | `client_id_secret_key`, `client_secret_secret_key` (+ register-level `tenant_id`, not a secret) |

Everything else Azure/ADLS supports (SAS tokens, managed identity, Unity Catalog credential
passthrough) is deliberately not implemented — see `sources/file/security.py`'s module
docstring. Adding one is a code change, not a config guess. **These credentials reach the
read as SESSION configuration (`spark.conf.set()`), not as reader `.option()` calls** — see
VB-26 for what is unverified about that mechanism on the target compute.

### Operational overrides — `ingest_control`

Three columns, mirroring Kafka's three exactly (`docs/build_log/DECISIONS.md` D-01):

| Column | Setting | Effect |
|---|---|---|
| `file_failure_mode` | `failure_mode` | `FAILFAST` ↔ `QUARANTINE` — see the NICE TO CHANGE row above. |
| `file_max_files_per_trigger` | `max_files_per_trigger` | Lower it to get a huge backlog through in survivable chunks. |
| `file_checkpoint_reset_id` | — | Bypasses the checkpoint-reset guard **and** forks the Delta transaction identity — see the MUST-READ block above and `docs/RUNBOOK_SUPPORT.md` §9. Single-use. Never blank it back out once set. |

Everything that decides where this source reads from and where it lands —
`storage_ref`, `source_path`, `target_schema`, `target_table`, `landing_partition_by` — is
structural, and an override of one is ignored and logged.
