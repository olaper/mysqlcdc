# MySQL CDC Monitor

A small Python proof of concept for copying selected source tables from
`gphinblue-poc-rep.mysql.database.azure.com` into one or two destination tables per source on
`gphingreen-poc.mysql.database.azure.com`.

All application code, messages, comments, configuration and UI are in English.
No Docker, Kafka, Redis or external job service is required.

## What it does

- YAML configuration for servers, databases, selected tables and one or two destinations per source.
- First destination: identical source column definitions and primary key.
- Optional second destination: rename or omit columns and supply constant values.
  Unmapped destination columns use their own defaults on insert.
- INSERT, UPDATE, primary-key changes and DELETE from the source binary log.
- Independent per-source-table workers, pause/resume, and initialize/reinitialize.
- A web dashboard showing source/target server binlog heads, table checkpoints,
  snapshot rows, committed CDC changes, status and errors.
- Target-side durable checkpoints, committed in the **same transaction as all configured
  destination writes**. A failed destination prevents that transaction committing.
- Online snapshots without an explicit source write lock.

## Deliberate boundaries

This is a replication-only PoC, not a general replacement for Debezium.

- Use MySQL **8.0.20+ or 8.4** on the source. All destinations must be on the
  same target server and all selected source/destination tables must be InnoDB.
- Pre-create the target databases and tables. The program creates only its
  checkpoint table, not user tables or their schema.
- Every source table needs a primary key. All targets must preserve all
  primary-key columns, in the same order, with matching types and collation.
- Targets must have **no secondary UNIQUE indexes or triggers**.
  Inbound/outbound foreign keys are rejected by default; the explicit
  `target.disable_foreign_key_checks: true` option allows them without enforcing
  them on this application's target sessions. Do not write to target tables outside this application.
  Have an administrator verify these restrictions, including referencing tables
  in other databases that the CDC user might not be allowed to inspect.
- The first target preserves column definitions and primary key; other indexes
  are not automatically created or compared.
- Mapped columns must preserve type, nullability and collation. This version
  does not perform type conversions, calculations, joins or aggregations.
- UPDATE requires the old primary key and a complete after-image. DELETE requires
  only the primary key. Omitted non-key before-image columns are safe and accepted;
  omitted after-image columns are rejected because snapshot-overlap replay needs
  the complete new row. Keep the FULL server prerequisites enabled.
- JSON, BIT, spatial/vector and generated columns are explicitly rejected.
  They require additional type-specific decoding/writing logic; failing early is
  safer than silently changing their meaning. Normal numeric, text, binary,
  date/time, ENUM and SET columns are handled. Invalid/zero dates stop replication.
- Keep source and target schemas fixed while replication is enabled.
  DDL and statement events stop the worker rather than being silently skipped.
  XA, partial-JSON and unknown events (including transaction payload compression)
  are rejected as well.
  This conservative check can also stop a worker for unrelated source-server DDL.
  Coordinate schema changes, update the configured targets/configuration and reinitialize.
- Each source table has an independent stream/transaction. A source transaction
  spanning multiple selected tables is **not atomic across those tables**.
  Each individual table's configured destinations and its checkpoint are atomic.
- Snapshots of different source tables are not a database-wide consistent snapshot.
- There is no automatic retry after an error. Fix the cause, then Resume;
  use Initialize after an interrupted snapshot or expired binlog.
- Pause requests take effect between streamed events or snapshot batches.
  In-flight CDC writes are rolled back unless their source COMMIT was processed.
  A long SQL statement, network timeout, or draining a snapshot cursor can delay
  the actual stop. Wait for `paused` before reinitializing.
- Pause is a process-local control. With `auto_start: true`, previously completed
  snapshots resume on application restart, including tables paused before shutdown.
  Set it to `false` when you want manual resume after restart.
- No automatic source failover/reset recovery or GTID auto-positioning is provided.
  After RESET BINARY LOGS / RESET MASTER, restoring/replacing a source, or changing
  binlog lineage, reinitialize even if a filename happens to match an old one.
- Use one application/configuration per destination set. Named MySQL locks prevent
  concurrent workers for the same source job, but not independently configured
  applications writing different source jobs into the same destination.
- Each selected table opens its own binlog stream and reads the source log.
  This keeps the code understandable but is intended for a modest table count.

## Online initialization and its consistency model

1. Read source binlog coordinate **C**, before starting the snapshot.
2. Persist `initializing`, then delete **all configured target table contents**.
3. Start a REPEATABLE READ, READ ONLY consistent snapshot on the source.
4. Copy rows in batches to all configured targets, committing each batch together.
5. End the snapshot and capture source coordinate **H**.
6. Replay the binlog starting at **C**, using full-row upserts and primary-key
   deletes. This intentionally overlaps the snapshot.
7. Once committed replay has reached **H**, show `running`.

This is an **eventually convergent** bootstrap, explicitly not an immediately
consistent online backup. During `initializing` and `catching_up`, the destinations
can be incomplete and replay can temporarily overwrite newer snapshot values
with older event values. FULL row images, stable schemas, primary-key-only
uniqueness and exclusive target ownership are necessary for convergence.
After reaching H, normal committed changes continue to be applied.

The source remains writable: there is no `FLUSH TABLES WITH READ LOCK` or
`LOCK TABLES`. Normal MySQL snapshot metadata locks still apply; long snapshots
consume undo history and can delay DDL. Retain enough binlog to cover the entire
copy plus catch-up, pauses and outages. CDC must be able to catch up with the
source write rate.

Interrupted snapshots require a fresh Initialize. A successful snapshot's
checkpoint survives Python/application restarts because it lives on the target.
Do not delete or independently restore the checkpoint table: it must stay
consistent with all configured destinations.

## 1. Configure Azure MySQL

On the source, configure/verify these server parameters:

| Parameter | Required value |
|---|---|
| `log_bin` | `ON` |
| `binlog_format` | `ROW` |
| `binlog_row_image` | `FULL` |
| `binlog_row_metadata` | `FULL` |
| `binlog_transaction_compression` | `OFF` |
| `binlog_row_value_options` | empty string |
| `binlog_expire_logs_seconds` | long enough for copy + catch-up + outages |

### Prerequisite script

Use `prepare_source.ps1` with Azure CLI installed and `az login` completed.
The Azure identity needs permission to read/update server configurations.
It is separate from the database CDC account.

```powershell
# Check only. Use the Azure resource name, not the full DNS hostname.
.\prepare_source.ps1 -Subscription '<subscription-id>' `
    -ResourceGroup '<resource-group>' -ServerName 'gphinblue-poc-rep'

# Apply supported changes. No automatic server restart.
.\prepare_source.ps1 -Subscription '<subscription-id>' `
    -ResourceGroup '<resource-group>' -ServerName 'gphinblue-poc-rep' -Apply
```

If CDC reads a read replica, run the same checks/configuration against its
**upstream writable primary as well**. Row image, row metadata and compression
must be correct where the events are originally generated; the replica does not
rewrite old upstream events to satisfy its own local settings. The CDC endpoint
must also expose compliant values because the application checks that endpoint.
Neither this script nor this prerequisite requires disabling replica read-only mode.

The script reports unavailable/read-only parameters as requiring manual action;
it does not claim that Azure exposes every variable as a configurable parameter.
Supported changes can be applied even if another parameter requires manual action.
Changes are not transactional and are not automatically rolled back.
Verify the effective values with SQL on both servers after any required restart:

```sql
SHOW GLOBAL VARIABLES WHERE Variable_name IN (
    'log_bin', 'binlog_format', 'binlog_row_image', 'binlog_row_metadata',
    'binlog_transaction_compression', 'binlog_row_value_options',
    'log_replica_updates', 'log_slave_updates'
);
```

Reconnect source-writing applications/connection pools so their new sessions
inherit global settings. Existing binlog events are not rewritten. Wait until
new compliant events reach the replica, then initialize/reinitialize CDC.
Reinitialization deletes all configured target table contents.
Configure binlog retention separately for the actual copy/outage duration.

If the source endpoint is itself a read replica, verify that upstream replicated
changes are written to its binlog (`log_replica_updates` / `log_slave_updates`).
Otherwise there is nothing for this application to consume for those changes.

Use MySQL-native credentials, TLS and firewall/private-network access from the
computer running Python to both servers on TCP 3306.
Azure documents that data-out replication is not supported on a source with
Microsoft Entra authentication configured; confirm your server meets the current
Azure requirements before proceeding.

Provision accounts separately using your normal secret-management process.
Typical privileges:

```sql
-- Source: substitute your real account/host/database.
GRANT SELECT ON source_db.* TO 'cdc_reader'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_reader'@'%';

-- Target: repeat the database-scoped grants for any other destination database.
-- TRIGGER allows inspecting INFORMATION_SCHEMA.TRIGGERS during preflight.
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, TRIGGER
    ON replica_db.* TO 'cdc_writer'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'cdc_writer'@'%';
```

Create the accounts with `REQUIRE SSL`; do not embed passwords in these files.
The target `CREATE` privilege is for `_python_cdc_state`.
An administrator can pre-create that table and remove CREATE afterward.
The target `REPLICATION CLIENT` privilege is for dashboard binlog inspection,
not for native target replication. The application writes ordinary SQL.

TLS certificate and hostname verification are always enabled. The OS trust store
is used by default. If required, configure `ssl_ca` with the current Azure-approved
PEM CA bundle. Do not disable certificate verification to fix certificate errors.

References:

- [Azure data-out replication concepts](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/concepts-data-out-replication)
- [Azure data-out replication setup](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/how-to-data-out-replication)
- [Azure MySQL TLS](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/security-tls)

## 2. Configure tables

Copy `config.example.yaml` to `config.yaml` and edit database names, usernames,
tables and column mappings. The requested hostnames are already in the example.
`config.yaml` is git-ignored; passwords are read only from environment variables.

`columns` maps **source column -> destination column**. Omit `columns` to map all
source columns by name. `constants` maps destination columns to fixed values.
Prefer quoted YAML strings for text values that look like dates or booleans.

A source may have only the first target configured. Primary keys, including
composite keys, and column definitions are discovered from MySQL; do not list
datatypes or primary keys in YAML. Adding a second target later changes the
mapping hash: pause that source, update the configuration, restart the app and
Initialize that source again. This deletes and reloads all of its configured
targets, including the existing first target. Other source tables do not need
reinitialization when their mappings remain unchanged.

`example_schema.sql` contains source and target sample table definitions.
Run its source and target sections **separately on the appropriate servers**.
The application does not execute this file.

Changing mappings, endpoint/database identity or validated column schemas
invalidates old checkpoints and requires Initialize. Reserve the configured
`server_id_start` range for this process; each selected table uses the start ID
plus its zero-based configuration index. It must not collide with other binlog
clients or replication servers.

## 3. Install and run on Windows

Python 3.11+ is recommended. Commands below are PowerShell:

```powershell
Set-Location C:\Temp\mysqlcdc
python -m venv .venv
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item config.example.yaml config.yaml

# Edit config.yaml before starting.
$env:CDC_SOURCE_PASSWORD = 'replace-with-source-password'
$env:CDC_TARGET_PASSWORD = 'replace-with-target-password'

& .\.venv\Scripts\python.exe app.py --config config.yaml
```

Open **http://127.0.0.1:8080**. No login or dashboard token is required.
No automatic initial data deletion occurs:
new tables stay `not_initialized` until you explicitly Initialize.

The dashboard is local-only by default and has no built-in authentication.
Anyone who can reach it can inspect replication status, pause/resume workers and
reinitialize target tables. Keep `web.host` set to `127.0.0.1`. For remote use,
put it behind an authenticated HTTPS reverse proxy and network access controls.
Do not expose Flask's development server directly to a network. JSON/header
validation and destructive-action confirmations remain enabled.
Run a single application process; do not enable Flask reload or multi-process
WSGI workers.

Environment variables must be set in the same shell that starts the application.
Avoid storing real secrets in shell history for a production deployment.

## 4. Operate

### Optional: disable target-session foreign key checks

```yaml
target:
  # Keep the other target connection settings.
  disable_foreign_key_checks: true
```

The default is `false` (checks remain enabled). The working `config.yaml` enables
this option; `config.example.yaml` keeps the safer `false` default.
Only the `target` section accepts it. Every target connection created by these
Python helpers initializes its own session with `foreign_key_checks=0`; source
connections and the server's GLOBAL setting are unchanged. This covers CDC
INSERT/UPDATE/DELETE, built-in snapshots and restore-checkpoint registration.
External `mysql`, `mysqlsh` and other applications' sessions are not changed.

Restart the app after changing this option. The option alone does not change
mapping hashes or require Initialize; Resume can retry the failed transaction
from its last committed checkpoint if its binlog still exists.

This bypasses enforcement; it does **not** preserve cross-table transaction
ordering or guarantee referential integrity. Orphan rows can exist temporarily
or permanently. Foreign-key cascades are not a recovery mechanism with checks
disabled. Setting checks back to 1 does not validate already loaded rows:
independently validate all parent/child relationships after catch-up, including
tables outside the CDC mapping. Disable this option only after that validation;
the default preflight still rejects defined target foreign keys. Secondary UNIQUE
indexes and triggers remain rejected even when this option is enabled.

- **Initialize**: type the source table name to confirm. This deletes all
  corresponding target contents and recopies the source without a source write lock.
- **Pause**: request a safe stop. Wait for `paused`. If stopped during snapshot,
  Resume will report that Initialize is required.
- **Resume**: continue from the last committed source checkpoint.
- **Pause all tables**, then **Initialize all tables**: wait for every worker to
  stop, then type `REINITIALIZE ALL`. All configured pairs are recopied.
  This is destructive to target data and is not atomic across source tables.
- **error**: inspect the table error and application console log. Fix schema,
  permissions, connection or data issues before resuming/reinitializing.
  For `cols bitmap`, the error includes the image (before/after), originating
  server ID, event timestamp, raw column-presence bitmaps, omitted column names,
  event-end coordinate and last committed checkpoint. Row values are not logged.
  A server/session reporting FULL does not prove a particular streamed event
  contains full images. Use these diagnostics to distinguish omitted event
  columns from decoding/schema issues rather than repeatedly reinitializing.
- **catching_up**: snapshot completed but overlap replay has not yet reached
  the captured initialization barrier.
- **running**: initial catch-up completed. This does not mean zero current lag.

Source and target binlog heads belong to **different servers**. Their positions
cannot be compared numerically to calculate replication lag. The source
checkpoint identifies committed work applied by this application; the target
head is server-wide binlog activity and includes unrelated writes.
No exact seconds-of-lag claim is made. `Last applied source event` is an event
timestamp, not a trustworthy lag estimate for an idle table.

### Lag and cutover monitoring

The dashboard shows each table worker's **committed binlog backlog** against a
sampled head of the configured source:

- **Bytes**: raw source log file bytes between the checkpoint and sampled head,
  including file headers when crossing rotations.
- **File transitions**: how many retained binlog files the worker must cross.
- **Caught up tables**: running workers with zero measured backlog.
- **Cutover barrier**: whether each running worker reached a manually captured
  source position. Uninitialized, initializing, paused or failed workers never
  count as ready, even if their saved coordinates happen to match.

These are server-wide binlog distances, not pending changed rows or time/ETA.
Unrelated source tables also generate binlog bytes. Missing/purged files,
checkpoint/head inconsistencies or source monitoring permission/connectivity
failures show **unknown/error**, never a fabricated zero. Monitoring needs
`SHOW BINARY LOGS` and binary-log status access on the source.

For cutover:

1. Stop all writes to the writable primary, including sample generators and
   scheduled/background writers. Finish or roll back in-flight transactions.
2. If CDC reads a replica, independently verify that the replica has received
   and applied **all final primary transactions**. This dashboard does not
   inspect the upstream primary, so its green state alone cannot prove this.
3. Click **Capture source cutover position** and acknowledge the warning.
4. Keep CDC running until **SOURCE BARRIER REACHED** appears: all configured
   workers are running, have committed through that position, have zero
   measured backlog, and the sampled source head has not changed since capture.
5. If the source advances (even because of rotation or unrelated events),
   readiness is withdrawn. Review the cause and capture again when appropriate.
6. Validate mapped data and any referential integrity requirements separately,
   then perform the operational cutover while writes remain fenced.

This is a sampled observation, not an automatic write fence, database-wide row
comparison or guarantee about tables absent from the configuration. Polling is
approximately every 3 seconds plus database query time. The captured barrier is
process-local and is cleared on app restart; capture a new one afterward.
Pausing workers withdraws the green readiness indication. The capture button
does not pause, initialize or modify source/target data or stored checkpoints.

Snapshot row count is exact for the snapshot; there is no fabricated percentage
based on InnoDB's approximate row count. CDC changes count source row events,
including overlap replay, rather than two separate destination writes.
Unmapped/default destination columns are not explicitly assigned on UPDATE;
their own MySQL `ON UPDATE` behavior, if any, still applies. Configured constant
columns are written on every upsert.

## Generate continuous sample INSERTs

`generate_sample_data.py` generates original synthetic data matching the eight
classicmodels tables in `mysqlsampledatabase.sql`. It does not execute that SQL
file, create tables, or copy its sample text. Every group inserts one related row
into each table: productlines, products, offices, employees, customers, orders,
orderdetails and payments. Parents are inserted before children in one transaction.
The explicit INSERT order is:

```text
offices -> employees -> customers -> productlines -> products
        -> orders -> orderdetails -> payments
```

Each child references the parent keys created in the same group.
`employees.reportsTo` is NULL, so no pre-existing manager row is required.
This guarantees source-side dependency order, not target-side CDC apply order:
independent CDC workers can still deliver customers before employees on green.
Enforcing target foreign keys requires a CDC transaction-ordering change, not
delays or reordering in this generator. The target-session bypass option described
above avoids enforcement errors but does not solve that ordering problem.

All connection settings come directly from `source` in `config.yaml`, including
host, database, user, password environment-variable name, port and verified TLS.
Each batch inserts exactly one row per table (8 rows total), then waits 2 seconds
before the next batch. There are no host/user, group-size or interval overrides.

```powershell
# Use the environment-variable name specified by source.password_env.
$env:CDC_SOURCE_PASSWORD = 'replace-with-source-writer-password'

# Start with a finite test: 3 batches, each adding 1 row per table (24 rows total).
& .\.venv\Scripts\python.exe generate_sample_data.py --batches 3

# Continuous: 1 row per table per batch, with a 2-second pause between batches.
& .\.venv\Scripts\python.exe generate_sample_data.py
```

The configured source must be the **writable primary** and its user must be able
to insert. The existing CDC configuration may point to a read replica and a
read-only account; that configuration cannot generate data. To preserve the CDC
endpoint/checkpoints, use a separate copy of the configuration with its source
set to the primary/writer, then pass `--config config.writer.yaml` to this script.
Do not change the running CDC source just to generate test data.

`--batches 0` (the default) runs until Ctrl+C. This continuously grows all eight
tables and their binlogs; use only an authorized test database with enough space.
The script never updates/deletes existing rows or disables foreign keys. Failed
inserts stop with an error and the uncommitted batch is rolled back on connection
close. A connection loss during COMMIT has an uncertain outcome; inspect before
retrying. Already committed rows are kept when you stop.

The writer needs SELECT/INSERT permissions, metadata visibility and permission
to set its ROW/FULL binlog session settings. The script explicitly rejects a
read-only server and validates supported InnoDB tables. It sets FULL images only
for its own connection, not global server settings.

Integer keys start above each table's current maximum; text keys use UUID-based
values. A named lock prevents simultaneous instances of this generator in the
same database, but not collisions with unrelated writers allocating integer keys.
Use a dedicated test database or coordinate writers. Duplicate keys are errors,
not silently ignored. All eight schema tables are always populated, even if CDC
is configured to replicate fewer tables. The source transaction is atomic; this
application's independent CDC workers do not preserve cross-table target atomicity.

## Tests and live acceptance checklist

For externally loading a large baseline before starting CDC, see
[Backup/restore CDC initialization](BACKUP_RESTORE_CDC_INITIALIZATION.md).
This is an advanced operator procedure; the Initialize button always performs
the built-in Python snapshot and does not adopt restored data.

```powershell
& .\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Unit tests use in-memory transactional doubles and Flask's HTTP test client.
They do not connect to Azure, start Docker, or prove server compatibility.
Before trusting real replication, use disposable source/target test tables:

1. Initialize while source writes continue. Wait for `running`, stop test writes,
   and compare source rows to both mapped destinations.
2. Commit INSERT, UPDATE, primary-key UPDATE and DELETE operations, including
   nullable values and composite keys if used. Verify both destinations.
3. Make a source transaction containing multiple row changes and roll back;
   verify no target changes. Then commit and verify the entire table's pair.
4. Pause, modify the source, then Resume. Verify missed changes are caught up.
5. Restart Python after a completed snapshot and verify checkpoint continuation.
6. Cause a second-target write failure in test tables. Confirm neither target
   commits the failing transaction, fix it and Resume.
7. Interrupt initialization and verify Resume refuses until Initialize is used.
8. Reinitialize and confirm stale destination rows are removed.

Live verification requires valid credentials, reachable endpoints and disposable
tables. No Azure server configuration, schema or data is modified merely by
creating this project.
