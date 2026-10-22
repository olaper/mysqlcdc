# Start CDC from a Backup/Restore

**Flow: record source position C -> dump -> restore configured targets -> register C -> Resume.**

This procedure uses a logical backup, not Azure managed PITR. Run all source
commands against the same read replica that CDC reads:
`gphinblue-poc-rep.mysql.database.azure.com`.

Before starting:

- Stop the CDC application and set `settings.auto_start: false` in [config.yaml](config.yaml).
- Source tables must be InnoDB. Keep schemas unchanged and retain the source
  binlogs until restore and CDC catch-up finish.
- Source events must have complete after-images. Both targets must satisfy the
  existing [CDC prerequisites](README.md), be empty and pre-created.
- Commands assume MySQL client tools are installed. The first option uses
  `cdc-blue` / `cdc-green` login paths and a CA file; the alternative below
  prompts for passwords without requiring login paths or a CA file.
  Create `C:\cdc-backup` first. Check every command succeeds before continuing.

## 1. Record the starting binlog position BEFORE backup

Run on the blue read replica:

```sql
SHOW BINARY LOG STATUS;       -- MySQL 8.4
-- SHOW MASTER STATUS; -- Use this instead on MySQL 8.0.
```

Record `File` and `Position` as **C**, for example:

```text
File: mysql-bin.000002
Position: 206561
```

Use the actual result, not these example values. Do not use the primary's
position, the target's position, or a position captured after backup.

## 2. Back up and restore the first target

PowerShell, from the project directory:

```powershell
mysqldump --login-path=cdc-blue `
  --ssl-mode=VERIFY_IDENTITY --ssl-ca=C:\certificates\mysql-ca.pem `
  --single-transaction --skip-lock-tables --set-gtid-purged=OFF `
  --no-tablespaces --no-create-info --skip-triggers --hex-blob --complete-insert `
  --result-file=C:\cdc-backup\customers.sql ghbin_sampledb customers
if ($LASTEXITCODE -ne 0) { throw "Backup failed" }

# cmd handles input redirection without PowerShell text-encoding conversion.
cmd.exe /c 'mysql --login-path=cdc-green --ssl-mode=VERIFY_IDENTITY --ssl-ca=C:\certificates\mysql-ca.pem --database=ghbin_newdb < C:\cdc-backup\customers.sql'
if ($LASTEXITCODE -ne 0) { throw "Restore failed" }
```

This restores data into green's pre-created `ghbin_newdb.customers`.
The source remains writable. Because C was captured before the snapshot, CDC
will replay an overlapping range after restore; that is intentional.

### Alternative: username and password prompt, without a CA file

Use this **instead of** the commands above. Replace the usernames with accounts
having the necessary backup/restore permissions. Each command prompts for its
server's password; do not put the password in the command or this document.

```powershell
mysqldump --host=gphinblue-poc-rep.mysql.database.azure.com `
  --port=3306 --user=cdc_reader --password --ssl-mode=REQUIRED `
  --single-transaction --skip-lock-tables --set-gtid-purged=OFF `
  --no-tablespaces --no-create-info --skip-triggers --hex-blob --complete-insert `
  --result-file=C:\cdc-backup\customers.sql ghbin_sampledb customers
if ($LASTEXITCODE -ne 0) { throw "Backup failed" }

cmd.exe /c 'mysql --host=gphingreen-poc.mysql.database.azure.com --port=3306 --user=cdc_writer --password --ssl-mode=REQUIRED --database=ghbin_newdb < C:\cdc-backup\customers.sql'
if ($LASTEXITCODE -ne 0) { throw "Restore failed" }
```

Username/password authentication and TLS are separate: Azure MySQL normally
requires encrypted connections. `--ssl-mode=REQUIRED` keeps TLS enabled without
specifying a CA file, but **does not verify the server certificate or hostname**.
Prefer the `VERIFY_IDENTITY` option above for production or untrusted networks.
This alternative does not disable SSL/TLS or change Azure server settings.

### Whole database instead of one table

For all table data in `ghbin_sampledb`, remove the trailing `customers` table
argument and change the output filename:

```powershell
mysqldump --host=gphinblue-poc-rep.mysql.database.azure.com `
  --port=3306 --user=cdc_reader --password --ssl-mode=REQUIRED `
  --single-transaction --skip-lock-tables --set-gtid-purged=OFF `
  --no-tablespaces --no-create-info --skip-triggers --hex-blob --complete-insert `
  --result-file=C:\cdc-backup\ghbin_sampledb.sql ghbin_sampledb
if ($LASTEXITCODE -ne 0) { throw "Backup failed" }

cmd.exe /c 'mysql --host=gphingreen-poc.mysql.database.azure.com --port=3306 --user=cdc_writer --password --ssl-mode=REQUIRED --database=ghbin_newdb < C:\cdc-backup\ghbin_sampledb.sql'
if ($LASTEXITCODE -ne 0) { throw "Restore failed" }
```

This copies all base-table rows into pre-created same-name target tables, not
users, grants, routines, events or schema definitions. Do not add `--databases`:
that can emit statements selecting the original database rather than `ghbin_newdb`.
The same C captured before this database-wide dump can be used for every table.
Only tables listed in `config.yaml` receive subsequent CDC.

### Alternative to mysqldump: MySQL Shell parallel dump/load

For large databases, **MySQL Shell (`mysqlsh`)** supports parallel, chunked
dump/load. Use a current compatible version (8.0.31+ for single-schema remapping).
Install it separately; it is not the `mysql` command-line client.

After recording C, connect to blue from PowerShell:

```powershell
mysqlsh --js --host=gphinblue-poc-rep.mysql.database.azure.com `
  --port=3306 --user=backup_reader --password --ssl-mode=REQUIRED
```

At the MySQL Shell JavaScript prompt:

```javascript
util.dumpSchemas(["ghbin_sampledb"], "C:\\cdc-backup\\sampledb-shell", {
    threads: 8,
    consistent: true,
    dataOnly: true
});
```

Use a new/empty dump directory. This backs up the entire schema's table data,
not just `customers`. Keep the complete dump directory and its metadata together.
`consistent: true` coordinates snapshots and can briefly acquire read locks;
it is **not a zero-lock guarantee**. Use a backup account with the privileges
required by the installed Shell version, rather than assuming `cdc_reader`
is sufficient. If source locking is unacceptable, use the single-transaction
mysqldump option above instead.

Connect to green from another PowerShell session:

```powershell
mysqlsh --js --host=gphingreen-poc.mysql.database.azure.com `
  --port=3306 --user=restore_writer --password --ssl-mode=REQUIRED
```

At the MySQL Shell JavaScript prompt:

```javascript
util.loadDump("C:\\cdc-backup\\sampledb-shell", {
    schema: "ghbin_newdb",
    threads: 8,
    loadDdl: false
});
```

All destination tables must already exist and be empty. The target must allow
`local_infile=ON` for the load utility; arrange the server setting and appropriate
load privileges with the administrator. Do not bypass errors or consistency
checks. Start with fewer threads if source/target load or IOPS becomes excessive.

Both command variants use encrypted TLS without certificate identity verification;
use `VERIFY_IDENTITY` and your CA file for production.

Other choices:

| Method | When to use |
|---|---|
| MySQL Shell `dumpSchemas` / `loadDump` | Parallel logical backup/restore into the existing green server; recommended to evaluate for a large database. |
| mydumper / myloader | Another parallel logical option, commonly run on Linux/WSL. Coordinate a consistent snapshot and review locking/version-specific options. |
| Azure managed backup / PITR | Whole-server recovery to a **new server**, not selected database restore into the existing green server. It does not provide an interchangeable checkpoint for the live blue replica. |

References: [MySQL Shell dump utilities](https://docs.oracle.com/cd/E17952_01/mysql-shell-8.0-en/mysql-shell-utilities-dump-instance-schema.html),
[load utility](https://docs.oracle.com/cd/E17952_01/mysql-shell-8.0-en/mysql-shell-utilities-load-dump.html).

## 3. Populate any second targets from the restored first targets

The database-wide restore populates the eight same-name first targets. Only
`customers` currently has a second target; populate `customers_new` separately.
Run on green. This matches the current configuration's identical column names:

```sql
INSERT INTO ghbin_newdb.customers_new (
    customerNumber, customerName, contactLastName, contactFirstName,
    phone, addressLine1, addressLine2, city, state, postalCode,
    country, salesRepEmployeeNumber, creditLimit
)
SELECT
    customerNumber, customerName, contactLastName, contactFirstName,
    phone, addressLine1, addressLine2, city, state, postalCode,
    country, salesRepEmployeeNumber, creditLimit
FROM ghbin_newdb.customers;
```

For a very large table, run this in non-overlapping primary-key ranges with a
commit per range. For different column mappings, adjust the column lists.
Verify both targets contain the same restored baseline before proceeding.

## 4. Register C as the CDC checkpoint, then Resume

**There is no YAML setting for an initial binlog position.** The worker reads it
from green's `ghbin_newdb._python_cdc_state` table. Use the existing Python
helpers below to write the correct job ID, configuration hash and position.

Keep the application stopped. Set the normal source/target password environment
variables. Replace C below with the values recorded in step 1, then execute:
For a database-wide restore, this registers all configured tables using the
same C. For a customers-only restore, set `TABLES = ["customers"]` instead.
Never register a table whose configured targets have not all been loaded.

Alternatively, the project script `start_cdc_after_restore.py` reads every table
and its target list from `config.yaml`. Set its C to the actual pre-backup
coordinate, then run `python .\start_cdc_after_restore.py` with the app stopped.
Unlike the per-table example below, that script validates all tables before
registering all checkpoints in one transaction. It does not run live CDC itself.
An existing registered baseline is replaced, so only run it to adopt restored
data, not as a routine application startup command.

```powershell
@'
from contextlib import closing
from cdc import CDCError, Manager, binlog_head, connect, load_config, qualified

config = load_config("config.yaml")
config["settings"]["auto_start"] = False
manager = Manager(config)
manager.initialize_store()
TABLES = list(manager.workers)  # For a customers-only restore: ["customers"].

# REPLACE with the File and Position recorded BEFORE backup.
C = {"file": "mysql-bin.000002", "position": 206561}

for table in TABLES:
    worker = manager.workers[table]
    with closing(connect(config["source"])) as source, closing(connect(config["target"])) as target:
        with target.cursor() as cursor:
            cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (f"cdc:{worker.job[:60]}",))
            if cursor.fetchone()["acquired"] != 1:
                raise CDCError("Stop the existing CDC worker first.")
        worker.prepare(source, target)
        worker.check_retention(source, C)
        H = binlog_head(source)  # Current source head: catch-up completion barrier.
        counts = []
        with target.cursor() as cursor:
            for destination in worker.table["targets"]:
                name = qualified(destination["database"], destination["table"])
                cursor.execute(f"SELECT COUNT(*) AS n FROM {name}")
                counts.append(cursor.fetchone()["n"])
        if len(set(counts)) != 1:
            raise CDCError(f"Restored target row counts differ for {table}.")
        worker.save(target, "catching_up", C, H, counts[0], 0, None)
        target.commit()
    print(f"Registered {table}")
print("Checkpoint registered. Start the app and click Resume, not Initialize.")
'@ | & .\.venv\Scripts\python.exe -
if ($LASTEXITCODE -ne 0) { throw "Checkpoint registration failed" }
```

The two key arguments are:

```python
worker.save(target, "catching_up", C, H, count, 0, None)
#                              ^  ^
#                              |  Catch up to this source head before showing running.
#                              Start reading the binlog at the pre-backup position.
```

Counts alone do not prove correctness: validate restored values as well.
Registration commits per table. If it fails partway through, keep the app stopped,
fix the problem and finish registration before starting any workers.

Start the application normally and click **Resume** for each registered table. It replays from C, reaches H,
then continues live CDC. Keep targets out of use until catch-up and validation finish.

**Do not click Initialize:** it deletes both restored targets and recopies the
source. If C's binlog has expired, take a new backup with a new C; do not guess
a later checkpoint. The registration recipe uses internal helpers, so review
it if the application changes.
