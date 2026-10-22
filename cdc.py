"""Small, per-table MySQL CDC workers with durable target-side checkpoints."""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import re
import ssl
import threading
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import pymysql
import yaml
from pymysql.cursors import DictCursor
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.constants import BINLOG
from pymysqlreplication.event import NotImplementedEvent, QueryEvent, XAPrepareEvent, XidEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent, PartialUpdateRowsEvent, UpdateRowsEvent, WriteRowsEvent,
)

LOG = logging.getLogger(__name__)
ROW_EVENTS = (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)
STRICT_MODE = (
    "STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)


class CDCError(RuntimeError):
    pass


class Stopped(Exception):
    pass


def ident(name):
    if not isinstance(name, str) or not name or len(name) > 64 or "\0" in name:
        raise CDCError(f"Invalid MySQL identifier: {name!r}")
    return "`" + name.replace("`", "``") + "`"


def qualified(database, table):
    return f"{ident(database)}.{ident(table)}"


def load_config(path):
    with Path(path).open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise CDCError("Configuration must be a YAML object.")
    for role in ("source", "target"):
        endpoint = config[role]
        for key in ("host", "user", "password_env", "database"):
            if not isinstance(endpoint.get(key), str) or not endpoint[key]:
                raise CDCError(f"{role}.{key} is required.")
        ident(endpoint["database"])
        endpoint.setdefault("port", 3306)
    if "disable_foreign_key_checks" in config["source"]:
        raise CDCError("disable_foreign_key_checks is only supported under target.")
    target = config["target"]
    target.setdefault("disable_foreign_key_checks", False)
    if type(target["disable_foreign_key_checks"]) is not bool:
        raise CDCError("target.disable_foreign_key_checks must be true or false.")
    if config["source"]["host"].lower() == config["target"]["host"].lower():
        raise CDCError("Source and target must be different servers.")
    settings = config.setdefault("settings", {})
    for key, default in (
        ("server_id_start", 71000), ("batch_size", 1000), ("poll_seconds", 2),
        ("metadata_table", "_python_cdc_state"), ("auto_start", True),
    ):
        settings.setdefault(key, default)
    for key in ("server_id_start", "batch_size", "poll_seconds"):
        if type(settings[key]) is not int or settings[key] < 1:
            raise CDCError(f"settings.{key} must be a positive integer.")
    ident(settings["metadata_table"])
    if type(settings["auto_start"]) is not bool:
        raise CDCError("settings.auto_start must be true or false.")
    web = config.setdefault("web", {})
    for key, default in (
        ("host", "127.0.0.1"), ("port", 8080),
    ):
        web.setdefault(key, default)
    tables = config.get("tables")
    if not isinstance(tables, list) or not tables:
        raise CDCError("At least one source table is required.")
    if settings["server_id_start"] + len(tables) - 1 > 4294967295:
        raise CDCError("Replication client IDs exceed the MySQL limit.")
    sources, destinations = set(), set()
    for table in tables:
        ident(table["source"])
        if table["source"] in sources:
            raise CDCError("Source table names must be unique.")
        sources.add(table["source"])
        targets = table.get("targets")
        if not isinstance(targets, list) or not 1 <= len(targets) <= 2:
            raise CDCError("Every source table must have one or two targets.")
        for index, target in enumerate(table["targets"]):
            target.setdefault("database", config["target"]["database"])
            ident(target["database"])
            ident(target["table"])
            key = (target["database"].casefold(), target["table"].casefold())
            if key in destinations:
                raise CDCError("A destination table cannot be shared by mappings.")
            destinations.add(key)
            if key == (
                config["target"]["database"].casefold(),
                settings["metadata_table"].casefold(),
            ):
                raise CDCError("A destination cannot be the checkpoint table.")
            columns = target.get("columns")
            constants = target.setdefault("constants", {})
            if columns is not None and (not isinstance(columns, dict) or not columns):
                raise CDCError("columns must be a nonempty source-to-target object.")
            if not isinstance(constants, dict):
                raise CDCError("constants must be an object.")
            for name in constants:
                ident(name)
            if columns:
                for source_name, target_name in columns.items():
                    ident(source_name)
                    ident(target_name)
                if len(set(columns.values())) != len(columns):
                    raise CDCError("Two source columns cannot map to one target column.")
                if set(columns.values()) & set(constants):
                    raise CDCError("Mapped columns and constants must not overlap.")
            if index == 0 and (columns is not None or constants):
                raise CDCError("The first target must use the unchanged source schema.")
    return config


def connection_options(endpoint):
    password = os.environ.get(endpoint["password_env"])
    if not password:
        raise CDCError(f"Set the {endpoint['password_env']} environment variable.")
    return dict(
        host=endpoint["host"], port=endpoint["port"], user=endpoint["user"],
        password=password, database=endpoint["database"], charset="utf8mb4",
        ssl=ssl.create_default_context(cafile=endpoint.get("ssl_ca")),
        connect_timeout=10, read_timeout=30, write_timeout=30,
        autocommit=False, init_command=(
            "SET time_zone = '+00:00', foreign_key_checks = 0"
            if endpoint.get("disable_foreign_key_checks", False)
            else "SET time_zone = '+00:00'"
        ),
        sql_mode=STRICT_MODE,
    )


def connect(endpoint):
    return pymysql.connect(**connection_options(endpoint), cursorclass=DictCursor)


def binlog_head(connection):
    with connection.cursor() as cursor:
        try:
            cursor.execute("SHOW BINARY LOG STATUS")
        except pymysql.err.ProgrammingError as error:
            if error.args[0] != 1064:
                raise
            cursor.execute("SHOW MASTER STATUS")
        row = cursor.fetchone()
        if not row:
            raise CDCError("Binary logging is disabled or its status is unavailable.")
        return {"file": row["File"], "position": int(row["Position"])}


def position_key(position):
    match = re.fullmatch(r"(.+)\.(\d+)", position["file"])
    if not match:
        raise CDCError("Unsupported binary log filename.")
    return match[1], int(match[2]), int(position["position"])


def at_or_after(position, barrier):
    left, right = position_key(position), position_key(barrier)
    if left[0] != right[0]:
        raise CDCError("Binary log lineage changed. Reinitialize the table.")
    return left >= right


def binlog_gap(checkpoint, head, logs):
    if not checkpoint or not head:
        raise CDCError("Source head or committed checkpoint is not available.")
    if at_or_after(checkpoint, head) and checkpoint != head:
        raise CDCError("Checkpoint is ahead of the observed source head. Check source lineage.")
    names = [row["Log_name"] for row in logs]
    if checkpoint["file"] not in names or head["file"] not in names:
        raise CDCError("A required binlog is unavailable or purged; backlog is unknown.")
    start, end = names.index(checkpoint["file"]), names.index(head["file"])
    if start > end:
        raise CDCError("Source binlog order does not match the checkpoint.")
    for position in (checkpoint, head):
        size = int(logs[names.index(position["file"])]["File_size"])
        if not 4 <= int(position["position"]) <= size:
            raise CDCError("A binlog position is outside its file; backlog is unknown.")
    remaining = sum(int(row["File_size"]) for row in logs[start:end])
    remaining += int(head["position"]) - int(checkpoint["position"])
    return {"bytes": remaining, "files": end - start}


def replication_progress(tables, head, logs, cutover):
    barrier = cutover["position"] if cutover else None
    for table in tables:
        lag = {"bytes": None, "files": None, "error": None, "cutover_reached": False}
        try:
            lag.update(binlog_gap(table["checkpoint"], head, logs))
            lag["cutover_reached"] = bool(
                barrier and table["status"] == "running"
                and at_or_after(table["checkpoint"], barrier)
            )
        except CDCError as error:
            lag["error"] = str(error)
        table["lag"] = lag
    caught_up = sum(
        table["status"] == "running" and table["lag"]["bytes"] == 0
        for table in tables
    )
    all_caught_up = bool(tables) and caught_up == len(tables)
    all_reached = bool(barrier and tables) and all(
        table["lag"]["cutover_reached"] for table in tables
    )
    unchanged = bool(head and barrier and head == barrier)
    return {
        "total_tables": len(tables), "caught_up_tables": caught_up,
        "all_caught_up": all_caught_up,
        "cutover": {
            "position": barrier,
            "captured_at": cutover["captured_at"] if cutover else None,
            "all_tables_reached": all_reached,
            "source_unchanged": unchanged,
            "ready": all_reached and all_caught_up and unchanged,
        },
    }


def encode_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, set):
        return ",".join(sorted(value))
    return value


def schema(connection, database, table):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (database, table),
        )
        info = cursor.fetchone()
        if not info or info["ENGINE"] != "InnoDB":
            raise CDCError(f"{database}.{table} must be an existing InnoDB table.")
        cursor.execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, "
            "COLLATION_NAME, GENERATION_EXPRESSION FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
            (database, table),
        )
        columns = cursor.fetchall()
        if any(c["GENERATION_EXPRESSION"] for c in columns):
            raise CDCError("Generated columns are not supported by this PoC.")
        if any(c["COLUMN_TYPE"].split("(")[0].lower() in {
            "geometry", "point", "linestring", "polygon", "multipoint",
            "multilinestring", "multipolygon", "geometrycollection", "vector", "json", "bit",
        } for c in columns):
            raise CDCError("JSON, BIT, spatial and vector columns are not supported by this PoC.")
        cursor.execute(
            "SELECT COLUMN_NAME FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND INDEX_NAME='PRIMARY' "
            "ORDER BY SEQ_IN_INDEX", (database, table),
        )
        primary_key = [row["COLUMN_NAME"] for row in cursor.fetchall()]
        if not primary_key:
            raise CDCError(f"{database}.{table} requires a primary key.")
        return columns, primary_key


def validate_target(connection, database, table, disable_foreign_key_checks=False):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) AS n FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s "
            "AND NON_UNIQUE=0 AND INDEX_NAME <> 'PRIMARY'", (database, table),
        )
        if cursor.fetchone()["n"]:
            raise CDCError("Targets cannot have UNIQUE indexes other than PRIMARY.")
        cursor.execute(
            "SELECT COUNT(*) AS n FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE (TABLE_SCHEMA=%s AND TABLE_NAME=%s AND REFERENCED_TABLE_NAME IS NOT NULL) "
            "OR (REFERENCED_TABLE_SCHEMA=%s AND REFERENCED_TABLE_NAME=%s)",
            (database, table, database, table),
        )
        if cursor.fetchone()["n"] and not disable_foreign_key_checks:
            raise CDCError("Targets cannot have inbound or outbound foreign keys.")
        cursor.execute(
            "SELECT COUNT(*) AS n FROM information_schema.TRIGGERS "
            "WHERE EVENT_OBJECT_SCHEMA=%s AND EVENT_OBJECT_TABLE=%s", (database, table),
        )
        if cursor.fetchone()["n"]:
            raise CDCError("Targets cannot have triggers.")


class Destination:
    def __init__(self, spec, source_columns, primary_key):
        self.name = qualified(spec["database"], spec["table"])
        self.mapping = spec.get("columns") or {name: name for name in source_columns}
        self.constants = spec["constants"]
        if not set(self.mapping) <= set(source_columns):
            raise CDCError(f"Unknown source columns for {self.name}.")
        if not set(primary_key) <= set(self.mapping):
            raise CDCError("Every source primary-key column must be mapped.")
        self.keys = [(key, self.mapping[key]) for key in primary_key]
        names = list(self.mapping.values()) + list(self.constants)
        if len(set(names)) != len(names):
            raise CDCError("Mapped columns and constants must not overlap.")
        self.columns = names
        updates = ", ".join(f"{ident(c)}=VALUES({ident(c)})" for c in names)
        self.upsert_sql = (
            f"INSERT INTO {self.name} ({', '.join(map(ident, names))}) "
            f"VALUES ({', '.join(['%s'] * len(names))}) ON DUPLICATE KEY UPDATE {updates}"
        )
        self.delete_sql = f"DELETE FROM {self.name} WHERE " + " AND ".join(
            f"{ident(target)}=%s" for _, target in self.keys
        )

    def values(self, row):
        return tuple(encode_value(row[key]) for key in self.mapping) + tuple(
            encode_value(value) for value in self.constants.values()
        )

    def upsert(self, cursor, row):
        cursor.execute(self.upsert_sql, self.values(row))

    def delete(self, cursor, row):
        cursor.execute(self.delete_sql, tuple(row[key] for key, _ in self.keys))


class Worker:
    def __init__(self, config, table, server_id):
        self.config, self.table, self.server_id = config, table, server_id
        self.name = table["source"]
        self.meta = qualified(
            config["target"]["database"], config["settings"]["metadata_table"],
        )
        identity = {
            "source": {k: config["source"][k] for k in ("host", "port", "database")},
            "target": {k: config["target"][k] for k in ("host", "port", "database")},
            "table": table,
        }
        self.config_hash = hashlib.sha256(
            json.dumps(identity, sort_keys=True, default=str).encode()
        ).hexdigest()
        self.base_hash = self.config_hash
        self.job = hashlib.sha256(
            f"{config['source']['host']}:{config['source']['port']}/"
            f"{config['source']['database']}/{self.name}".encode()
        ).hexdigest()
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.thread = None
        self.state = dict(
            table=self.name, status="not_initialized", error=None, checkpoint=None,
            snapshot_rows=0, changes=0, last_event_at=None, barrier=None,
            targets=[f"{t['database']}.{t['table']}" for t in table["targets"]],
        )

    def update(self, **values):
        with self.lock:
            self.state.update(values)

    def status(self):
        with self.lock:
            return copy.deepcopy(self.state)

    def start(self, initialize=False):
        with self.lock:
            if self.thread and self.thread.is_alive():
                raise CDCError(f"{self.name} is busy. Pause it first.")
            self.stop.clear()
            self.state.update(status="starting", error=None)
            self.thread = threading.Thread(
                target=self.run, args=(initialize,), name=f"cdc-{self.name}", daemon=True,
            )
            self.thread.start()

    def pause(self):
        with self.lock:
            if self.thread and self.thread.is_alive():
                self.state["status"] = "pausing"
                self.stop.set()

    def check_stop(self):
        if self.stop.is_set():
            raise Stopped()

    def read_checkpoint(self, target):
        with target.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.meta} WHERE job=%s", (self.job,))
            return cursor.fetchone()

    def save(self, target, phase, checkpoint, barrier, snapshot_rows, changes, event_at):
        with target.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {self.meta} "
                "(job, config_hash, phase, log_file, log_pos, barrier_file, barrier_pos, "
                "snapshot_rows, changes, event_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(6)) "
                "ON DUPLICATE KEY UPDATE config_hash=VALUES(config_hash), phase=VALUES(phase), "
                "log_file=VALUES(log_file), log_pos=VALUES(log_pos), "
                "barrier_file=VALUES(barrier_file), barrier_pos=VALUES(barrier_pos), "
                "snapshot_rows=VALUES(snapshot_rows), changes=VALUES(changes), "
                "event_at=VALUES(event_at), updated_at=VALUES(updated_at)",
                (
                    self.job, self.config_hash, phase,
                    checkpoint["file"] if checkpoint else None,
                    checkpoint["position"] if checkpoint else None,
                    barrier["file"] if barrier else None,
                    barrier["position"] if barrier else None,
                    snapshot_rows, changes, event_at,
                ),
            )

    def prepare(self, source, target):
        with source.cursor() as cursor:
            cursor.execute(
                "SELECT @@log_bin AS log_bin, @@binlog_format AS format, "
                "@@binlog_row_image AS image, @@binlog_row_metadata AS metadata, "
                "@@binlog_transaction_compression AS compression, "
                "@@binlog_row_value_options AS row_options"
            )
            settings = cursor.fetchone()
            if not (
                settings["log_bin"] and settings["format"] == "ROW"
                and settings["image"] == "FULL" and settings["metadata"] == "FULL"
                and not settings["compression"] and not settings["row_options"]
            ):
                raise CDCError(
                    "Source requires log_bin=ON, binlog_format=ROW, "
                    "binlog_row_image=FULL, binlog_row_metadata=FULL, "
                    "binlog_transaction_compression=OFF and empty binlog_row_value_options."
                )
        columns, self.primary_key = schema(
            source, self.config["source"]["database"], self.name,
        )
        self.source_columns = [c["COLUMN_NAME"] for c in columns]
        self.destinations = []
        fingerprints = [columns, self.primary_key]
        disable_foreign_key_checks = self.config["target"]["disable_foreign_key_checks"]
        if disable_foreign_key_checks:
            LOG.warning(
                "Foreign key checks are disabled for target sessions replicating %s. "
                "Referential integrity is not enforced or automatically revalidated.",
                self.name,
            )
        for index, spec in enumerate(self.table["targets"]):
            target_columns, target_pk = schema(target, spec["database"], spec["table"])
            validate_target(
                target, spec["database"], spec["table"],
                disable_foreign_key_checks=disable_foreign_key_checks,
            )
            destination = Destination(spec, self.source_columns, self.primary_key)
            if target_pk != [mapped for _, mapped in destination.keys]:
                raise CDCError("Target PRIMARY KEY must equal the mapped source PRIMARY KEY.")
            if index == 0 and columns != target_columns:
                raise CDCError("The first target must have identical column definitions.")
            by_name = {c["COLUMN_NAME"]: c for c in target_columns}
            if not set(destination.columns) <= set(by_name):
                raise CDCError(f"Unknown target columns for {destination.name}.")
            for source_name, target_name in destination.mapping.items():
                original = next(c for c in columns if c["COLUMN_NAME"] == source_name)
                mapped = by_name[target_name]
                if any(original[k] != mapped[k] for k in (
                    "COLUMN_TYPE", "IS_NULLABLE", "COLLATION_NAME",
                )):
                    raise CDCError("Mapped column types, nullability and collation must match.")
            for column in target_columns:
                if (
                    column["COLUMN_NAME"] not in destination.columns
                    and column["IS_NULLABLE"] == "NO" and column["COLUMN_DEFAULT"] is None
                    and "auto_increment" not in column["EXTRA"]
                ):
                    raise CDCError("Unmapped target columns need a default or must allow NULL.")
            self.destinations.append(destination)
            fingerprints.extend([target_columns, target_pk])
        self.config_hash = hashlib.sha256(
            (self.base_hash + json.dumps(fingerprints, sort_keys=True, default=str)).encode()
        ).hexdigest()
        source.rollback()
        target.rollback()

    def snapshot(self, source, target):
        # Capture before the snapshot; overlap is replayed using FULL row images.
        start = binlog_head(source)
        self.save(target, "initializing", start, None, 0, 0, None)
        target.commit()
        self.update(status="initializing", checkpoint=start, snapshot_rows=0, changes=0)
        with target.cursor() as cursor:
            for destination in self.destinations:
                cursor.execute(f"DELETE FROM {destination.name}")
        target.commit()
        source.rollback()
        with source.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        count = 0
        try:
            with source.cursor(pymysql.cursors.SSDictCursor) as cursor:
                cursor.execute(
                    f"SELECT {', '.join(map(ident, self.source_columns))} FROM "
                    f"{qualified(self.config['source']['database'], self.name)}"
                )
                while True:
                    self.check_stop()
                    rows = cursor.fetchmany(self.config["settings"]["batch_size"])
                    if not rows:
                        break
                    with target.cursor() as output:
                        for destination in self.destinations:
                            output.executemany(
                                destination.upsert_sql, [destination.values(row) for row in rows],
                            )
                    count += len(rows)
                    self.save(target, "initializing", start, None, count, 0, None)
                    target.commit()
                    self.update(snapshot_rows=count)
        finally:
            source.rollback()
        barrier = binlog_head(source)
        self.save(target, "catching_up", start, barrier, count, 0, None)
        target.commit()
        self.update(status="catching_up", barrier=barrier)

    def apply(self, cursor, event):
        count = 0
        for row in event.rows:
            before = row.get("before_values", row.get("values"))
            after = row.get("after_values", row.get("values"))
            if isinstance(event, UpdateRowsEvent):
                images = (
                    ("before_none_sources", before, self.primary_key),
                    ("after_none_sources", after, self.source_columns),
                )
            elif isinstance(event, DeleteRowsEvent):
                images = (("none_sources", before, self.primary_key),)
            else:
                images = (("none_sources", after, self.source_columns),)
            for metadata, values, required_columns in images:
                if (
                    values is None
                    or not set(required_columns) <= set(values)
                    or not set(values) <= set(self.source_columns)
                ):
                    raise CDCError("Required row columns are missing or a schema change was detected.")
                for name, reason in row.get(metadata, {}).items():
                    if name not in required_columns:
                        continue
                    if reason not in {"null", "empty set"}:
                        details = {
                            "image": metadata,
                            "event": type(event).__name__,
                            "event_time_utc": datetime.fromtimestamp(
                                event.timestamp, timezone.utc,
                            ).isoformat(),
                            "origin_server_id": getattr(
                                getattr(event, "packet", None), "server_id", None,
                            ),
                            "event_column_count": getattr(event, "number_of_columns", None),
                            "expected_column_count": len(self.source_columns),
                        }
                        for label, attribute in (
                            ("before_or_values_bitmap", "columns_present_bitmap"),
                            ("after_bitmap", "columns_present_bitmap2"),
                        ):
                            bitmap = getattr(event, attribute, None)
                            if isinstance(bitmap, (bytes, bytearray)):
                                details[label] = bitmap.hex()
                                details[label + "_omitted_columns"] = [
                                    column for index, column in enumerate(self.source_columns)
                                    if index // 8 < len(bitmap)
                                    and not bitmap[index // 8] & (1 << (index % 8))
                                ]
                        raise CDCError(
                            f"Unsupported row value for {name}: {reason}; "
                            f"event_details={json.dumps(details, sort_keys=True)}"
                        )
                    if reason == "empty set":
                        values[name] = ""
                if any(values[key] is None for key in self.primary_key):
                    raise CDCError("A required primary-key value is NULL or missing.")
            for destination in self.destinations:
                if isinstance(event, DeleteRowsEvent):
                    destination.delete(cursor, before)
                else:
                    if isinstance(event, UpdateRowsEvent) and any(
                        before[key] != after[key] for key in self.primary_key
                    ):
                        destination.delete(cursor, before)
                    destination.upsert(cursor, after)
            count += 1
        return count

    def check_retention(self, source, checkpoint):
        with source.cursor() as cursor:
            cursor.execute("SHOW BINARY LOGS")
            logs = {row["Log_name"]: row["File_size"] for row in cursor.fetchall()}
        if checkpoint["file"] not in logs:
            raise CDCError("The checkpoint binlog was purged. Reinitialize this table.")
        if checkpoint["position"] > logs[checkpoint["file"]]:
            raise CDCError("The binlog is shorter than the checkpoint. Reinitialize.")

    def replicate(self, source, target, saved):
        checkpoint = {"file": saved["log_file"], "position": saved["log_pos"]}
        barrier = {"file": saved["barrier_file"], "position": saved["barrier_pos"]}
        copied, changes, event_at = saved["snapshot_rows"], saved["changes"], saved["event_at"]
        phase = saved["phase"]
        self.update(
            status=phase, checkpoint=checkpoint, barrier=barrier, snapshot_rows=copied,
            changes=changes, last_event_at=event_at,
        )
        while True:
            self.check_stop()
            self.check_retention(source, checkpoint)
            stream = BinLogStreamReader(
                connection_settings=connection_options(self.config["source"]),
                server_id=self.server_id, log_file=checkpoint["file"],
                log_pos=checkpoint["position"], resume_stream=True, blocking=False,
                only_schemas=[self.config["source"]["database"]], only_tables=[self.name],
                only_events=[
                    *ROW_EVENTS, XidEvent, QueryEvent, NotImplementedEvent,
                    PartialUpdateRowsEvent, XAPrepareEvent,
                ],
                filter_non_implemented_events=False, verify_checksum=True, enable_logging=False,
            )
            pending, pending_time, in_transaction = 0, event_at, False
            try:
                for event in stream:
                    self.check_stop()
                    if isinstance(event, NotImplementedEvent):
                        if event.event_type == BINLOG.ANONYMOUS_GTID_LOG_EVENT:
                            in_transaction = True
                            continue
                        raise CDCError(f"Unsupported binlog event type: {event.event_type}")
                    if isinstance(event, (PartialUpdateRowsEvent, XAPrepareEvent)):
                        raise CDCError("Partial row images and XA transactions are not supported.")
                    if isinstance(event, ROW_EVENTS):
                        in_transaction = True
                        with target.cursor() as cursor:
                            try:
                                pending += self.apply(cursor, event)
                            except CDCError as error:
                                raise CDCError(
                                    f"{error}; source_endpoint={self.config['source']['host']}; "
                                    f"event_end={stream.log_file}:{stream.log_pos}; "
                                    f"committed_checkpoint={checkpoint['file']}:"
                                    f"{checkpoint['position']}"
                                ) from error
                        pending_time = datetime.fromtimestamp(event.timestamp, timezone.utc).replace(
                            tzinfo=None,
                        )
                    elif isinstance(event, QueryEvent):
                        query = event.query.strip().upper()
                        if query not in {"BEGIN", "COMMIT", "ROLLBACK"}:
                            # DDL and statement-based DML must never be silently skipped.
                            raise CDCError(
                                "A DDL or statement event was encountered. "
                                "Keep source schemas fixed; investigate, then reinitialize."
                            )
                        if query == "ROLLBACK":
                            target.rollback()
                            pending, pending_time = 0, event_at
                            in_transaction = False
                        if query == "BEGIN":
                            in_transaction = True
                        if query != "COMMIT":
                            continue
                    if isinstance(event, XidEvent) or (
                        isinstance(event, QueryEvent) and event.query.strip().upper() == "COMMIT"
                    ):
                        next_position = {"file": stream.log_file, "position": stream.log_pos}
                        if at_or_after(next_position, barrier):
                            phase = "running"
                        self.save(
                            target, phase, next_position, barrier,
                            copied, changes + pending, pending_time,
                        )
                        target.commit()
                        checkpoint, changes, event_at = next_position, changes + pending, pending_time
                        pending = 0
                        in_transaction = False
                        self.update(
                            status=phase, checkpoint=checkpoint, changes=changes,
                            last_event_at=event_at,
                        )
                # EOF can be inside a transaction. Never persist its row-event position.
                target.rollback()
                if not in_transaction:
                    # Keep idle checkpoints current, including safe rotation/format events.
                    next_position = {"file": stream.log_file, "position": stream.log_pos}
                    if at_or_after(next_position, barrier):
                        phase = "running"
                    self.save(
                        target, phase, next_position, barrier, copied, changes, event_at,
                    )
                    target.commit()
                    checkpoint = next_position
                self.update(status=phase, checkpoint=checkpoint)
            finally:
                stream.close()
            self.stop.wait(self.config["settings"]["poll_seconds"])

    def run(self, initialize):
        try:
            with closing(connect(self.config["source"])) as source, closing(
                connect(self.config["target"])
            ) as target:
                with target.cursor() as cursor:
                    cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (f"cdc:{self.job[:60]}",))
                    if cursor.fetchone()["acquired"] != 1:
                        raise CDCError("Another process owns this replication job.")
                self.prepare(source, target)
                saved = self.read_checkpoint(target)
                if initialize:
                    self.snapshot(source, target)
                    saved = self.read_checkpoint(target)
                elif not saved:
                    raise CDCError("No snapshot exists. Use Initialize first.")
                elif saved["config_hash"] != self.config_hash:
                    raise CDCError("Mapping changed. Reinitialize before resuming.")
                elif saved["phase"] == "initializing":
                    raise CDCError("Snapshot was interrupted. Reinitialize before resuming.")
                self.replicate(source, target, saved)
        except Stopped:
            self.update(status="paused")
        except Exception as error:
            # A worker boundary: rollback/close happens above; surface every failure.
            LOG.exception("CDC worker failed for %s", self.name)
            self.update(status="error", error=f"{type(error).__name__}: {error}")


class Manager:
    def __init__(self, config):
        self.config = config
        self.cutover_lock = threading.Lock()
        self.cutover = None
        self.workers = {
            table["source"]: Worker(config, table, config["settings"]["server_id_start"] + index)
            for index, table in enumerate(config["tables"])
        }

    def initialize_store(self):
        meta = qualified(
            self.config["target"]["database"], self.config["settings"]["metadata_table"],
        )
        with closing(connect(self.config["target"])) as target:
            with target.cursor() as cursor:
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {meta} ("
                    "job CHAR(64) PRIMARY KEY, config_hash CHAR(64) NOT NULL, "
                    "phase VARCHAR(24) NOT NULL, log_file VARCHAR(255), log_pos BIGINT UNSIGNED, "
                    "barrier_file VARCHAR(255), barrier_pos BIGINT UNSIGNED, "
                    "snapshot_rows BIGINT UNSIGNED NOT NULL, changes BIGINT UNSIGNED NOT NULL, "
                    "event_at DATETIME(6), updated_at DATETIME(6) NOT NULL) ENGINE=InnoDB"
                )
            target.commit()
            for worker in self.workers.values():
                saved = worker.read_checkpoint(target)
                if saved:
                    worker.update(
                        status="paused" if saved["phase"] != "initializing" else "needs_initialize",
                        checkpoint={"file": saved["log_file"], "position": saved["log_pos"]},
                        snapshot_rows=saved["snapshot_rows"], changes=saved["changes"],
                        last_event_at=saved["event_at"],
                    )
                    if self.config["settings"]["auto_start"] and saved["phase"] != "initializing":
                        worker.start()

    def capture_cutover(self):
        with closing(connect(self.config["source"])) as source:
            position = binlog_head(source)
        captured = {
            "position": position,
            "captured_at": datetime.now(timezone.utc).isoformat(),
        }
        with self.cutover_lock:
            self.cutover = captured
        LOG.info("Captured source cutover position %s:%s", position["file"], position["position"])
        return copy.deepcopy(captured)

    def status(self):
        result = {
            "tables": [worker.status() for worker in self.workers.values()],
            "servers": {},
        }
        logs = []
        for role in ("source", "target"):
            endpoint = self.config[role]
            info = {"host": endpoint["host"], "database": endpoint["database"]}
            try:
                with closing(connect(endpoint)) as connection:
                    info["binlog"] = binlog_head(connection)
                    if role == "source":
                        with connection.cursor() as cursor:
                            cursor.execute("SHOW BINARY LOGS")
                            logs = cursor.fetchall()
            except (pymysql.MySQLError, CDCError, OSError) as error:
                LOG.warning("Cannot read %s binlog status: %s", role, error)
                info["error"] = f"{type(error).__name__}: {error}"
            result["servers"][role] = info
        with self.cutover_lock:
            cutover = copy.deepcopy(self.cutover)
        source_info = result["servers"]["source"]
        head = source_info.get("binlog") if not source_info.get("error") else None
        result["progress"] = replication_progress(result["tables"], head, logs, cutover)
        return result

    def close(self):
        for worker in self.workers.values():
            worker.pause()
        for worker in self.workers.values():
            if worker.thread:
                worker.thread.join()
