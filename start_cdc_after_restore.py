"""Register restored baselines for every table configured in config.yaml."""

from contextlib import closing
from cdc import CDCError, Manager, at_or_after, binlog_head, connect, load_config, qualified

# File and Position recorded on the CDC source BEFORE the database-wide backup.
C = {"file": "mysql-bin.000002", "position": 7179013}


def register_restored_tables(config, checkpoint):
    if type(checkpoint.get("position")) is not int or checkpoint["position"] < 4:
        raise CDCError("C must contain the valid position recorded before backup.")
    config["settings"]["auto_start"] = False
    manager = Manager(config)
    manager.initialize_store()
    workers = list(manager.workers.values())
    with closing(connect(config["source"])) as source, closing(connect(config["target"])) as target:
        with target.cursor() as cursor:
            for worker in workers:
                cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (f"cdc:{worker.job[:60]}",))
                if cursor.fetchone()["acquired"] != 1:
                    raise CDCError(f"Stop the existing CDC worker for {worker.name} first.")
        counts = {}
        for worker in workers:
            worker.check_retention(source, checkpoint)
            worker.prepare(source, target)
            target_counts = []
            with target.cursor() as cursor:
                for destination in worker.table["targets"]:
                    name = qualified(destination["database"], destination["table"])
                    cursor.execute(f"SELECT COUNT(*) AS n FROM {name}")
                    target_counts.append(cursor.fetchone()["n"])
            if len(set(target_counts)) != 1:
                raise CDCError(f"Restored target row counts differ for {worker.name}.")
            counts[worker.name] = target_counts[0]
        workers[0].check_retention(source, checkpoint)
        barrier = binlog_head(source)
        if not at_or_after(barrier, checkpoint):
            raise CDCError("C is beyond the current source head. Check the backup coordinates.")
        # All schema checks finish before any checkpoints are saved.
        for worker in workers:
            worker.save(
                target, "catching_up", checkpoint, barrier, counts[worker.name], 0, None,
            )
        target.commit()
    for worker in workers:
        print(f"Registered {worker.name}: {counts[worker.name]} baseline rows.")
    print("All checkpoints registered. Start the app and Resume; do not Initialize.")


if __name__ == "__main__":
    register_restored_tables(load_config("config.yaml"), C)