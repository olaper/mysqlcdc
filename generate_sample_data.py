"""Continuously insert related synthetic rows into the classicmodels schema."""

import argparse
import hashlib
import math
import time
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from cdc import CDCError, connect, load_config, qualified, schema


TABLES = (
    "offices", "employees", "customers", "productlines",
    "products", "orders", "orderdetails", "payments",
)
INTEGER_KEYS = {
    "employees": "employeeNumber", "customers": "customerNumber", "orders": "orderNumber",
}


def prepare(connection, database):

    for table in TABLES:
        schema(connection, database, table)
    connection.rollback()


def next_ids(cursor, database, groups):
    result = {}
    for table, column in INTEGER_KEYS.items():
        cursor.execute(
            f"SELECT COALESCE(MAX(`{column}`), 0) AS last_id FROM {qualified(database, table)}"
        )
        first = max(cursor.fetchone()["last_id"], 0) + 1
        if first + groups - 1 > 2147483647:
            raise CDCError(f"No signed INT key space remains in {table}.{column}.")
        result[table] = first
    return result


def insert_group(cursor, database, employee, customer, order):
    token = uuid.uuid4().hex
    office = token[:10]
    product_line = f"CDC Line {token}"
    product = "CDC" + token[:12]
    today = datetime.now(timezone.utc).date()
    quantity, price = 2, Decimal("125.50")

    def insert(table, values):
        columns = ", ".join(f"`{column}`" for column in values)
        placeholders = ", ".join(["%s"] * len(values))
        cursor.execute(
            f"INSERT INTO {qualified(database, table)} ({columns}) VALUES ({placeholders})",
            tuple(values.values()),
        )

    insert("offices", {
        "officeCode": office, "city": "Sample City", "phone": "+1-202-555-0100",
        "addressLine1": "100 Sample Street", "addressLine2": None, "state": "CA",
        "country": "USA", "postalCode": "90001", "territory": "NA",
    })
    insert("employees", {
        "employeeNumber": employee, "lastName": "Sample", "firstName": "CDC",
        "extension": "x1000", "email": f"cdc-{token}@example.invalid",
        "officeCode": office, "reportsTo": None, "jobTitle": "Sales Rep",
    })
    insert("customers", {
        "customerNumber": customer, "customerName": f"CDC Customer {token[:12]}",
        "contactLastName": "Sample", "contactFirstName": "CDC", "phone": "+1-202-555-0101",
        "addressLine1": "200 Sample Street", "addressLine2": None, "city": "Sample City",
        "state": "CA", "postalCode": "90001", "country": "USA",
        "salesRepEmployeeNumber": employee, "creditLimit": Decimal("50000.00"),
    })
    insert("productlines", {
        "productLine": product_line, "textDescription": "Synthetic CDC sample product line.",
        "htmlDescription": "<p>Synthetic CDC sample.</p>", "image": b"CDC-SAMPLE",
    })
    insert("products", {
        "productCode": product, "productName": f"CDC Model {token[:12]}",
        "productLine": product_line, "productScale": "1:18", "productVendor": "CDC Samples",
        "productDescription": "Synthetic model for replication testing.",
        "quantityInStock": 100, "buyPrice": Decimal("70.00"), "MSRP": price,
    })
    insert("orders", {
        "orderNumber": order, "orderDate": today, "requiredDate": today + timedelta(days=7),
        "shippedDate": None, "status": "In Process", "comments": f"CDC sample {token}",
        "customerNumber": customer,
    })
    insert("orderdetails", {
        "orderNumber": order, "productCode": product, "quantityOrdered": quantity,
        "priceEach": price, "orderLineNumber": 1,
    })
    insert("payments", {
        "customerNumber": customer, "checkNumber": f"CDC-{token}",
        "paymentDate": today, "amount": price * quantity,
    })


def insert_batch(connection, database, groups):
    connection.begin()
    with connection.cursor() as cursor:
        keys = next_ids(cursor, database, groups)
        for offset in range(groups):
            insert_group(
                cursor, database, keys["employees"] + offset,
                keys["customers"] + offset, keys["orders"] + offset,
            )
    connection.commit()


def parse_args(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--host", required=True, help="Writable PRIMARY hostname, not the CDC read replica")
    parser.add_argument("--user", required=True, help="MySQL account with SELECT/INSERT on all eight tables")
    parser.add_argument("--password-env", default="CDC_SAMPLE_PASSWORD")
    parser.add_argument("--groups-per-batch", type=int, default=1, help="One group adds one row per table")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between committed batches")
    parser.add_argument("--batches", type=int, default=0, help="Number of batches; 0 runs until Ctrl+C")
    args = parser.parse_args(arguments)
    if not 1 <= args.groups_per_batch <= 1000:
        parser.error("--groups-per-batch must be between 1 and 1000.")
    if not math.isfinite(args.interval) or args.interval < 0:
        parser.error("--interval must be a finite nonnegative number.")
    if args.batches < 0:
        parser.error("--batches must be nonnegative.")
    return args


def main():
    args = parse_args()
    endpoint = dict(load_config(args.config)["source"])
    endpoint.update(host=args.host, user=args.user, password_env=args.password_env)
    print(
        f"Writing to {endpoint['host']}/{endpoint['database']}: "
        f"{args.groups_per_batch} row(s) per table per batch. Stop with Ctrl+C.",
        flush=True,
    )
    try:
        with closing(connect(endpoint)) as connection:
            prepare(connection, endpoint["database"])
            batch = 0
            while args.batches == 0 or batch < args.batches:
                insert_batch(connection, endpoint["database"], args.groups_per_batch)
                batch += 1
                print(
                    f"Committed batch {batch}: {args.groups_per_batch * len(TABLES)} rows; "
                    f"total {batch * args.groups_per_batch * len(TABLES)} rows.",
                    flush=True,
                )
                if args.batches == 0 or batch < args.batches:
                    time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped. An uncommitted batch is rolled back when the connection closes.")


if __name__ == "__main__":
    main()
