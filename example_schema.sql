-- Run on the source server with an administrator account.
-- These are example names. Review and adapt before executing.
CREATE DATABASE IF NOT EXISTS source_db;
CREATE TABLE source_db.customers (
    id BIGINT NOT NULL,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Run the following statements on the TARGET server, not the source.
CREATE DATABASE IF NOT EXISTS replica_db;
CREATE TABLE replica_db.customers_copy (
    id BIGINT NOT NULL,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE replica_db.customers_projection (
    customer_id BIGINT NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    email_address VARCHAR(255) NULL,
    origin VARCHAR(20) NOT NULL DEFAULT 'blue',
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
