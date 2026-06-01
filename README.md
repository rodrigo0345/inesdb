# InesDB — Indexed Node Engine Storage

## Overview

InesDB is a sophisticated on-disk database engine written in Go. It focuses on **row-major, interchangeable storage backends** (B+ Tree and LSM Tree), full **ACID transaction support**, and built-in **vector similarity search** powered by an HNSW index. The server speaks the PostgreSQL wire protocol, so any `psql` client or PostgreSQL driver works out of the box.

## Key Features

- **Row-Based Storage** — structured data with slotted pages
- **Interchangeable Backends** — B+ Tree and LSM Tree access methods
- **Vector Search** — `VECTOR(n)` column type with `vec_dist()` k-NN queries; HNSW index for O(log n) approximate search
- **ACID Transactions** — full transaction support with SERIALIZABLE and READ COMMITTED isolation
- **Advanced Concurrency** — MVCC, OCC, and Two-Phase Locking
- **Smart Caching** — multiple replacement policies with Write-Ahead Logging
- **PostgreSQL Wire Protocol** — connect with `psql`, Python `psycopg2`, Go `pgx`, and more

## Quick Start

```bash
# Build and start the server (listens on :5432 by default)
go run main.go

# In another terminal, connect with psql
psql -h 127.0.0.1 -p 5432 -U postgres
```

Available flags:

| Flag | Default | Description |
|------|---------|-------------|
| `-listen` | `:5432` | TCP address for the PostgreSQL wire server |
| `-db` | `./omag.db` | Database file path |
| `-lsm-data-dir` | `./lsm_data` | LSM data directory |
| `-wal` | `./omag.wal` | Write-Ahead Log file path |
| `-debug` | `false` | Enable debug logging |
| `-pprof-listen` | — | Optional pprof profiling endpoint (e.g. `:6060`) |

---

## Vector Search

InesDB ships a full vector similarity engine. Declare a `VECTOR(n)` column, choose `ENGINE = VECTOR`, and the database automatically builds and maintains an HNSW index for fast approximate k-nearest-neighbour queries.

### Create a vector table

```sql
CREATE TABLE products (
  id        INT  NOT NULL,
  name      TEXT,
  category  TEXT,
  embedding VECTOR(4),      -- 4-dimensional float32 vector
  PRIMARY KEY (id)
) ENGINE = VECTOR;           -- activates HNSW indexing
```

`VECTOR(n)` columns also work on `ENGINE = LSM` or `ENGINE = BTREE` tables — queries fall back to a full-scan sort instead of HNSW.

### Insert vectors

```sql
INSERT INTO products (id, name, category, embedding) VALUES
  (1, 'Laptop Pro',          'electronics', '[1.0, 0.0, 0.0, 0.0]'),
  (2, 'Wireless Headphones', 'electronics', '[0.9, 0.1, 0.0, 0.0]'),
  (3, 'Running Shoes',       'sports',      '[0.0, 0.0, 0.0, 1.0]');

-- VECTOR columns are nullable; omit the column to store NULL
INSERT INTO products (id, name, category) VALUES (99, 'Mystery Item', 'unknown');
```

### k-NN similarity search

Use `vec_dist(column, '[f1, f2, ...]')` in an `ORDER BY ... ASC LIMIT k` clause:

```sql
-- Top-3 products closest to an "electronics shopper" query vector
SELECT id, name, category
FROM products
ORDER BY vec_dist(embedding, '[0.9, 0.1, 0.0, 0.0]') ASC
LIMIT 3;

-- Find the single most similar item (distance = 0 means exact match)
SELECT id, name
FROM products
ORDER BY vec_dist(embedding, '[1.0, 0.0, 0.0, 0.0]') ASC
LIMIT 1;
```

### Distance metrics

| Metric | Description | Use case |
|--------|-------------|----------|
| **L2 (default)** | Squared Euclidean distance; `sqrt` applied to final results | General-purpose similarity |
| **Cosine** | `1 − cos(a, b)`, range [0, 2]; 0 = identical direction | Normalised text / image embeddings |
| **Dot product** | `−dot(a, b)`; minimising = maximising inner product | Maximum inner-product search |

The metric is configured per-table at engine initialisation via `HNSWConfig`.

### Secondary indexes on non-vector columns

A `VECTOR`-engine table can carry standard `UNIQUE` constraints on any non-vector column. The engine maintains **all indexes simultaneously** on every write:

| Index | Stored in | Purpose |
|-------|-----------|---------|
| `PRIMARY` (keyed by PK) | VectorEngine in-memory store | Full row storage |
| HNSW (on `VECTOR` column) | In-memory graph + `hnsw.bin` | Approximate k-NN search |
| `UNIQUE_sku` (on `sku TEXT UNIQUE`) | LSM Tree (`idx_UNIQUE_sku/`) | Uniqueness enforcement |

```sql
CREATE TABLE products (
  id        INT  NOT NULL,
  sku       TEXT UNIQUE,        -- secondary LSM index, enforced on every write
  name      TEXT,
  category  TEXT,
  embedding VECTOR(4),
  PRIMARY KEY (id)
) ENGINE = VECTOR;

-- Duplicate SKU is rejected even though this is a VECTOR table
INSERT INTO products (id, sku, name, category, embedding)
  VALUES (1, 'ELEC-001', 'Laptop Pro', 'electronics', '[1.0, 0.0, 0.0, 0.0]');
INSERT INTO products (id, sku, name, category, embedding)
  VALUES (2, 'ELEC-001', 'Laptop Clone', 'electronics', '[0.9, 0.0, 0.0, 0.0]');
-- ERROR: duplicate key value violates unique constraint on column "sku"

-- After deleting a row, its SKU is freed in the secondary index and can be reused
DELETE FROM products WHERE id = 1;
INSERT INTO products (id, sku, name, category, embedding)
  VALUES (3, 'ELEC-001', 'Laptop Pro Gen 2', 'electronics', '[0.95, 0.05, 0.0, 0.0]');
-- OK: SKU recycled because the secondary index entry was removed on DELETE
```

On `DELETE`, the engine uses the row's **before-image** to locate and remove the correct secondary index entry — the stale `sku_bytes → primary_key` mapping is deleted from LSM before the primary row is removed, keeping all indexes consistent.

### HNSW index behaviour

- Built automatically when a `VECTOR(n)` column is present on a `VECTOR`-engine table.
- Nodes with `NULL` vectors are skipped during indexing but remain queryable via `SELECT`.
- Deletions use soft-marking; the graph is automatically rebuilt when the deletion ratio exceeds 20%.
- The graph and all vectors are persisted to `hnsw.bin` and `vectors.bin` on `Close()` and reloaded on restart — vector search state survives server restarts.

### Dimension validation

Querying with a vector of the wrong dimensionality returns an error:

```sql
-- Table has VECTOR(3); querying with VECTOR(2) → error
SELECT id FROM embeddings ORDER BY vec_dist(vec, '[1.0, 2.0]') ASC;
-- ERROR: dimension mismatch
```

---

## SQL Reference

### Data types

| SQL type(s) | Notes |
|-------------|-------|
| `INT`, `INTEGER`, `INT4`, `SERIAL` | 32-bit integer; `SERIAL` auto-increments |
| `BIGINT`, `INT8`, `BIGSERIAL` | 64-bit integer; `BIGSERIAL` auto-increments |
| `FLOAT8`, `DOUBLE PRECISION`, `NUMERIC`, `DECIMAL` | 64-bit float |
| `BOOL`, `BOOLEAN` | True / false |
| `TEXT`, `VARCHAR`, `CHAR` | UTF-8 string |
| `UUID` | Stored as string; auto-generated on insert if omitted |
| `VECTOR(n)` | n-dimensional `float32` vector |

### Supported statements

```sql
-- DDL
CREATE TABLE t (id INT PRIMARY KEY, val TEXT) [ENGINE = VECTOR|BTREE|LSM];
CREATE TABLE t (...) IF NOT EXISTS;
DROP TABLE t [IF EXISTS];
ANALYZE t;

-- DML
INSERT INTO t (col1, col2) VALUES (...), (...);
SELECT col1, col2 FROM t [WHERE ...] [ORDER BY ... ASC|DESC] [LIMIT n] [OFFSET m];
UPDATE t SET col = val [WHERE ...];
DELETE FROM t [WHERE ...];

-- Joins
SELECT ... FROM a INNER JOIN b ON a.id = b.a_id [WHERE ...];
SELECT ... FROM a LEFT JOIN b ON a.id = b.a_id [WHERE ...];

-- Transactions
BEGIN;
COMMIT;
ROLLBACK;

-- Vector similarity search
SELECT ... FROM t ORDER BY vec_dist(vec_col, '[f1, f2, ...]') ASC LIMIT k;
```

### Column constraints

`NOT NULL` · `UNIQUE` · `PRIMARY KEY` · `AUTO_INCREMENT` / `SERIAL` / `BIGSERIAL` · `REFERENCES table(col) [ON DELETE CASCADE|RESTRICT|NO ACTION]` · `FOREIGN KEY (cols) REFERENCES table(cols)`

### Storage engines

| `ENGINE =` | Index | Best for |
|------------|-------|----------|
| `LSM` (default) | LSM Tree | Write-heavy workloads |
| `BTREE` | B+ Tree | Read-heavy / range scans |
| `VECTOR` | HNSW + in-memory store | Vector similarity search |

---

## Demo Scripts

| Script | What it shows |
|--------|---------------|
| `bootstrap_banking_demo.sh` | Relational schema with customers, accounts, transfers, and loans |
| `bootstrap_vector_demo.sh` | Full vector-search showcase: HNSW k-NN, nullable vectors, upsert, delete, transactions, LSM fallback |

Run either script against a running server:

```bash
go run main.go &           # start the server
./bootstrap_vector_demo.sh # run the vector demo
```

---

## Documentation

For architecture details and internals see the [`docs/`](docs/index.md) directory.
