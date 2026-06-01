#!/usr/bin/env bash
set -euo pipefail

# Demonstrates all vector-search capabilities of omag-db, including:
#   - ENGINE = VECTOR with HNSW approximate k-NN search
#   - UNIQUE secondary indexes on non-vector columns (maintained alongside HNSW)
#   - Nullable vectors, upsert, delete, transactional rollback
#
# Uses a product-recommendation scenario: 4-dimensional embeddings where
# each dimension loosely represents affinity to (electronics, clothing, food, sports).
#
# Usage:
#   ./bootstrap_vector_demo.sh
#   PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGDATABASE=postgres ./bootstrap_vector_demo.sh

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"

PSQL=(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1)

echo "[vector-demo] target: $PGUSER@$PGHOST:$PGPORT/$PGDATABASE"
echo ""

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
for tbl in user_profiles products; do
  "${PSQL[@]}" -c "DROP TABLE $tbl;" >/dev/null 2>&1 || true
done

# ---------------------------------------------------------------------------
# 1. Schema
# ---------------------------------------------------------------------------
echo "=== 1. Creating tables ==="

"${PSQL[@]}" <<'SQL'

-- HNSW-backed table: O(log n) approximate nearest-neighbour search.
--
-- ENGINE = VECTOR builds and maintains an HNSW index on the first VECTOR
-- column.  Any additional UNIQUE constraints get their own LSM-backed
-- secondary index, maintained automatically on every write alongside HNSW.
--
-- On each INSERT/UPDATE the engine writes to:
--   • HNSW graph  (vector similarity search)
--   • PRIMARY LSM (row storage keyed by id)
--   • idx_UNIQUE_sku LSM (secondary index: sku_bytes → primary_key)
--
-- On DELETE the secondary index entry is removed using the row's before-image.
CREATE TABLE products (
  id        INT  NOT NULL,
  sku       TEXT UNIQUE,        -- secondary LSM index enforcing uniqueness
  name      TEXT,
  category  TEXT,
  embedding VECTOR(4),
  PRIMARY KEY (id)
) ENGINE = VECTOR;

-- Standard LSM-backed table that also has a VECTOR column.
-- vec_dist() still works here via a full-scan sort (no HNSW acceleration).
CREATE TABLE user_profiles (
  user_id        INT  NOT NULL,
  username       TEXT UNIQUE,   -- secondary LSM index
  preference_vec VECTOR(4),
  PRIMARY KEY (user_id)
);

SQL

echo "  products      -> ENGINE=VECTOR  (HNSW + LSM secondary index on sku)"
echo "  user_profiles -> ENGINE=LSM     (full-scan vec_dist + LSM secondary index on username)"
echo ""

# ---------------------------------------------------------------------------
# 2. Seed products (4D embedding: [electronics, clothing, food, sports])
# ---------------------------------------------------------------------------
echo "=== 2. Inserting product embeddings ==="

"${PSQL[@]}" <<'SQL'

INSERT INTO products (id, sku, name, category, embedding) VALUES
  (1,  'ELEC-001', 'Laptop Pro',          'electronics', '[1.0, 0.0, 0.0, 0.0]');
INSERT INTO products (id, sku, name, category, embedding) VALUES
  (2,  'ELEC-002', 'Wireless Headphones', 'electronics', '[0.9, 0.1, 0.0, 0.0]');
INSERT INTO products (id, sku, name, category, embedding) VALUES
  (3,  'ELEC-003', 'Smart Watch',         'electronics', '[0.8, 0.0, 0.2, 0.0]');
INSERT INTO products (id, sku, name, category, embedding) VALUES
  (4,  'SPRT-001', 'Running Shoes',       'sports',      '[0.0, 0.0, 0.0, 1.0]');
INSERT INTO products (id, sku, name, category, embedding) VALUES
  (5,  'SPRT-002', 'Yoga Mat',            'sports',      '[0.0, 0.1, 0.0, 0.9]');
INSERT INTO products (id, sku, name, category, embedding) VALUES
  (6,  'CLTH-001', 'Denim Jacket',        'clothing',    '[0.0, 1.0, 0.0, 0.0]');
INSERT INTO products (id, sku, name, category, embedding) VALUES
  (7,  'CLTH-002', 'Cotton T-Shirt',      'clothing',    '[0.1, 0.9, 0.0, 0.0]');
INSERT INTO products (id, sku, name, category, embedding) VALUES
  (8,  'FOOD-001', 'Organic Coffee',      'food',        '[0.0, 0.0, 1.0, 0.0]');
INSERT INTO products (id, sku, name, category, embedding) VALUES
  (9,  'FOOD-002', 'Protein Bar',         'food',        '[0.0, 0.0, 0.8, 0.2]');

-- Row with no embedding: VECTOR column is nullable.
-- Stored in primary and secondary (sku) indexes; skipped by HNSW.
INSERT INTO products (id, sku, name, category) VALUES
  (99, 'UNKN-001', 'Mystery Item', 'unknown');

SQL

echo "  9 products with embeddings + 1 with NULL embedding"
echo ""

# ---------------------------------------------------------------------------
# 3. Seed user profiles (LSM engine, VECTOR column + UNIQUE username)
# ---------------------------------------------------------------------------
echo "=== 3. Inserting user preference vectors (LSM engine) ==="

"${PSQL[@]}" <<'SQL'

INSERT INTO user_profiles (user_id, username, preference_vec) VALUES
  (1, 'alice',   '[0.9, 0.0, 0.0, 0.1]');
INSERT INTO user_profiles (user_id, username, preference_vec) VALUES
  (2, 'bob',     '[0.0, 0.0, 0.1, 0.9]');
INSERT INTO user_profiles (user_id, username, preference_vec) VALUES
  (3, 'charlie', '[0.1, 0.8, 0.0, 0.1]');

SQL

echo "  3 user profiles"
echo ""

# ---------------------------------------------------------------------------
# 4. Secondary index: UNIQUE constraint enforcement
#
# Each write to products simultaneously updates:
#   (a) the HNSW graph for the embedding column
#   (b) the LSM secondary index for the sku column (sku_bytes → primary_key)
#
# The secondary index enforces UNIQUE across both new inserts and updates.
# ---------------------------------------------------------------------------
echo "=== 4. Secondary index: UNIQUE constraint enforcement on sku ==="
echo ""

echo "--- Attempting to insert duplicate SKU 'ELEC-001' (must be rejected) ---"
(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
  -c "INSERT INTO products (id, sku, name, category, embedding) VALUES (10, 'ELEC-001', 'Laptop Clone', 'electronics', '[0.99, 0.01, 0.0, 0.0]');" \
  2>&1 && echo "  [ERROR: should have failed]") \
  || echo "  [OK: duplicate key violation caught as expected]"

echo ""
echo "--- Attempting UPDATE to steal an existing SKU (must be rejected) ---"
(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
  -c "UPDATE products SET sku = 'ELEC-001' WHERE id = 2;" \
  2>&1 && echo "  [ERROR: should have failed]") \
  || echo "  [OK: duplicate key violation on UPDATE caught as expected]"

echo ""
echo "--- Delete id=3 (SKU 'ELEC-003') then re-insert with the same SKU (must succeed) ---"
echo "    Secondary index entry is purged on delete using the row before-image."
"${PSQL[@]}" -c "DELETE FROM products WHERE id = 3;"
"${PSQL[@]}" -c "INSERT INTO products (id, sku, name, category, embedding) VALUES (3, 'ELEC-003', 'Smart Watch Gen 2', 'electronics', '[0.85, 0.05, 0.1, 0.0]');"
echo "  [OK: SKU reused after delete]"

echo ""

# ---------------------------------------------------------------------------
# 5. k-NN search — HNSW fast path
# ---------------------------------------------------------------------------
echo "=== 5. k-NN search via HNSW (ENGINE = VECTOR) ==="
echo ""

echo "--- Top-3 products for an electronics shopper [0.9, 0.1, 0.0, 0.0] ---"
"${PSQL[@]}" -c "SELECT id, sku, name, category FROM products ORDER BY vec_dist(embedding, '[0.9, 0.1, 0.0, 0.0]') ASC LIMIT 3;"

echo ""
echo "--- Top-3 products for a sports shopper [0.0, 0.1, 0.0, 0.9] ---"
"${PSQL[@]}" -c "SELECT id, sku, name, category FROM products ORDER BY vec_dist(embedding, '[0.0, 0.1, 0.0, 0.9]') ASC LIMIT 3;"

echo ""
echo "--- Top-3 products for a food shopper [0.0, 0.0, 0.9, 0.1] ---"
"${PSQL[@]}" -c "SELECT id, sku, name, category FROM products ORDER BY vec_dist(embedding, '[0.0, 0.0, 0.9, 0.1]') ASC LIMIT 3;"

echo ""
echo "--- Exact match: query identical to id=1 embedding (distance = 0) ---"
"${PSQL[@]}" -c "SELECT id, sku, name FROM products ORDER BY vec_dist(embedding, '[1.0, 0.0, 0.0, 0.0]') ASC LIMIT 1;"

echo ""

# ---------------------------------------------------------------------------
# 6. k-NN search — LSM full-scan fallback
# ---------------------------------------------------------------------------
echo "=== 6. k-NN search via full-scan (ENGINE = LSM, no HNSW) ==="
echo ""

echo "--- Users closest to preference [0.5, 0.5, 0.0, 0.0] ---"
"${PSQL[@]}" -c "SELECT user_id, username FROM user_profiles ORDER BY vec_dist(preference_vec, '[0.5, 0.5, 0.0, 0.0]') ASC LIMIT 2;"

echo ""

# ---------------------------------------------------------------------------
# 7. NULL vector handling
# ---------------------------------------------------------------------------
echo "=== 7. NULL vector handling ==="
echo ""

echo "--- The mystery item (NULL embedding) is still selectable ---"
"${PSQL[@]}" -c "SELECT id, sku, name, embedding FROM products WHERE id = 99;"

echo ""
echo "--- NULL rows are invisible to k-NN (HNSW skips them) ---"
echo "    Top-10 search returns only the 9 rows that have embeddings:"
"${PSQL[@]}" -c "SELECT id, sku, name FROM products ORDER BY vec_dist(embedding, '[0.5, 0.5, 0.0, 0.0]') ASC LIMIT 10;"

echo ""

# ---------------------------------------------------------------------------
# 8. Vector upsert (re-insert updates stored vector + HNSW node)
# ---------------------------------------------------------------------------
echo "=== 8. Vector upsert — re-inserting an existing key updates the embedding ==="
echo ""

echo "--- Before update: nearest to [0.95, 0.05, 0.0, 0.0] ---"
"${PSQL[@]}" -c "SELECT id, sku, name FROM products ORDER BY vec_dist(embedding, '[0.95, 0.05, 0.0, 0.0]') ASC LIMIT 1;"

"${PSQL[@]}" -c "INSERT INTO products (id, sku, name, category, embedding) VALUES (1, 'ELEC-001', 'Laptop Pro X', 'electronics', '[0.95, 0.05, 0.0, 0.0]');"

echo "--- After update (id=1 vector shifted): still closest ---"
"${PSQL[@]}" -c "SELECT id, sku, name FROM products ORDER BY vec_dist(embedding, '[0.95, 0.05, 0.0, 0.0]') ASC LIMIT 1;"

echo ""

# ---------------------------------------------------------------------------
# 9. Delete removes the row from primary, HNSW index, and secondary indexes
# ---------------------------------------------------------------------------
echo "=== 9. Delete — row removed from primary storage, HNSW, and secondary indexes ==="
echo ""

echo "--- Before delete: Wireless Headphones (id=2, sku=ELEC-002) in top results ---"
"${PSQL[@]}" -c "SELECT id, sku, name FROM products ORDER BY vec_dist(embedding, '[0.9, 0.1, 0.0, 0.0]') ASC LIMIT 3;"

"${PSQL[@]}" -c "DELETE FROM products WHERE id = 2;"

echo "--- After delete: id=2 gone from k-NN results; sku ELEC-002 now reusable ---"
"${PSQL[@]}" -c "SELECT id, sku, name FROM products ORDER BY vec_dist(embedding, '[0.9, 0.1, 0.0, 0.0]') ASC LIMIT 3;"

"${PSQL[@]}" -c "INSERT INTO products (id, sku, name, category, embedding) VALUES (2, 'ELEC-002', 'Wireless Headphones V2', 'electronics', '[0.88, 0.12, 0.0, 0.0]');"
echo "  [OK: sku ELEC-002 reused after delete]"

echo ""

# ---------------------------------------------------------------------------
# 10. Transaction rollback
# ---------------------------------------------------------------------------
echo "=== 10. Transaction rollback — all indexes (HNSW + secondary) roll back together ==="
echo ""

"${PSQL[@]}" <<'SQL'
BEGIN;
INSERT INTO products (id, sku, name, category, embedding) VALUES (50, 'TEMP-001', 'Ghost Product', 'misc', '[0.0, 0.0, 0.0, 1.0]');
ROLLBACK;
SQL

echo "--- id=50 / sku TEMP-001 should not exist after ROLLBACK ---"
"${PSQL[@]}" -c "SELECT id, sku FROM products WHERE id = 50;"

echo ""
echo "--- sku TEMP-001 also gone from secondary index: re-insert must succeed ---"
"${PSQL[@]}" -c "INSERT INTO products (id, sku, name, category, embedding) VALUES (50, 'TEMP-001', 'Confirmed Rollback', 'misc', '[0.0, 0.0, 0.0, 1.0]');"
"${PSQL[@]}" -c "DELETE FROM products WHERE id = 50;"
echo "  [OK: rollback cleaned up both HNSW and secondary index]"

echo ""

# ---------------------------------------------------------------------------
# 11. Full table scan
# ---------------------------------------------------------------------------
echo "=== 11. Full scan: all products with SKU and stored embeddings ==="
echo ""
"${PSQL[@]}" -c "SELECT id, sku, name, category, embedding FROM products ORDER BY id;"

echo ""

# ---------------------------------------------------------------------------
# 12. ANALYZE
# ---------------------------------------------------------------------------
echo "=== 12. ANALYZE — collect column statistics ==="
echo ""
"${PSQL[@]}" -c "ANALYZE products;"

echo ""
echo "[vector-demo] done"
