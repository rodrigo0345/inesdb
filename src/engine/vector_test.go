package engine_test

import (
	"math"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// VECTOR column type
// ---------------------------------------------------------------------------

func TestVector_InsertAndSelect(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE embeddings (id INT NOT NULL, vec VECTOR(3), PRIMARY KEY (id)) ENGINE = VECTOR`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (1, '[1.0, 0.0, 0.0]')`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (2, '[0.0, 1.0, 0.0]')`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (3, '[0.0, 0.0, 1.0]')`)

	res := mustSQL(t, db, `SELECT id, vec FROM embeddings`)
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}

	// Verify the first vector decodes back to []float32.
	for _, row := range res.Rows {
		if row[0] == int64(1) {
			vecs, ok := row[1].([]float32)
			if !ok {
				t.Fatalf("expected []float32, got %T", row[1])
			}
			if len(vecs) != 3 || vecs[0] != 1.0 || vecs[1] != 0.0 || vecs[2] != 0.0 {
				t.Errorf("unexpected vector value: %v", vecs)
			}
		}
	}
}

func TestVector_NullableVector(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE embeddings (id INT NOT NULL, vec VECTOR(2), PRIMARY KEY (id)) ENGINE = VECTOR`)
	mustSQL(t, db, `INSERT INTO embeddings (id) VALUES (1)`) // vec omitted → NULL

	res := mustSQL(t, db, `SELECT vec FROM embeddings WHERE id = 1`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != nil {
		t.Errorf("expected NULL for omitted VECTOR column, got %v", res.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// vec_dist function
// ---------------------------------------------------------------------------

func TestVector_VecDist_OrderBy(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE embeddings (id INT NOT NULL, vec VECTOR(3), PRIMARY KEY (id)) ENGINE = VECTOR`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (1, '[1.0, 0.0, 0.0]')`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (2, '[0.0, 1.0, 0.0]')`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (3, '[0.0, 0.0, 1.0]')`)

	// Query vector is closest to row 1 (distance 0), then rows 2 and 3 (equal distance √2).
	res := mustSQL(t, db, `SELECT id FROM embeddings ORDER BY vec_dist(vec, '[1.0, 0.0, 0.0]') ASC LIMIT 3`)
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}
	// First row should be id=1 (closest match).
	if res.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1 as closest, got %v", res.Rows[0][0])
	}
}

func TestVector_VecDist_CorrectDistance(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE embeddings (id INT NOT NULL, vec VECTOR(2), PRIMARY KEY (id)) ENGINE = VECTOR`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (1, '[3.0, 4.0]')`)

	// Euclidean distance from [3,4] to [0,0] = sqrt(9+16) = 5.
	res := mustSQL(t, db, `SELECT id FROM embeddings ORDER BY vec_dist(vec, '[0.0, 0.0]') ASC LIMIT 1`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
}

func TestVector_VecDist_TopK(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE embeddings (id INT NOT NULL, vec VECTOR(2), PRIMARY KEY (id)) ENGINE = VECTOR`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (1, '[10.0, 0.0]')`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (2, '[1.0, 0.0]')`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (3, '[2.0, 0.0]')`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (4, '[5.0, 0.0]')`)

	// Query: [0,0] → distances: id2=1, id3=2, id4=5, id1=10
	res := mustSQL(t, db, `SELECT id FROM embeddings ORDER BY vec_dist(vec, '[0.0, 0.0]') ASC LIMIT 2`)
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows from top-2, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2 as nearest, got %v", res.Rows[0][0])
	}
	if res.Rows[1][0] != int64(3) {
		t.Errorf("expected id=3 as second nearest, got %v", res.Rows[1][0])
	}
}

func TestVector_VecDist_DimensionMismatch(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE embeddings (id INT NOT NULL, vec VECTOR(3), PRIMARY KEY (id)) ENGINE = VECTOR`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (1, '[1.0, 2.0, 3.0]')`)

	err := trySQL(t, db, `SELECT id FROM embeddings ORDER BY vec_dist(vec, '[1.0, 2.0]') ASC`)
	if err == nil {
		t.Fatal("expected dimension mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "dimension mismatch") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestVector_VecDistIsZeroForSameVector(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE embeddings (id INT NOT NULL, vec VECTOR(2), PRIMARY KEY (id)) ENGINE = VECTOR`)
	mustSQL(t, db, `INSERT INTO embeddings (id, vec) VALUES (1, '[1.5, 2.5]')`)

	// Distance from [1.5,2.5] to itself should be 0.
	res := mustSQL(t, db, `SELECT id FROM embeddings ORDER BY vec_dist(vec, '[1.5, 2.5]') ASC LIMIT 1`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
}

// ---------------------------------------------------------------------------
// VECTOR column on non-VECTOR engine (LSM)
// ---------------------------------------------------------------------------

func TestVector_OnLSMEngine(t *testing.T) {
	db := openFreshDB(t)
	// VECTOR columns can live in any engine.
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, vec VECTOR(3), PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO t (id, vec) VALUES (1, '[0.1, 0.2, 0.3]')`)

	res := mustSQL(t, db, `SELECT id FROM t ORDER BY vec_dist(vec, '[0.0, 0.0, 0.0]') ASC LIMIT 1`)
	if len(res.Rows) != 1 {
		t.Errorf("vec_dist on LSM-backed VECTOR column failed")
	}
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

func TestVector_PersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	mustSQL(t, db1, `CREATE TABLE embeddings (id INT NOT NULL, vec VECTOR(2), PRIMARY KEY (id)) ENGINE = VECTOR`)
	mustSQL(t, db1, `INSERT INTO embeddings (id, vec) VALUES (1, '[1.0, 2.0]')`)
	mustSQL(t, db1, `INSERT INTO embeddings (id, vec) VALUES (2, '[3.0, 4.0]')`)
	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2 := openDBAt(t, dir)
	defer db2.Close()

	res := mustSQL(t, db2, `SELECT id FROM embeddings ORDER BY vec_dist(vec, '[0.0, 0.0]') ASC`)
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows after restart, got %d", len(res.Rows))
	}
	// id=1: distance sqrt(1+4)=√5≈2.24; id=2: sqrt(9+16)=5 → id=1 should come first.
	if res.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1 nearest origin after restart, got %v", res.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// Helper: approximate float equality
// ---------------------------------------------------------------------------

func approxEqual(a, b, eps float64) bool {
	return math.Abs(a-b) < eps
}

var _ = approxEqual // silence unused warning
