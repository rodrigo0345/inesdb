package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWriteBenchmarkPDF(t *testing.T) {
	tempDir := t.TempDir()
	output := filepath.Join(tempDir, "report.pdf")

	report := benchmarkReport{
		GeneratedAt:   time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC),
		Topology:      "three-node-replication",
		HostOS:        "linux",
		HostArch:      "amd64",
		GoVersion:     "go1.25.1",
		EngineImage:   "omag-benchmark-engine:latest",
		PostgresImage: "postgres:16-alpine",
		SeedRows:      8,
		WarmupOps:     2,
		MeasuredOps:   4,
		Results: []workloadResult{
			{Backend: backendEngine, Workload: workloadReads, Throughput: 123.45, Avg: 10 * time.Millisecond, P50: 9 * time.Millisecond, P95: 12 * time.Millisecond, P99: 15 * time.Millisecond, Min: 8 * time.Millisecond, Max: 16 * time.Millisecond},
			{Backend: backendPostgres, Workload: workloadWrites, Throughput: 234.56, Avg: 8 * time.Millisecond, P50: 7 * time.Millisecond, P95: 10 * time.Millisecond, P99: 11 * time.Millisecond, Min: 6 * time.Millisecond, Max: 13 * time.Millisecond},
		},
		PostgresNote: "analyzer rolled back write/delete probes to keep benchmark tables unchanged",
		PostgresAnalysis: []postgresExplain{
			{Workload: workloadReads, Statement: "SELECT payload FROM bench_postgres_reads_run WHERE id = 'bench_postgres_reads_run_seed_0000'", PlanningTimeMS: 0.123, ExecutionTimeMS: 0.456, NodeDetails: []string{"Index Scan using bench_postgres_reads_run_pkey on bench_postgres_reads_run  (actual time=0.010..0.020 rows=1 loops=1)"}},
			{Workload: workloadWrites, Statement: "INSERT INTO bench_postgres_writes_run (id, category, status, payload) VALUES ('explain_write_0001', 'hot', 'active', 'payload')", PlanningTimeMS: 0.234, ExecutionTimeMS: 0.567, NodeDetails: []string{"Insert on bench_postgres_writes_run  (actual time=0.030..0.040 rows=0 loops=1)"}},
		},
		CPUProfileSeconds: 10,
		CPUProfilePath:    "/tmp/omag-engine.pprof",
		CPUHotspots: []cpuHotspot{
			{Function: "github.com/rodrigo0345/omag/internal/storage/lsm.(*LSMTreeBackend).Get", FlatPercent: 27.5, CumPercent: 42.1, AmdahlMaxSpeedup: 1.38, Amdahl2xSpeedup: 1.16},
		},
	}

	if err := writeBenchmarkPDF(output, report); err != nil {
		t.Fatalf("writeBenchmarkPDF() error = %v", err)
	}

	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read output = %v", err)
	}
	if !strings.HasPrefix(string(data), "%PDF-") {
		t.Fatalf("expected PDF header, got %q", string(data[:8]))
	}
	if len(data) < 200 {
		t.Fatalf("pdf output too small: %d bytes", len(data))
	}
	if !strings.Contains(string(data), "ENGINE CPU HOTSPOTS") {
		t.Fatalf("expected cpu hotspot section in rendered report")
	}
	if !strings.Contains(string(data), "POSTGRES QUERY ANALYZER") {
		t.Fatalf("expected postgres analyzer section in rendered report")
	}
	if !strings.Contains(string(data), "analyzer rolled back write/delete probes") {
		t.Fatalf("expected postgres analyzer note in rendered report")
	}
	if !strings.Contains(string(data), "Topology: three-node-replication") {
		t.Fatalf("expected topology metadata in rendered report")
	}
}

func TestCalculateThroughput(t *testing.T) {
	tests := []struct {
		name    string
		ops     int
		elapsed time.Duration
		want    float64
	}{
		{name: "normal", ops: 10, elapsed: 2 * time.Second, want: 5},
		{name: "zero elapsed", ops: 10, elapsed: 0, want: 0},
		{name: "zero ops", ops: 0, elapsed: time.Second, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateThroughput(tt.ops, tt.elapsed)
			if got != tt.want {
				t.Fatalf("calculateThroughput(%d, %s) = %v, want %v", tt.ops, tt.elapsed, got, tt.want)
			}
		})
	}
}

func TestEvaluatePostgresReplicaReadiness_ExactAppMatch(t *testing.T) {
	expected := map[string]struct{}{"replica-1": {}, "replica-2": {}}
	rows := []postgresReplicationRow{
		{ApplicationName: "replica-1", State: "streaming"},
		{ApplicationName: "replica-2", State: "streaming"},
	}

	ready, exact, streamingCount, matchedNames := evaluatePostgresReplicaReadiness(expected, rows)
	if !ready || !exact {
		t.Fatalf("expected ready+exact, got ready=%v exact=%v", ready, exact)
	}
	if streamingCount != 2 || matchedNames != 2 {
		t.Fatalf("unexpected counts: streaming=%d matched=%d", streamingCount, matchedNames)
	}
}

func TestEvaluatePostgresReplicaReadiness_FallbackByStreamingCount(t *testing.T) {
	expected := map[string]struct{}{"replica-a": {}, "replica-b": {}}
	rows := []postgresReplicationRow{
		{ApplicationName: "walreceiver-1", State: "streaming"},
		{ApplicationName: "walreceiver-2", State: "streaming"},
	}

	ready, exact, streamingCount, matchedNames := evaluatePostgresReplicaReadiness(expected, rows)
	if !ready {
		t.Fatalf("expected fallback readiness, got ready=false")
	}
	if exact {
		t.Fatalf("expected fallback (non-exact) readiness")
	}
	if streamingCount != 2 || matchedNames != 0 {
		t.Fatalf("unexpected counts: streaming=%d matched=%d", streamingCount, matchedNames)
	}
}

func TestEvaluatePostgresReplicaReadiness_NotReadyWhenNotStreaming(t *testing.T) {
	expected := map[string]struct{}{"replica-1": {}, "replica-2": {}}
	rows := []postgresReplicationRow{
		{ApplicationName: "replica-1", State: "startup"},
		{ApplicationName: "replica-2", State: "catchup"},
	}

	ready, exact, streamingCount, matchedNames := evaluatePostgresReplicaReadiness(expected, rows)
	if ready || exact {
		t.Fatalf("expected not ready, got ready=%v exact=%v", ready, exact)
	}
	if streamingCount != 0 || matchedNames != 0 {
		t.Fatalf("unexpected counts: streaming=%d matched=%d", streamingCount, matchedNames)
	}
}

func TestFormatPostgresReplicationRows(t *testing.T) {
	rows := []postgresReplicationRow{
		{ApplicationName: "", State: "streaming"},
		{ApplicationName: "replica-2", State: ""},
	}
	got := formatPostgresReplicationRows(rows)
	want := "<empty>:streaming, replica-2:<empty>"
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected format: got=%q want=%q", got, want)
	}

	if got := formatPostgresReplicationRows(nil); got != "none" {
		t.Fatalf("unexpected empty format: %q", got)
	}
}
