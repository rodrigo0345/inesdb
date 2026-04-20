package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultEngineImage   = "omag-benchmark-engine:latest"
	defaultPostgresImage = "postgres:16-alpine"
	defaultReportPath    = "docs/papers/engine-vs-postgres-benchmark.pdf"
	defaultSeedRows      = 256
	defaultWarmupOps     = 32
	defaultMeasuredOps   = 128
	defaultTopology      = "single-node"
	threeNodeTopology    = "three-node-replication"
)

type backendName string

type workloadName string

const (
	backendEngine   backendName = "engine"
	backendPostgres backendName = "postgres"

	workloadReads  workloadName = "reads"
	workloadWhere  workloadName = "where-clauses"
	workloadWrites workloadName = "writes"
	workloadDelete workloadName = "deletes"
	workloadMixed  workloadName = "mixed"
)

type benchStep struct {
	sql     string
	isQuery bool
}

type workloadResult struct {
	Backend    backendName
	Workload   workloadName
	Warmup     int
	Measured   int
	Total      time.Duration
	Throughput float64
	Avg        time.Duration
	P50        time.Duration
	P95        time.Duration
	P99        time.Duration
	Min        time.Duration
	Max        time.Duration
}

type benchmarkReport struct {
	GeneratedAt       time.Time
	Topology          string
	HostOS            string
	HostArch          string
	GoVersion         string
	EngineImage       string
	PostgresImage     string
	SeedRows          int
	WarmupOps         int
	MeasuredOps       int
	Results           []workloadResult
	PostgresAnalysis  []postgresExplain
	PostgresNote      string
	CPUProfileSeconds int
	CPUProfilePath    string
	CPUProfileNote    string
	CPUHotspots       []cpuHotspot
}

type cpuHotspot struct {
	Function         string
	FlatPercent      float64
	CumPercent       float64
	AmdahlMaxSpeedup float64
	Amdahl2xSpeedup  float64
	Nested           []cpuNestedHotspot
}

type cpuNestedHotspot struct {
	Function    string
	FlatPercent float64
	CumPercent  float64
}

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type postgresExplain struct {
	Workload        workloadName
	Statement       string
	PlanningTimeMS  float64
	ExecutionTimeMS float64
	NodeDetails     []string
}

func main() {
	var (
		engineImage   = flag.String("engine-image", defaultEngineImage, "engine container image to benchmark")
		postgresImage = flag.String("postgres-image", defaultPostgresImage, "postgres container image to benchmark")
		topology      = flag.String("topology", defaultTopology, "benchmark topology (single-node|three-node-replication)")
		seedRows      = flag.Int("seed-rows", defaultSeedRows, "number of seed rows to load into each table")
		warmupOps     = flag.Int("warmup-ops", defaultWarmupOps, "warmup operations to run before timing")
		measuredOps   = flag.Int("ops", defaultMeasuredOps, "measured operations per workload")
		reportPath    = flag.String("output", defaultReportPath, "path to write the final PDF report")
		iterations    = flag.Int("iterations", 2, "number of benchmark iterations to run for test-improve loops")
		maxConns      = flag.Int("max-conns", 8, "max open/idle connections for benchmark clients")
	)
	flag.Parse()

	if *seedRows <= 0 || *warmupOps < 0 || *measuredOps <= 0 || *iterations <= 0 || *maxConns <= 0 {
		fmt.Fprintln(os.Stderr, "invalid benchmark arguments")
		os.Exit(2)
	}
	if *topology != defaultTopology && *topology != threeNodeTopology {
		fmt.Fprintln(os.Stderr, "invalid topology, expected single-node or three-node-replication")
		os.Exit(2)
	}

	root, err := findRepoRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "resolve repository root:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()

	engineRuntime, postgresRuntime, cleanup, err := startContainers(ctx, root, *engineImage, *postgresImage, *topology, *maxConns)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start containers:", err)
		os.Exit(1)
	}
	defer cleanup()

	report, err := runBenchmarksIterative(ctx, engineRuntime, postgresRuntime, *seedRows, *warmupOps, *measuredOps, *iterations, *engineImage, *postgresImage)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run benchmark:", err)
		os.Exit(1)
	}

	hotspots, profilePath, profileSeconds, profileNote := collectEngineCPUHotspots(ctx, engineRuntime, *seedRows, *warmupOps, *measuredOps)
	report.CPUHotspots = hotspots
	report.CPUProfilePath = profilePath
	report.CPUProfileSeconds = profileSeconds
	report.CPUProfileNote = profileNote
	printEngineCPUHotspotTiming(hotspots, profileSeconds)

	analysis, explainErr := collectPostgresExplain(ctx, postgresRuntime.conn, *seedRows)
	if explainErr != nil {
		fmt.Fprintf(os.Stderr, "postgres query analyzer warning: %v\n", explainErr)
		report.PostgresNote = explainErr.Error()
	} else {
		printPostgresExplain(analysis)
	}
	report.PostgresAnalysis = analysis

	printConnectionStats("engine", engineRuntime.conn)
	printConnectionStats("postgres", postgresRuntime.conn)

	report.GeneratedAt = time.Now().UTC()
	report.Topology = *topology
	report.HostOS = runtime.GOOS
	report.HostArch = runtime.GOARCH
	report.GoVersion = runtime.Version()

	if err := os.MkdirAll(filepath.Dir(*reportPath), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "create report directory:", err)
		os.Exit(1)
	}
	if err := writeBenchmarkPDF(*reportPath, report); err != nil {
		fmt.Fprintln(os.Stderr, "write pdf:", err)
		os.Exit(1)
	}

	fmt.Printf("Benchmark report written to %s\n", *reportPath)
	printSummary(report)
}

type runtimeDB struct {
	name          string
	dsn           string
	conn          *sql.DB
	cleanup       func() error
	endpoint      string
	pprofEndpoint string
}

func startContainers(ctx context.Context, repoRoot, engineImage, postgresImage, topology string, maxConns int) (*runtimeDB, *runtimeDB, func(), error) {
	switch topology {
	case defaultTopology:
		return startSingleNodeContainers(ctx, repoRoot, engineImage, postgresImage, maxConns)
	case threeNodeTopology:
		return startThreeNodeReplicationContainers(ctx, repoRoot, engineImage, postgresImage, maxConns)
	default:
		return nil, nil, nil, fmt.Errorf("unsupported topology %q", topology)
	}
}

func startSingleNodeContainers(ctx context.Context, repoRoot, engineImage, postgresImage string, maxConns int) (*runtimeDB, *runtimeDB, func(), error) {
	engineTag := engineImage
	if engineTag == defaultEngineImage {
		engineTag = "omag-benchmark-engine:local"
	}

	if err := runCommand(ctx, repoRoot, "docker", "build", "-t", engineTag, "-f", filepath.Join(repoRoot, "Dockerfile"), repoRoot); err != nil {
		return nil, nil, nil, fmt.Errorf("build engine image: %w", err)
	}

	unique := strings.ReplaceAll(strconv.FormatInt(time.Now().UnixNano(), 36), "-", "")
	engineName := fmt.Sprintf("omag-bench-engine-%s", unique)
	postgresName := fmt.Sprintf("omag-bench-postgres-%s", unique)
	benchDir := filepath.Join(os.TempDir(), fmt.Sprintf("omag-bench-%s", unique))
	if err := os.MkdirAll(benchDir, 0o777); err != nil {
		return nil, nil, nil, err
	}
	if err := os.Chmod(benchDir, 0o777); err != nil {
		return nil, nil, nil, err
	}
	if err := os.MkdirAll(filepath.Join(benchDir, "engine", "lsm_data"), 0o777); err != nil {
		return nil, nil, nil, err
	}
	if err := os.Chmod(filepath.Join(benchDir, "engine", "lsm_data"), 0o777); err != nil {
		return nil, nil, nil, err
	}

	cleanup := func() {
		_, _ = exec.Command("docker", "rm", "-f", engineName).CombinedOutput()
		_, _ = exec.Command("docker", "rm", "-f", postgresName).CombinedOutput()
		_ = os.RemoveAll(benchDir)
	}

	if _, err := runOutput(ctx, repoRoot, "docker", "run", "-d", "--name", postgresName, "-P",
		"-e", "POSTGRES_DB=benchmark",
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust",
		postgresImage); err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("start postgres container: %w", err)
	}

	engineArgs := []string{
		"run", "-d", "--name", engineName, "-P",
		"-p", "127.0.0.1::6060",
		"-v", benchDir + ":/data",
		engineTag,
		"--listen", ":5432",
		"--pprof-listen", ":6060",
		"--db", "/data/test.db",
		"--lsm-data-dir", "/data/lsm_data",
		"--wal", "/data/test.wal",
	}
	if _, err := runOutput(ctx, repoRoot, "docker", engineArgs...); err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("start engine container: %w", err)
	}

	postgresPort, err := discoverPublishedPort(ctx, postgresName)
	if err != nil {
		cleanup()
		return nil, nil, nil, err
	}
	enginePort, err := discoverPublishedPort(ctx, engineName)
	if err != nil {
		cleanup()
		return nil, nil, nil, err
	}
	enginePprofPort, err := discoverPublishedPortFor(ctx, engineName, "6060/tcp")
	if err != nil {
		cleanup()
		return nil, nil, nil, err
	}

	postgresDB, err := openDB(ctx, postgresPort, "benchmark", maxConns)
	if err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("connect postgres: %w", err)
	}
	engineDB, err := openDB(ctx, enginePort, "postgres", maxConns)
	if err != nil {
		_ = postgresDB.conn.Close()
		cleanup()
		return nil, nil, nil, fmt.Errorf("connect engine: %w", err)
	}

	engineDB.pprofEndpoint = fmt.Sprintf("127.0.0.1:%d", enginePprofPort)

	cleanupFn := func() {
		_ = engineDB.conn.Close()
		_ = postgresDB.conn.Close()
		cleanup()
	}
	return engineDB, postgresDB, cleanupFn, nil
}

func startThreeNodeReplicationContainers(ctx context.Context, repoRoot, engineImage, postgresImage string, maxConns int) (*runtimeDB, *runtimeDB, func(), error) {
	engineTag := engineImage
	if engineTag == defaultEngineImage {
		engineTag = "omag-benchmark-engine:local"
	}

	if err := runCommand(ctx, repoRoot, "docker", "build", "-t", engineTag, "-f", filepath.Join(repoRoot, "Dockerfile"), repoRoot); err != nil {
		return nil, nil, nil, fmt.Errorf("build engine image: %w", err)
	}

	unique := strings.ReplaceAll(strconv.FormatInt(time.Now().UnixNano(), 36), "-", "")
	benchDir := filepath.Join(os.TempDir(), fmt.Sprintf("omag-bench-%s", unique))
	if err := os.MkdirAll(benchDir, 0o777); err != nil {
		return nil, nil, nil, err
	}
	if err := os.Chmod(benchDir, 0o777); err != nil {
		return nil, nil, nil, err
	}

	networkName := fmt.Sprintf("omag-bench-net-%s", unique)
	if _, err := runOutput(ctx, repoRoot, "docker", "network", "create", networkName); err != nil {
		_ = os.RemoveAll(benchDir)
		return nil, nil, nil, fmt.Errorf("create docker network: %w", err)
	}

	containerNames := make([]string, 0, 6)
	cleanup := func() {
		for _, name := range containerNames {
			_, _ = exec.Command("docker", "rm", "-f", name).CombinedOutput()
		}
		_, _ = exec.Command("docker", "network", "rm", networkName).CombinedOutput()
		_ = os.RemoveAll(benchDir)
	}

	postgresPrimaryName := fmt.Sprintf("omag-bench-pg-primary-%s", unique)
	postgresReplica1Name := fmt.Sprintf("omag-bench-pg-replica1-%s", unique)
	postgresReplica2Name := fmt.Sprintf("omag-bench-pg-replica2-%s", unique)
	postgresReplicas := []string{postgresReplica1Name, postgresReplica2Name}
	containerNames = append(containerNames, postgresPrimaryName, postgresReplica1Name, postgresReplica2Name)

	if _, err := runOutput(ctx, repoRoot, "docker", "run", "-d",
		"--name", postgresPrimaryName,
		"--network", networkName,
		"-P",
		"-e", "POSTGRES_DB=benchmark",
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust",
		postgresImage,
		"postgres",
		"-c", "wal_level=replica",
		"-c", "max_wal_senders=10",
		"-c", "max_replication_slots=10",
		"-c", "hot_standby=on",
	); err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("start postgres primary: %w", err)
	}

	postgresPort, err := discoverPublishedPort(ctx, postgresPrimaryName)
	if err != nil {
		cleanup()
		return nil, nil, nil, err
	}

	primaryReadyDB, err := openDB(ctx, postgresPort, "benchmark", maxConns)
	if err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("connect postgres primary: %w", err)
	}
	_ = primaryReadyDB.conn.Close()

	if _, err := runOutput(ctx, repoRoot, "docker", "exec", postgresPrimaryName, "sh", "-ceu",
		`tmp_hba="$(mktemp)"
cat <<'EOF' > "$tmp_hba"
host replication all all trust
host all all all trust
EOF
cat "$PGDATA/pg_hba.conf" >> "$tmp_hba"
cat "$tmp_hba" > "$PGDATA/pg_hba.conf"
rm -f "$tmp_hba"
psql -U postgres -d benchmark -c "SELECT pg_reload_conf();"`); err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("configure postgres primary replication access: %w", err)
	}

	for _, replica := range postgresReplicas {
		script := fmt.Sprintf(`set -eu
rm -rf "$PGDATA"/*
until pg_basebackup -h %s -D "$PGDATA" -U postgres -Fp -Xs -R -P; do
  sleep 1
done
chown -R postgres:postgres "$PGDATA"
chmod -R 700 "$PGDATA"
echo "hot_standby = on" >> "$PGDATA/postgresql.conf"
echo "primary_conninfo = 'host=%s port=5432 user=postgres application_name=%s'" >> "$PGDATA/postgresql.auto.conf"
exec su-exec postgres postgres
`, postgresPrimaryName, postgresPrimaryName, replica)
		if _, err := runOutput(ctx, repoRoot, "docker", "run", "-d",
			"--name", replica,
			"--network", networkName,
			"--entrypoint", "sh",
			"-e", "POSTGRES_DB=benchmark",
			"-e", "POSTGRES_HOST_AUTH_METHOD=trust",
			postgresImage,
			"-ceu", script,
		); err != nil {
			cleanup()
			return nil, nil, nil, fmt.Errorf("start postgres replica %s: %w", replica, err)
		}
	}

	if err := waitForPostgresReplicas(ctx, postgresPort, maxConns, postgresReplicas); err != nil {
		diagnostics := collectContainerStartupDiagnostics(postgresPrimaryName, postgresReplicas)
		cleanup()
		return nil, nil, nil, fmt.Errorf("%w\n%s", err, diagnostics)
	}

	engineNames := []string{
		fmt.Sprintf("omag-bench-engine-n0-%s", unique),
		fmt.Sprintf("omag-bench-engine-n1-%s", unique),
		fmt.Sprintf("omag-bench-engine-n2-%s", unique),
	}
	containerNames = append(containerNames, engineNames...)

	for i, engineName := range engineNames {
		nodeID := fmt.Sprintf("n%d", i)
		nodeDir := filepath.Join(benchDir, "engine", nodeID)
		if err := os.MkdirAll(filepath.Join(nodeDir, "lsm_data"), 0o777); err != nil {
			cleanup()
			return nil, nil, nil, err
		}
		if err := os.Chmod(nodeDir, 0o777); err != nil {
			cleanup()
			return nil, nil, nil, err
		}
		if err := os.Chmod(filepath.Join(nodeDir, "lsm_data"), 0o777); err != nil {
			cleanup()
			return nil, nil, nil, err
		}

		args := []string{"run", "-d", "--name", engineName, "--network", networkName, "-v", nodeDir + ":/data"}
		if i == 0 {
			args = append(args, "-p", "127.0.0.1::5432", "-p", "127.0.0.1::6060")
		}
		args = append(args,
			engineTag,
			"--listen", ":5432",
			"--db", "/data/test.db",
			"--lsm-data-dir", "/data/lsm_data",
			"--wal", "/data/test.wal",
			"--replication-strategy", "raft",
			"--replication-backend", "grpc",
			"--replication-min-write-acks", "2",
			"--replication-grpc-listen", ":7000",
		)
		peerParts := make([]string, 0, 2)
		for j, peer := range engineNames {
			if j == i {
				continue
			}
			peerParts = append(peerParts, fmt.Sprintf("n%d=%s:7000", j, peer))
		}
		args = append(args, "--replication-peer-nodes", strings.Join(peerParts, ","))
		if i == 0 {
			args = append(args, "--pprof-listen", ":6060")
		}

		if _, err := runOutput(ctx, repoRoot, "docker", args...); err != nil {
			cleanup()
			return nil, nil, nil, fmt.Errorf("start engine node %s: %w", nodeID, err)
		}
	}

	// Wait for engine nodes to be fully started before discovering ports
	time.Sleep(3 * time.Second)

	engineLeaderPort, err := discoverPublishedPortWithRetry(ctx, engineNames[0], 15)
	if err != nil {
		engineDiagnostics := collectContainerStartupDiagnostics(engineNames[0], engineNames[1:])
		cleanup()
		return nil, nil, nil, fmt.Errorf("%w\n%s", err, engineDiagnostics)
	}
	enginePprofPort, err := discoverPublishedPortForWithRetry(ctx, engineNames[0], "6060/tcp", 15)
	if err != nil {
		cleanup()
		return nil, nil, nil, err
	}

	postgresDB, err := openDB(ctx, postgresPort, "benchmark", maxConns)
	if err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("connect postgres primary: %w", err)
	}
	engineDB, err := openDB(ctx, engineLeaderPort, "postgres", maxConns)
	if err != nil {
		_ = postgresDB.conn.Close()
		cleanup()
		return nil, nil, nil, fmt.Errorf("connect engine leader: %w", err)
	}
	engineDB.pprofEndpoint = fmt.Sprintf("127.0.0.1:%d", enginePprofPort)

	cleanupFn := func() {
		_ = engineDB.conn.Close()
		_ = postgresDB.conn.Close()
		cleanup()
	}
	return engineDB, postgresDB, cleanupFn, nil
}

func waitForPostgresReplicas(ctx context.Context, primaryPort, maxConns int, expectedApps []string) error {
	db, err := openDB(ctx, primaryPort, "benchmark", maxConns)
	if err != nil {
		return fmt.Errorf("open postgres primary for replica wait: %w", err)
	}
	defer db.conn.Close()

	expected := map[string]struct{}{}
	for _, app := range expectedApps {
		trimmed := strings.TrimSpace(app)
		if trimmed == "" {
			continue
		}
		expected[trimmed] = struct{}{}
	}
	if len(expected) == 0 {
		return nil
	}

	deadline := time.Now().Add(90 * time.Second)
	var lastRows []postgresReplicationRow
	for time.Now().Before(deadline) {
		rows, err := fetchPostgresReplicationRows(ctx, db.conn)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		lastRows = rows
		ready, exactMatch, _, matchedNames := evaluatePostgresReplicaReadiness(expected, rows)
		if ready {
			if !exactMatch {
				fmt.Fprintf(os.Stderr, "postgres replica readiness fallback: expected names=%v matched=%d/%d observed=%s\n",
					expectedApps,
					matchedNames,
					len(expected),
					formatPostgresReplicationRows(rows),
				)
			}
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timeout waiting for %d streaming postgres replicas (expected apps=%v, last observed=%s)",
		len(expected),
		expectedApps,
		formatPostgresReplicationRows(lastRows),
	)
}

type postgresReplicationRow struct {
	ApplicationName string
	State           string
}

func fetchPostgresReplicationRows(ctx context.Context, db *sql.DB) ([]postgresReplicationRow, error) {
	rows, err := db.QueryContext(ctx, "SELECT application_name, state FROM pg_stat_replication")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]postgresReplicationRow, 0, 4)
	for rows.Next() {
		var appName, state string
		if err := rows.Scan(&appName, &state); err != nil {
			return nil, err
		}
		out = append(out, postgresReplicationRow{ApplicationName: strings.TrimSpace(appName), State: strings.TrimSpace(state)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func evaluatePostgresReplicaReadiness(expected map[string]struct{}, rows []postgresReplicationRow) (ready bool, exactMatch bool, streamingCount int, matchedNames int) {
	streamingByName := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if !strings.EqualFold(strings.TrimSpace(row.State), "streaming") {
			continue
		}
		streamingCount++
		if row.ApplicationName != "" {
			streamingByName[row.ApplicationName] = struct{}{}
		}
	}

	for app := range expected {
		if _, ok := streamingByName[app]; ok {
			matchedNames++
		}
	}
	exactMatch = matchedNames == len(expected)
	if exactMatch {
		return true, true, streamingCount, matchedNames
	}

	// Some environments expose different application_name values; fall back to replica count.
	if streamingCount >= len(expected) {
		return true, false, streamingCount, matchedNames
	}
	return false, false, streamingCount, matchedNames
}

func formatPostgresReplicationRows(rows []postgresReplicationRow) string {
	if len(rows) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		app := row.ApplicationName
		if app == "" {
			app = "<empty>"
		}
		state := row.State
		if state == "" {
			state = "<empty>"
		}
		parts = append(parts, fmt.Sprintf("%s:%s", app, state))
	}
	return strings.Join(parts, ", ")
}

func collectContainerStartupDiagnostics(primary string, replicas []string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	containers := make([]string, 0, 1+len(replicas))
	containers = append(containers, primary)
	containers = append(containers, replicas...)

	var b strings.Builder
	b.WriteString("postgres startup diagnostics:")
	for _, name := range containers {
		if strings.TrimSpace(name) == "" {
			continue
		}
		statusOut, statusErr := runOutput(ctx, "", "docker", "inspect", "--format", "{{.State.Status}}", name)
		logOut, logErr := runOutput(ctx, "", "docker", "logs", "--tail", "40", name)

		b.WriteString("\n- ")
		b.WriteString(name)
		if statusErr != nil {
			b.WriteString(" status=<unavailable>")
		} else {
			b.WriteString(" status=")
			b.WriteString(strings.TrimSpace(statusOut))
		}
		if logErr != nil {
			b.WriteString(" logs=<unavailable>")
			continue
		}
		logs := strings.TrimSpace(logOut)
		if logs == "" {
			b.WriteString(" logs=<empty>")
			continue
		}
		b.WriteString(" logs=\n")
		b.WriteString(indentLines(logs, "    "))
	}
	return b.String()
}

func indentLines(text, prefix string) string {
	if text == "" {
		return ""
	}
	lines := strings.Split(text, "\n")
	for i := range lines {
		lines[i] = prefix + lines[i]
	}
	return strings.Join(lines, "\n")
}

func openDB(ctx context.Context, port int, dbName string, maxConns int) (*runtimeDB, error) {
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://postgres@127.0.0.1:%d/%s?sslmode=disable", port, dbName))
	if err != nil {
		return nil, err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["application_name"] = "omag-benchmark"

	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(0)

	pingCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for i := 0; i < 90; i++ {
		var ready string
		if err := db.QueryRowContext(pingCtx, "SELECT 'ok'").Scan(&ready); err == nil && ready == "ok" {
			return &runtimeDB{
				conn:     db,
				endpoint: fmt.Sprintf("127.0.0.1:%d", port),
			}, nil
		}
		time.Sleep(time.Second)
	}
	_ = db.Close()
	return nil, fmt.Errorf("timeout waiting for postgres at 127.0.0.1:%d", port)
}

func discoverPublishedPort(ctx context.Context, containerName string) (int, error) {
	return discoverPublishedPortFor(ctx, containerName, "5432/tcp")
}

func discoverPublishedPortFor(ctx context.Context, containerName string, containerPort string) (int, error) {
	out, err := runOutput(ctx, "", "docker", "port", containerName, containerPort)
	if err != nil {
		return 0, err
	}
	line := firstNonEmptyLine(out)
	if line == "" {
		return 0, fmt.Errorf("no published port discovered for %s", containerName)
	}
	rhs := line
	if idx := strings.Index(line, "->"); idx >= 0 {
		rhs = strings.TrimSpace(line[idx+2:])
	}
	host, portStr, err := net.SplitHostPort(strings.TrimSpace(rhs))
	if err != nil {
		return 0, fmt.Errorf("parse published port for %s %s from %q: %w", containerName, containerPort, line, err)
	}
	_ = host
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, err
	}
	return port, nil
}

func discoverPublishedPortWithRetry(ctx context.Context, containerName string, maxRetries int) (int, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		port, err := discoverPublishedPort(ctx, containerName)
		if err == nil {
			return port, nil
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	return 0, fmt.Errorf("port discovery failed after %d attempts for %s: %w", maxRetries, containerName, lastErr)
}

func discoverPublishedPortForWithRetry(ctx context.Context, containerName, containerPort string, maxRetries int) (int, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		port, err := discoverPublishedPortFor(ctx, containerName, containerPort)
		if err == nil {
			return port, nil
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	return 0, fmt.Errorf("port %s discovery failed after %d attempts for %s: %w", containerPort, maxRetries, containerName, lastErr)
}

func collectEngineCPUHotspots(ctx context.Context, engineDB *runtimeDB, seedRows, warmupOps, measuredOps int) ([]cpuHotspot, string, int, string) {
	if engineDB == nil || engineDB.conn == nil || strings.TrimSpace(engineDB.pprofEndpoint) == "" {
		return nil, "", 0, "engine pprof endpoint unavailable"
	}

	profileSeconds := 10
	profilePath := filepath.Join(os.TempDir(), fmt.Sprintf("omag-engine-cpu-%d.pprof", time.Now().UnixNano()))

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- replayEngineWorkloadForProfile(runCtx, engineDB.conn, seedRows, warmupOps, measuredOps)
	}()

	if err := downloadCPUProfile(ctx, engineDB.pprofEndpoint, profileSeconds, profilePath); err != nil {
		cancelRun()
		_ = <-runErrCh
		return nil, "", 0, fmt.Sprintf("cpu profile collection failed: %v", err)
	}

	cancelRun()
	runErr := <-runErrCh
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return nil, profilePath, profileSeconds, fmt.Sprintf("workload replay warning: %v", runErr)
	}

	hotspots, err := parseCPUHotspotsFromProfile(ctx, profilePath, 8)
	if err != nil {
		return nil, profilePath, profileSeconds, fmt.Sprintf("cpu hotspot parsing failed: %v", err)
	}
	if len(hotspots) == 0 {
		return nil, profilePath, profileSeconds, "cpu profile collected but no omag functions found"
	}
	return hotspots, profilePath, profileSeconds, ""
}

func replayEngineWorkloadForProfile(ctx context.Context, db *sql.DB, seedRows, warmupOps, measuredOps int) error {
	workloads := []workloadName{workloadReads, workloadWhere, workloadWrites, workloadDelete, workloadMixed}
	replayID := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, workload := range workloads {
			tableName := strings.ReplaceAll(string(workload), "-", "_")
			warmupTable := fmt.Sprintf("bench_engine_%s_profile_warmup_%d", tableName, replayID)
			measuredTable := fmt.Sprintf("bench_engine_%s_profile_run_%d", tableName, replayID)

			if err := prepareTable(ctx, db, warmupTable, seedRows); err != nil {
				return err
			}
			if warmupOps > 0 {
				if err := runSteps(ctx, db, buildSteps(warmupTable, workload, seedRows, warmupOps)); err != nil {
					return err
				}
			}
			if err := prepareTable(ctx, db, measuredTable, seedRows); err != nil {
				return err
			}
			if err := runSteps(ctx, db, buildSteps(measuredTable, workload, seedRows, measuredOps)); err != nil {
				return err
			}
		}
		replayID++
	}
}

func downloadCPUProfile(ctx context.Context, endpoint string, seconds int, outputPath string) error {
	url := fmt.Sprintf("http://%s/debug/pprof/profile?seconds=%d", endpoint, seconds)
	client := &http.Client{Timeout: time.Duration(seconds+15) * time.Second}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected pprof status: %s", resp.Status)
	}

	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return err
	}
	return nil
}

func parseCPUHotspotsFromProfile(ctx context.Context, profilePath string, limit int) ([]cpuHotspot, error) {
	cmd := exec.CommandContext(ctx, "go", "tool", "pprof", "-top", "-nodecount", "120", profilePath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("go tool pprof failed: %w\n%s", err, string(out))
	}

	lines := strings.Split(string(out), "\n")
	hotspots := make([]cpuHotspot, 0, limit)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		flatPct, ok := parsePercent(fields[1])
		if !ok {
			continue
		}
		cumPct, ok := parsePercent(fields[4])
		if !ok {
			continue
		}
		fn := fields[len(fields)-1]
		if !strings.Contains(fn, "github.com/rodrigo0345/omag/") {
			continue
		}

		p := flatPct / 100.0
		maxSpeedup := 1.0
		twoXSpeedup := 1.0
		if p > 0 && p < 1 {
			maxSpeedup = 1.0 / (1.0 - p)
			twoXSpeedup = 1.0 / ((1.0 - p) + (p / 2.0))
		}
		hotspots = append(hotspots, cpuHotspot{
			Function:         fn,
			FlatPercent:      flatPct,
			CumPercent:       cumPct,
			AmdahlMaxSpeedup: maxSpeedup,
			Amdahl2xSpeedup:  twoXSpeedup,
		})
		if len(hotspots) >= limit {
			break
		}
	}

	for i := range hotspots {
		nested, err := parseNestedCPUHotspotsForFunction(ctx, profilePath, hotspots[i].Function, 4)
		if err == nil {
			hotspots[i].Nested = nested
		}
	}

	return hotspots, nil
}

func parseNestedCPUHotspotsForFunction(ctx context.Context, profilePath, fn string, limit int) ([]cpuNestedHotspot, error) {
	escapedFn := regexp.QuoteMeta(fn)
	cmd := exec.CommandContext(
		ctx,
		"go", "tool", "pprof", "-top", "-nodecount", "120",
		"-focus", "^"+escapedFn+"$",
		profilePath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("nested pprof failed for %s: %w\n%s", fn, err, string(out))
	}

	entries := make([]cpuNestedHotspot, 0, limit)
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		flatPct, ok := parsePercent(fields[1])
		if !ok {
			continue
		}
		cumPct, ok := parsePercent(fields[4])
		if !ok {
			continue
		}
		cand := fields[len(fields)-1]
		if cand == fn || !strings.Contains(cand, "github.com/rodrigo0345/omag/") {
			continue
		}
		entries = append(entries, cpuNestedHotspot{Function: cand, FlatPercent: flatPct, CumPercent: cumPct})
		if len(entries) >= limit {
			break
		}
	}
	return entries, nil
}

func parsePercent(raw string) (float64, bool) {
	if !strings.HasSuffix(raw, "%") {
		return 0, false
	}
	v, err := strconv.ParseFloat(strings.TrimSuffix(raw, "%"), 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func runBenchmarks(ctx context.Context, engineDB, postgresDB *runtimeDB, seedRows, warmupOps, measuredOps int, engineImage, postgresImage string) (benchmarkReport, error) {
	workloads := []workloadName{workloadReads, workloadWhere, workloadWrites, workloadDelete, workloadMixed}
	results := make([]workloadResult, 0, len(workloads)*2)

	for _, backend := range []struct {
		name backendName
		db   *runtimeDB
	}{
		{name: backendEngine, db: engineDB},
		{name: backendPostgres, db: postgresDB},
	} {
		backendConn, err := backend.db.conn.Conn(ctx)
		if err != nil {
			return benchmarkReport{}, fmt.Errorf("pin %s connection: %w", backend.name, err)
		}
		defer backendConn.Close()

		for _, workload := range workloads {
			tableName := strings.ReplaceAll(string(workload), "-", "_")
			warmupTable := fmt.Sprintf("bench_%s_%s_warmup", backend.name, tableName)
			measuredTable := fmt.Sprintf("bench_%s_%s_run", backend.name, tableName)

			if err := prepareTable(ctx, backendConn, warmupTable, seedRows); err != nil {
				return benchmarkReport{}, fmt.Errorf("prepare warmup %s/%s: %w", backend.name, workload, err)
			}

			if warmupOps > 0 {
				warmupSteps := buildSteps(warmupTable, workload, seedRows, warmupOps)
				if err := runSteps(ctx, backendConn, warmupSteps); err != nil {
					return benchmarkReport{}, fmt.Errorf("warmup %s/%s: %w", backend.name, workload, err)
				}
			}

			if err := prepareTable(ctx, backendConn, measuredTable, seedRows); err != nil {
				return benchmarkReport{}, fmt.Errorf("prepare measured %s/%s: %w", backend.name, workload, err)
			}

			steps := buildSteps(measuredTable, workload, seedRows, measuredOps)
			res, err := measureWorkload(ctx, backendConn, backend.name, workload, steps, 0)
			if err != nil {
				return benchmarkReport{}, fmt.Errorf("measure %s/%s: %w", backend.name, workload, err)
			}
			res.Warmup = warmupOps
			results = append(results, res)
		}
	}

	return benchmarkReport{
		EngineImage:   engineImage,
		PostgresImage: postgresImage,
		SeedRows:      seedRows,
		WarmupOps:     warmupOps,
		MeasuredOps:   measuredOps,
		Results:       results,
	}, nil
}

func runBenchmarksIterative(ctx context.Context, engineDB, postgresDB *runtimeDB, seedRows, warmupOps, measuredOps, iterations int, engineImage, postgresImage string) (benchmarkReport, error) {
	var last benchmarkReport
	for i := 1; i <= iterations; i++ {
		report, err := runBenchmarks(ctx, engineDB, postgresDB, seedRows, warmupOps, measuredOps, engineImage, postgresImage)
		if err != nil {
			return benchmarkReport{}, fmt.Errorf("iteration %d: %w", i, err)
		}
		fmt.Printf("\nIteration %d/%d complete\n", i, iterations)
		printSummary(report)
		if i > 1 {
			printIterationDelta(last, report)
		}
		last = report
	}
	return last, nil
}

func prepareTable(ctx context.Context, db sqlExecutor, table string, seedRows int) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", table)); err != nil {
		// Best effort cleanup; the table may not exist yet.
	}
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			category TEXT,
			status TEXT,
			payload TEXT
		)`, table),
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	batchSize := 25
	for i := 0; i < seedRows; i += batchSize {
		var batch strings.Builder
		for j := i; j < i+batchSize && j < seedRows; j++ {
			if batch.Len() > 0 {
				batch.WriteString(";")
			}
			batch.WriteString(fmt.Sprintf(`INSERT INTO %s (id, category, status, payload) VALUES ('%s', '%s', '%s', '%s')`,
				table,
				seedKey(table, j),
				seedCategory(j),
				seedStatus(j),
				seedPayload(j),
			))
		}
		if _, err := db.ExecContext(ctx, batch.String()); err != nil {
			return err
		}
	}
	return nil
}

func buildSteps(table string, workload workloadName, seedRows, measuredOps int) []benchStep {
	steps := make([]benchStep, 0, measuredOps)
	for i := 0; i < measuredOps; i++ {
		switch workload {
		case workloadReads:
			id := seedKey(table, i%seedRows)
			steps = append(steps, benchStep{isQuery: true, sql: fmt.Sprintf("SELECT payload FROM %s WHERE id = '%s'", table, id)})
		case workloadWhere:
			steps = append(steps, benchStep{isQuery: true, sql: fmt.Sprintf("SELECT id, payload FROM %s WHERE category = 'hot' AND status = 'active'", table)})
		case workloadWrites:
			steps = append(steps, benchStep{sql: fmt.Sprintf("INSERT INTO %s (id, category, status, payload) VALUES ('write_%04d', '%s', '%s', '%s')", table, i, seedCategory(i), seedStatus(i), seedPayload(1000+i))})
		case workloadDelete:
			id := seedKey(table, i%seedRows)
			steps = append(steps, benchStep{sql: fmt.Sprintf("DELETE FROM %s WHERE id = '%s'", table, id)})
		case workloadMixed:
			switch i % 4 {
			case 0:
				id := seedKey(table, i%seedRows)
				steps = append(steps, benchStep{isQuery: true, sql: fmt.Sprintf("SELECT payload FROM %s WHERE id = '%s'", table, id)})
			case 1:
				steps = append(steps, benchStep{isQuery: true, sql: fmt.Sprintf("SELECT id, payload FROM %s WHERE category = 'hot' AND status = 'active'", table)})
			case 2:
				steps = append(steps, benchStep{sql: fmt.Sprintf("INSERT INTO %s (id, category, status, payload) VALUES ('mix_write_%04d', '%s', '%s', '%s')", table, i, seedCategory(i), seedStatus(i), seedPayload(2000+i))})
			default:
				id := seedKey(table, i%seedRows)
				steps = append(steps, benchStep{sql: fmt.Sprintf("DELETE FROM %s WHERE id = '%s'", table, id)})
			}
		}
	}
	return steps
}

func measureWorkload(ctx context.Context, db sqlExecutor, backend backendName, workload workloadName, steps []benchStep, warmupOps int) (workloadResult, error) {
	durations := make([]time.Duration, 0, len(steps))
	start := time.Now()
	for _, step := range steps {
		stepStart := time.Now()
		if err := executeStep(ctx, db, step); err != nil {
			return workloadResult{}, err
		}
		durations = append(durations, time.Since(stepStart))
	}
	total := time.Since(start)

	avg := time.Duration(0)
	if len(durations) > 0 {
		var sum time.Duration
		for _, d := range durations {
			sum += d
		}
		avg = sum / time.Duration(len(durations))
	}

	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	result := workloadResult{
		Backend:    backend,
		Workload:   workload,
		Warmup:     warmupOps,
		Measured:   len(steps),
		Total:      total,
		Throughput: calculateThroughput(len(steps), total),
		Avg:        avg,
		P50:        percentile(sorted, 0.50),
		P95:        percentile(sorted, 0.95),
		P99:        percentile(sorted, 0.99),
	}
	if len(sorted) > 0 {
		result.Min = sorted[0]
		result.Max = sorted[len(sorted)-1]
	}
	return result, nil
}

func calculateThroughput(ops int, elapsed time.Duration) float64 {
	if ops <= 0 || elapsed <= 0 {
		return 0
	}
	return float64(ops) / elapsed.Seconds()
}

func runSteps(ctx context.Context, db sqlExecutor, steps []benchStep) error {
	for _, step := range steps {
		if err := executeStep(ctx, db, step); err != nil {
			return err
		}
	}
	return nil
}

func executeStep(ctx context.Context, db sqlExecutor, step benchStep) error {
	stepCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if step.isQuery {
		rows, err := db.QueryContext(stepCtx, step.sql)
		if err != nil {
			return err
		}
		defer rows.Close()
		return drainRows(rows)
	}
	_, err := db.ExecContext(stepCtx, step.sql)
	return err
}

func drainRows(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	values := make([][]byte, len(cols))
	args := make([]any, len(cols))
	for i := range values {
		args[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(args...); err != nil {
			return err
		}
	}
	return rows.Err()
}

func printSummary(report benchmarkReport) {
	fmt.Println()
	fmt.Println("Workload summary:")
	for _, result := range report.Results {
		fmt.Printf("- %-8s %-14s %8.2f ops/s avg=%s p95=%s\n",
			result.Backend,
			result.Workload,
			result.Throughput,
			formatDurationCompact(result.Avg),
			formatDurationCompact(result.P95),
		)
	}
}

func printIterationDelta(prev benchmarkReport, current benchmarkReport) {
	if len(prev.Results) == 0 || len(current.Results) == 0 {
		return
	}
	prevByKey := make(map[string]workloadResult, len(prev.Results))
	for _, r := range prev.Results {
		prevByKey[string(r.Backend)+"|"+string(r.Workload)] = r
	}

	fmt.Println("Iteration delta (throughput):")
	for _, r := range current.Results {
		key := string(r.Backend) + "|" + string(r.Workload)
		before, ok := prevByKey[key]
		if !ok || before.Throughput == 0 {
			continue
		}
		delta := ((r.Throughput - before.Throughput) / before.Throughput) * 100
		fmt.Printf("- %-8s %-14s %8.2f -> %8.2f ops/s (%+.2f%%)\n", r.Backend, r.Workload, before.Throughput, r.Throughput, delta)
	}
}

func printConnectionStats(name string, db *sql.DB) {
	if db == nil {
		return
	}
	stats := db.Stats()
	fmt.Printf("\nConnection stats [%s]: open=%d inUse=%d idle=%d waitCount=%d waitDuration=%s maxOpen=%d\n",
		name,
		stats.OpenConnections,
		stats.InUse,
		stats.Idle,
		stats.WaitCount,
		stats.WaitDuration,
		stats.MaxOpenConnections,
	)
}

func printEngineCPUHotspotTiming(hotspots []cpuHotspot, profileSeconds int) {
	if len(hotspots) == 0 || profileSeconds <= 0 {
		return
	}
	profileDuration := time.Duration(profileSeconds) * time.Second
	fmt.Println("\nEngine function timing estimate (from pprof percentages):")
	for _, h := range hotspots {
		flat := time.Duration(float64(profileDuration) * (h.FlatPercent / 100.0))
		cum := time.Duration(float64(profileDuration) * (h.CumPercent / 100.0))
		fmt.Printf("- %-70s flat=%8s cum=%8s\n", h.Function, formatDurationCompact(flat), formatDurationCompact(cum))
		for _, nested := range h.Nested {
			nestedCum := time.Duration(float64(profileDuration) * (nested.CumPercent / 100.0))
			fmt.Printf("    nested %-63s cum=%8s\n", nested.Function, formatDurationCompact(nestedCum))
		}
	}
}

func collectPostgresExplain(ctx context.Context, db *sql.DB, seedRows int) ([]postgresExplain, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres db is nil")
	}
	tableByWorkload := map[workloadName]string{
		workloadReads:  "bench_postgres_reads_run",
		workloadWhere:  "bench_postgres_where_clauses_run",
		workloadWrites: "bench_postgres_writes_run",
		workloadDelete: "bench_postgres_deletes_run",
		workloadMixed:  "bench_postgres_mixed_run",
	}
	queries := []struct {
		workload workloadName
		stmtFn   func(table string) string
	}{
		{workload: workloadReads, stmtFn: func(table string) string {
			return fmt.Sprintf("SELECT payload FROM %s WHERE id = '%s'", table, seedKey(table, 0))
		}},
		{workload: workloadWhere, stmtFn: func(table string) string {
			return fmt.Sprintf("SELECT id, payload FROM %s WHERE category = 'hot' AND status = 'active'", table)
		}},
		{workload: workloadWrites, stmtFn: func(table string) string {
			return fmt.Sprintf("INSERT INTO %s (id, category, status, payload) VALUES ('explain_write_0001', 'hot', 'active', '%s')", table, seedPayload(seedRows+1))
		}},
		{workload: workloadDelete, stmtFn: func(table string) string {
			return fmt.Sprintf("DELETE FROM %s WHERE id = '%s'", table, seedKey(table, 1))
		}},
		{workload: workloadMixed, stmtFn: func(table string) string {
			return fmt.Sprintf("SELECT id, payload FROM %s WHERE category = 'hot' AND status = 'active'", table)
		}},
	}

	analyses := make([]postgresExplain, 0, len(queries))
	for _, q := range queries {
		table := tableByWorkload[q.workload]
		if table == "" {
			continue
		}
		stmt := q.stmtFn(table)
		explainStmt := "EXPLAIN (ANALYZE, BUFFERS, VERBOSE) " + stmt
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("begin explain tx %s: %w", q.workload, err)
		}

		rows, err := tx.QueryContext(ctx, explainStmt)
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("explain %s: %w", q.workload, err)
		}

		lines := make([]string, 0, 16)
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				_ = rows.Close()
				_ = tx.Rollback()
				return nil, fmt.Errorf("scan explain %s: %w", q.workload, err)
			}
			lines = append(lines, line)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			return nil, fmt.Errorf("rows explain %s: %w", q.workload, err)
		}
		_ = rows.Close()
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			return nil, fmt.Errorf("rollback explain %s: %w", q.workload, err)
		}

		planning, execution, nodes := parseExplainLines(lines)
		analyses = append(analyses, postgresExplain{
			Workload:        q.workload,
			Statement:       stmt,
			PlanningTimeMS:  planning,
			ExecutionTimeMS: execution,
			NodeDetails:     nodes,
		})
	}

	return analyses, nil
}

func parseExplainLines(lines []string) (planningMS float64, executionMS float64, nodes []string) {
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "Planning Time:") {
			planningMS = parseExplainDuration(trimmed, "Planning Time:")
			continue
		}
		if strings.HasPrefix(trimmed, "Execution Time:") {
			executionMS = parseExplainDuration(trimmed, "Execution Time:")
			continue
		}
		if strings.Contains(trimmed, "actual time=") {
			nodes = append(nodes, trimmed)
		}
	}
	if len(nodes) > 8 {
		nodes = nodes[:8]
	}
	return planningMS, executionMS, nodes
}

func parseExplainDuration(line, prefix string) float64 {
	fields := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, prefix)))
	if len(fields) < 2 {
		return 0
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	return v
}

func printPostgresExplain(analysis []postgresExplain) {
	if len(analysis) == 0 {
		return
	}
	fmt.Println("\nPostgres query analyzer (EXPLAIN ANALYZE):")
	for _, a := range analysis {
		fmt.Printf("- %-14s planning=%6.3f ms execution=%6.3f ms\n", a.Workload, a.PlanningTimeMS, a.ExecutionTimeMS)
		fmt.Printf("  sql: %s\n", a.Statement)
		for _, node := range a.NodeDetails {
			fmt.Printf("  node: %s\n", node)
		}
	}
}

func percentile(sorted []time.Duration, pct float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if pct <= 0 {
		return sorted[0]
	}
	if pct >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(float64(len(sorted)-1) * pct)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func seedCategory(i int) string {
	switch i % 4 {
	case 0:
		return "hot"
	case 1:
		return "warm"
	case 2:
		return "cool"
	default:
		return "cold"
	}
}

func seedStatus(i int) string {
	if i%2 == 0 {
		return "active"
	}
	return "inactive"
}

func seedPayload(i int) string {
	return fmt.Sprintf("payload-%04d-%s", i, strings.Repeat("x", 24))
}

func seedKey(table string, i int) string {
	return fmt.Sprintf("%s_seed_%04d", strings.ReplaceAll(table, "-", "_"), i)
}

func findRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(wd, "go.mod")); err == nil {
			return wd, nil
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			return "", errors.New("go.mod not found in parent directories")
		}
		wd = parent
	}
}

func runCommand(ctx context.Context, dir string, name string, args ...string) error {
	_, err := runOutput(ctx, dir, name, args...)
	return err
}

func runOutput(ctx context.Context, dir string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, string(out))
	}
	return string(out), nil
}

func firstNonEmptyLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return ""
}
