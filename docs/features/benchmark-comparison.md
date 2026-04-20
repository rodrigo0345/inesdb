# Engine vs PostgreSQL Benchmark

This report compares OMAG's SQL engine against a stock PostgreSQL container using the same simple-query workload profile.

## What it measures

- Point reads by primary key
- `WHERE`-filtered reads using equality predicates joined by `AND`
- Insert-heavy write workloads
- Deletes against seeded rows
- A mixed workload that interleaves reads, filters, inserts, and deletes

## How to run it

From the repository root:

```bash
go run ./cmd/benchmark
```

The command will:

1. Build the OMAG engine container from `Dockerfile`
2. Start PostgreSQL with default container settings
3. Seed both databases with the same table and rows
4. Run the benchmark workloads over both backends
5. Collect a Postgres `EXPLAIN ANALYZE` summary for the benchmark tables without persisting write/delete side effects
6. Write the final PDF report to `docs/papers/engine-vs-postgres-benchmark.pdf`

You can adjust the workload size with flags such as:

```bash
go run ./cmd/benchmark -seed-rows 512 -warmup-ops 64 -ops 256
```

To benchmark 3-node replication on both systems (OMAG Raft leader+2 followers vs PostgreSQL primary+2 streaming replicas):

```bash
go run ./cmd/benchmark -topology three-node-replication
```

This mode drives SQL traffic through the OMAG leader and PostgreSQL primary while replication is active in the background.

The generated PDF includes:

- workload throughput and latency summaries
- engine CPU hotspot estimates from `pprof`
- a Postgres `EXPLAIN ANALYZE` section with planning and execution timings
- warning notes when analyzer data is unavailable or partially degraded

## Report

The latest generated report is stored at:

- [`docs/papers/engine-vs-postgres-benchmark.pdf`](../papers/engine-vs-postgres-benchmark.pdf)



