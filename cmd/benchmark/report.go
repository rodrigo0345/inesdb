package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/phpdave11/gofpdf"
)

func writeBenchmarkPDF(path string, report benchmarkReport) error {
	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.SetCompression(false)
	pdf.SetMargins(12, 12, 12)
	pdf.SetAutoPageBreak(true, 12)
	pdf.SetFont("Courier", "", 10)
	pdf.SetTextColor(0, 0, 0)

	for _, line := range renderReportLines(report) {
		if pdf.PageNo() == 0 {
			pdf.AddPage()
		}
		if line == "" {
			pdf.Ln(4)
			continue
		}
		pdf.MultiCell(0, 4.5, line, "", "L", false)
	}

	return pdf.OutputFileAndClose(path)
}

func renderReportLines(report benchmarkReport) []string {
	results := append([]workloadResult(nil), report.Results...)
	sort.Slice(results, func(i, j int) bool {
		if results[i].Backend == results[j].Backend {
			return results[i].Workload < results[j].Workload
		}
		return results[i].Backend < results[j].Backend
	})

	lines := []string{
		"OMAG / INESDB ENGINE VS POSTGRES PERFORMANCE REPORT",
		"",
		fmt.Sprintf("Generated: %s", report.GeneratedAt.Format(time.RFC3339)),
		fmt.Sprintf("Topology: %s", report.Topology),
		fmt.Sprintf("Host: %s/%s | Go: %s", report.HostOS, report.HostArch, report.GoVersion),
		fmt.Sprintf("Engine image: %s", report.EngineImage),
		fmt.Sprintf("Postgres image: %s", report.PostgresImage),
		fmt.Sprintf("Seed rows: %d | Warmup ops: %d | Measured ops: %d", report.SeedRows, report.WarmupOps, report.MeasuredOps),
		"",
		"RESULTS",
		"backend     workload        ops/s      avg        p50        p95        p99        min        max",
		strings.Repeat("-", 96),
	}
	for _, r := range results {
		lines = append(lines, fmt.Sprintf(
			"%-11s %-14s %8.2f %10s %10s %10s %10s %10s %10s",
			r.Backend,
			r.Workload,
			r.Throughput,
			formatDurationCompact(r.Avg),
			formatDurationCompact(r.P50),
			formatDurationCompact(r.P95),
			formatDurationCompact(r.P99),
			formatDurationCompact(r.Min),
			formatDurationCompact(r.Max),
		))
	}

	lines = append(lines, "")
	lines = append(lines, "POSTGRES QUERY ANALYZER (EXPLAIN ANALYZE)")
	if report.PostgresNote != "" {
		lines = append(lines, fmt.Sprintf("note: %s", report.PostgresNote))
	}
	if len(report.PostgresAnalysis) == 0 {
		lines = append(lines, "no postgres explain analysis captured")
	} else {
		for _, a := range report.PostgresAnalysis {
			lines = append(lines,
				fmt.Sprintf("- %-14s planning=%6.3f ms execution=%6.3f ms", a.Workload, a.PlanningTimeMS, a.ExecutionTimeMS),
				fmt.Sprintf("  sql: %s", a.Statement),
			)
			for _, node := range a.NodeDetails {
				lines = append(lines, fmt.Sprintf("  node: %s", node))
			}
		}
	}

	lines = append(lines, "")
	lines = append(lines, "ENGINE CPU HOTSPOTS (PPROF + AMDAHL)")
	if report.CPUProfileSeconds > 0 {
		lines = append(lines, fmt.Sprintf("profile capture: %ds", report.CPUProfileSeconds))
	}
	if report.CPUProfilePath != "" {
		lines = append(lines, fmt.Sprintf("profile file: %s", report.CPUProfilePath))
	}
	if report.CPUProfileNote != "" {
		lines = append(lines, fmt.Sprintf("note: %s", report.CPUProfileNote))
	}
	if len(report.CPUHotspots) == 0 {
		lines = append(lines, "no engine cpu hotspots captured")
	} else {
		lines = append(lines,
			"function                                                       flat%    cum%    amdahl(max)    amdahl(2x)",
			strings.Repeat("-", 96),
		)
		for _, h := range report.CPUHotspots {
			lines = append(lines, fmt.Sprintf("%-62s %6.2f%% %7.2f%% %11.2fx %11.2fx",
				h.Function,
				h.FlatPercent,
				h.CumPercent,
				h.AmdahlMaxSpeedup,
				h.Amdahl2xSpeedup,
			))
			if len(h.Nested) > 0 {
				lines = append(lines, "  nested hotspots inside selected samples:")
				for _, n := range h.Nested {
					lines = append(lines, fmt.Sprintf("    %-58s flat=%5.2f%% cum=%5.2f%%", n.Function, n.FlatPercent, n.CumPercent))
				}
			}
		}
	}

	lines = append(lines,
		"",
		"Notes:",
		"- Read workloads issue simple SELECT statements with point lookups.",
		"- Where-clause workloads filter with equality predicates joined by AND.",
		"- Write workloads insert new rows; delete workloads remove existing seed rows.",
		"- Mixed workloads interleave reads, filters, inserts, and deletes.",
		"- Amdahl(max) assumes only that function is optimized with infinite speedup.",
		"- Amdahl(2x) assumes that function is optimized by exactly 2x.",
	)
	return lines
}

func formatDurationCompact(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}
