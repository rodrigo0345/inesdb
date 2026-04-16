package acid_compliance

import (
	"fmt"
	"os"
	"time"

	wallog "github.com/rodrigo0345/omag/internal/txn/log"
)

type CrashPoint string

const (
	CrashPointBeforeMemtableWrite CrashPoint = "before_memtable_write"
	CrashPointAfterMemtableWrite  CrashPoint = "after_memtable_write"
	CrashPointBeforeWALFlush      CrashPoint = "before_wal_flush"
	CrashPointAfterWALFlush       CrashPoint = "after_wal_flush"
	CrashPointBeforeLSMCompaction CrashPoint = "before_lsm_compaction"
	CrashPointAfterLSMCompaction  CrashPoint = "after_lsm_compaction"
	CrashPointBeforeBTreeSplit    CrashPoint = "before_btree_split"
	CrashPointAfterBTreeSplit     CrashPoint = "after_btree_split"
	CrashPointDuringTransaction   CrashPoint = "during_transaction"
	CrashPointDuringBatchWrite    CrashPoint = "during_batch_write"
	CrashPointRandom              CrashPoint = "random"
)

type CrashSimulator struct {
	CrashPoint   CrashPoint
	Enabled      bool
	TriggerCount int
	CurrentCount int
}

type CrashEvent struct {
	Point       CrashPoint
	Timestamp   int64
	Details     map[string]interface{}
	Recovered   bool
	DataLoss    bool
	Description string
}

type CrashLog struct {
	Events               []CrashEvent
	TotalCrashes         int
	SuccessfulRecoveries int
	FailedRecoveries     int
	DataLossCount        int
}

func NewCrashSimulator(crashPoint CrashPoint) *CrashSimulator {
	return &CrashSimulator{
		CrashPoint:   crashPoint,
		Enabled:      crashPoint != "",
		TriggerCount: 1,
		CurrentCount: 0,
	}
}

func (cs *CrashSimulator) ShouldCrash(currentPoint CrashPoint) bool {
	if !cs.Enabled {
		return false
	}

	if cs.CrashPoint == CrashPointRandom {
		return false
	}

	if cs.CrashPoint == currentPoint {
		cs.CurrentCount++
		if cs.CurrentCount >= cs.TriggerCount {
			return true
		}
	}

	return false
}

func (cs *CrashSimulator) TriggerCrash(point CrashPoint, details map[string]interface{}) error {
	return fmt.Errorf("simulated crash at %s", point)
}

type CrashRecoveryScenario struct {
	Name                   string
	Description            string
	CrashPoint             CrashPoint
	PreCrashOperations     func() error
	PostRecoveryValidation func() error
	ExpectDataLoss         bool
	ExpectedCommittedCount int
	ExpectedAbortedCount   int
}

func NewCrashRecoveryScenario(name, description string, crashPoint CrashPoint) *CrashRecoveryScenario {
	return &CrashRecoveryScenario{
		Name:           name,
		Description:    description,
		CrashPoint:     crashPoint,
		ExpectDataLoss: false,
	}
}

func (s *CrashRecoveryScenario) ExecuteScenario() (*CrashEvent, error) {
	event := &CrashEvent{
		Point:     s.CrashPoint,
		Timestamp: time.Now().Unix(),
		Details:   make(map[string]interface{}),
	}

	if s.PreCrashOperations != nil {
		if err := s.PreCrashOperations(); err != nil {
			event.Details["preCrashError"] = err.Error()
			return event, fmt.Errorf("pre-crash operations failed: %w", err)
		}
	}

	event.Details["crashed"] = true
	event.Details["timestamp"] = event.Timestamp

	return event, nil
}

func (s *CrashRecoveryScenario) ValidateRecovery(event *CrashEvent, state *wallog.RecoveryState) error {
	if state == nil {
		return fmt.Errorf("recovery state is nil")
	}

	committedCount := len(state.CommittedTxns)
	if s.ExpectedCommittedCount > 0 && committedCount != s.ExpectedCommittedCount {
		return fmt.Errorf("expected %d committed transactions, got %d",
			s.ExpectedCommittedCount, committedCount)
	}

	abortedCount := len(state.AbortedTxns)
	if s.ExpectedAbortedCount > 0 && abortedCount != s.ExpectedAbortedCount {
		return fmt.Errorf("expected %d aborted transactions, got %d",
			s.ExpectedAbortedCount, abortedCount)
	}

	if s.PostRecoveryValidation != nil {
		if err := s.PostRecoveryValidation(); err != nil {
			return fmt.Errorf("post-recovery validation failed: %w", err)
		}
	}

	event.Recovered = true
	return nil
}

func NewCrashLog() *CrashLog {
	return &CrashLog{
		Events: make([]CrashEvent, 0),
	}
}

func (cl *CrashLog) LogCrash(event CrashEvent) {
	cl.Events = append(cl.Events, event)
	cl.TotalCrashes++
	if event.Recovered {
		cl.SuccessfulRecoveries++
	} else {
		cl.FailedRecoveries++
	}
	if event.DataLoss {
		cl.DataLossCount++
	}
}

func (cl *CrashLog) PrintSummary() string {
	summary := fmt.Sprintf("\n=== CRASH LOG SUMMARY ===\n")
	summary += fmt.Sprintf("Total Crashes: %d\n", cl.TotalCrashes)
	summary += fmt.Sprintf("Successful Recoveries: %d\n", cl.SuccessfulRecoveries)
	summary += fmt.Sprintf("Failed Recoveries: %d\n", cl.FailedRecoveries)
	summary += fmt.Sprintf("Data Loss Events: %d\n", cl.DataLossCount)
	summary += fmt.Sprintf("Recovery Success Rate: %.1f%%\n",
		float64(cl.SuccessfulRecoveries)/float64(cl.TotalCrashes)*100)

	if cl.DataLossCount > 0 {
		summary += fmt.Sprintf("\n⚠ WARNING: %d data loss events detected!\n", cl.DataLossCount)
	}

	return summary
}

func ClearState(walPath, dbPath string) error {
	if err := os.RemoveAll(walPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove WAL file: %w", err)
	}

	if err := os.RemoveAll(dbPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove database file: %w", err)
	}

	return nil
}

func CrashDuringMemtableWrite() *CrashRecoveryScenario {
	scenario := NewCrashRecoveryScenario(
		"Memtable Write Crash",
		"Crash occurs while writing data to LSM memtable",
		CrashPointBeforeMemtableWrite,
	)
	scenario.ExpectDataLoss = false
	scenario.ExpectedCommittedCount = 0
	return scenario
}

func CrashDuringLSMCompaction() *CrashRecoveryScenario {
	scenario := NewCrashRecoveryScenario(
		"LSM Compaction Crash",
		"Crash occurs during LSM tree compaction/merging",
		CrashPointBeforeLSMCompaction,
	)
	scenario.ExpectDataLoss = false
	return scenario
}

func CrashDuringBTreeNodeSplit() *CrashRecoveryScenario {
	scenario := NewCrashRecoveryScenario(
		"B+Tree Node Split Crash",
		"Crash occurs while splitting a B+Tree internal node",
		CrashPointBeforeBTreeSplit,
	)
	scenario.ExpectDataLoss = false
	return scenario
}

func CrashDuringBatchTransaction() *CrashRecoveryScenario {
	scenario := NewCrashRecoveryScenario(
		"Batch Transaction Crash",
		"Crash occurs while committing multiple transactions",
		CrashPointDuringBatchWrite,
	)
	scenario.ExpectDataLoss = false
	return scenario
}
