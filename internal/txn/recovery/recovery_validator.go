package recovery

import (
	"fmt"
	"log"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	wallog "github.com/rodrigo0345/omag/internal/txn/log"
)

type RecoveryValidator struct {
	storageEngine storage.IStorageEngine
	bufferPool    buffer.IBufferPoolManager
}

type ValidationResult struct {
	IsValid bool
	Errors  []string
	Details map[string]interface{}
}

func NewRecoveryValidator(storageEngine storage.IStorageEngine, bufferPool buffer.IBufferPoolManager) *RecoveryValidator {
	return &RecoveryValidator{
		storageEngine: storageEngine,
		bufferPool:    bufferPool,
	}
}

func (rv *RecoveryValidator) ValidateRecoveredState(state *wallog.RecoveryState) (*ValidationResult, error) {
	result := &ValidationResult{
		IsValid: true,
		Errors:  make([]string, 0),
		Details: make(map[string]interface{}),
	}

	if state == nil {
		result.IsValid = false
		result.Errors = append(result.Errors, "recovery state is nil")
		return result, nil
	}

	log.Printf("[RecoveryValidator] Validating recovered state...")
	log.Printf("[RecoveryValidator] Committed transactions: %d, Aborted transactions: %d",
		len(state.CommittedTxns), len(state.AbortedTxns))

	if err := rv.validateStateConsistency(state, result); err != nil {
		log.Printf("[RecoveryValidator] State consistency check failed: %v", err)
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("state consistency failed: %v", err))
	}

	if err := rv.validateDirtyPageTable(state, result); err != nil {
		log.Printf("[RecoveryValidator] Dirty page table validation failed: %v", err)
		result.Errors = append(result.Errors, fmt.Sprintf("dirty page validation failed: %v", err))
	}

	if err := rv.validateTransactionConsistency(state, result); err != nil {
		log.Printf("[RecoveryValidator] Transaction consistency check failed: %v", err)
		result.Errors = append(result.Errors, fmt.Sprintf("transaction consistency failed: %v", err))
	}

	if err := rv.validateStorageEngineState(state, result); err != nil {
		log.Printf("[RecoveryValidator] Storage engine validation failed: %v", err)
		result.Errors = append(result.Errors, fmt.Sprintf("storage engine validation failed: %v", err))
	}

	if len(result.Errors) > 0 {
		result.IsValid = false
		log.Printf("[RecoveryValidator] Validation found %d issues", len(result.Errors))
	} else {
		log.Printf("[RecoveryValidator] Validation passed successfully")
	}

	return result, nil
}

func (rv *RecoveryValidator) validateStateConsistency(state *wallog.RecoveryState, result *ValidationResult) error {
	for txnID := range state.CommittedTxns {
		if _, exists := state.AbortedTxns[txnID]; exists {
			return fmt.Errorf("transaction %d appears in both committed and aborted sets", txnID)
		}
	}

	if state.PageStates == nil {
		log.Printf("[RecoveryValidator] Warning: page states map is nil (acceptable for clean startup)")
	}

	if state.UndoList == nil {
		log.Printf("[RecoveryValidator] Warning: undo list is nil")
		result.Details["undoListNil"] = true
	}

	result.Details["committedTxns"] = len(state.CommittedTxns)
	result.Details["abortedTxns"] = len(state.AbortedTxns)
	result.Details["pageStates"] = len(state.PageStates)

	return nil
}

func (rv *RecoveryValidator) validateDirtyPageTable(state *wallog.RecoveryState, result *ValidationResult) error {
	if state.DirtyPages == nil {
		log.Printf("[RecoveryValidator] Warning: dirty page table is nil")
		return nil
	}

	dptSize := len(state.DirtyPages)
	log.Printf("[RecoveryValidator] Validating dirty page table: %d pages", dptSize)

	if dptSize == 0 {
		log.Printf("[RecoveryValidator] Dirty page table is empty (acceptable for clean shutdown)")
	}

	result.Details["dirtyPageCount"] = dptSize

	return nil
}

func (rv *RecoveryValidator) validateTransactionConsistency(state *wallog.RecoveryState, result *ValidationResult) error {
	if state.TobeRedone == nil {
		log.Printf("[RecoveryValidator] Warning: redo list is nil")
		return nil
	}

	redoTxnCount := len(state.TobeRedone)
	var totalRedoRecords int64 = 0
	for _, lsns := range state.TobeRedone {
		totalRedoRecords += int64(len(lsns))
	}

	log.Printf("[RecoveryValidator] Redo transactions: %d, total redo records: %d", redoTxnCount, totalRedoRecords)

	for txnID := range state.TobeRedone {
		if _, isCommitted := state.CommittedTxns[txnID]; !isCommitted {
			return fmt.Errorf("redo txn %d not in committed transaction set", txnID)
		}
	}

	result.Details["redoTransactions"] = redoTxnCount
	result.Details["totalRedoRecords"] = totalRedoRecords

	return nil
}

func (rv *RecoveryValidator) validateStorageEngineState(state *wallog.RecoveryState, result *ValidationResult) error {
	entries, err := rv.storageEngine.Scan()
	if err != nil {
		return fmt.Errorf("storage engine scan failed: %w", err)
	}

	log.Printf("[RecoveryValidator] Storage engine scan successful: %d entries", len(entries))

	result.Details["storageEngineEntries"] = len(entries)


	return nil
}

func (r *ValidationResult) GetValidationSummary() string {
	status := "PASSED"
	if !r.IsValid {
		status = "FAILED"
	}

	summary := fmt.Sprintf("Recovery Validation: %s\n", status)

	if len(r.Details) > 0 {
		summary += "Details:\n"
		for key, value := range r.Details {
			summary += fmt.Sprintf("  %s: %v\n", key, value)
		}
	}

	if len(r.Errors) > 0 {
		summary += fmt.Sprintf("Errors (%d):\n", len(r.Errors))
		for i, err := range r.Errors {
			summary += fmt.Sprintf("  %d. %s\n", i+1, err)
		}
	}

	return summary
}
