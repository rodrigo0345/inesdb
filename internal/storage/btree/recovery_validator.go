package btree

import (
	"fmt"
	"log"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/page"
)

type BTreeRecoveryValidator struct {
	bufferManager buffer.IBufferPoolManager
	btreeBackend  *BPlusTreeBackend
}

type CorruptionReport struct {
	TotalPagesScanned    int
	ValidPages           int
	CorruptedPages       []page.ResourcePageID
	OrphanedPages        []page.ResourcePageID
	BrokenOverflowChains []page.ResourcePageID
	UnreachableBlocks    []page.ResourcePageID

	IsCorrupted bool
	ErrorCount  int
	ErrorLog    []string
}

func NewBTreeRecoveryValidator(btreeBackend *BPlusTreeBackend, bufferManager buffer.IBufferPoolManager) *BTreeRecoveryValidator {
	return &BTreeRecoveryValidator{
		btreeBackend:  btreeBackend,
		bufferManager: bufferManager,
	}
}

func (v *BTreeRecoveryValidator) ValidateStructure() (*CorruptionReport, error) {
	report := &CorruptionReport{
		CorruptedPages:       make([]page.ResourcePageID, 0),
		OrphanedPages:        make([]page.ResourcePageID, 0),
		BrokenOverflowChains: make([]page.ResourcePageID, 0),
		UnreachableBlocks:    make([]page.ResourcePageID, 0),
		ErrorLog:             make([]string, 0),
	}

	log.Printf("[BTreeValidator] Starting B+Tree structure validation...")

	if err := v.validateMetadataPage(report); err != nil {
		report.ErrorLog = append(report.ErrorLog, fmt.Sprintf("Metadata validation failed: %v", err))
		report.ErrorCount++
	}

	if err := v.validateTreeStructure(report); err != nil {
		report.ErrorLog = append(report.ErrorLog, fmt.Sprintf("Tree structure validation failed: %v", err))
		report.ErrorCount++
	}

	if err := v.findOrphanedPages(report); err != nil {
		report.ErrorLog = append(report.ErrorLog, fmt.Sprintf("Orphaned page detection failed: %v", err))
		report.ErrorCount++
	}

	report.IsCorrupted = len(report.CorruptedPages) > 0 ||
		len(report.OrphanedPages) > 0 ||
		len(report.BrokenOverflowChains) > 0 ||
		report.ErrorCount > 0

	log.Printf("[BTreeValidator] Validation complete: corrupted=%v, errors=%d, orphaned=%d",
		report.IsCorrupted, report.ErrorCount, len(report.OrphanedPages))

	return report, nil
}

func (v *BTreeRecoveryValidator) validateMetadataPage(report *CorruptionReport) error {
	log.Printf("[BTreeValidator] Validating metadata page...")

	if v.btreeBackend.meta == nil {
		return fmt.Errorf("metadata page is nil")
	}


	rootID := v.btreeBackend.meta.RootPage()
	if rootID == 0 {
		errMsg := "metadata root page ID is 0 (invalid)"
		log.Printf("[BTreeValidator] Warning: %s", errMsg)
		report.ErrorLog = append(report.ErrorLog, errMsg)
		report.ErrorCount++
	}

	log.Printf("[BTreeValidator] Metadata page valid: root=%d", rootID)
	return nil
}

func (v *BTreeRecoveryValidator) validateTreeStructure(report *CorruptionReport) error {
	log.Printf("[BTreeValidator] Validating tree structure from root...")

	rootID := v.btreeBackend.meta.RootPage()
	if rootID == 0 {
		return fmt.Errorf("cannot validate tree: invalid root ID")
	}


	log.Printf("[BTreeValidator] Tree structure validation deferred to Phase 3 implementation")
	report.ErrorLog = append(report.ErrorLog, "Tree structure validation not yet implemented (Phase 3)")

	return nil
}

func (v *BTreeRecoveryValidator) findOrphanedPages(report *CorruptionReport) error {
	log.Printf("[BTreeValidator] Scanning for orphaned pages...")


	log.Printf("[BTreeValidator] Orphaned page detection deferred to Phase 3 implementation")
	report.ErrorLog = append(report.ErrorLog, "Orphaned page detection not yet implemented (Phase 3)")

	return nil
}

func (v *BTreeRecoveryValidator) RepairStructure(report *CorruptionReport) error {
	if !report.IsCorrupted {
		log.Printf("[BTreeValidator] No corruption detected, skipping repair")
		return nil
	}

	log.Printf("[BTreeValidator] Attempting to repair B+Tree structure...")
	log.Printf("[BTreeValidator] Found: %d corrupted pages, %d orphaned pages, %d broken chains",
		len(report.CorruptedPages), len(report.OrphanedPages), len(report.BrokenOverflowChains))


	log.Printf("[BTreeValidator] Repair deferred to Phase 3 implementation")

	return nil
}

func (v *BTreeRecoveryValidator) VerifyRecovery() error {
	log.Printf("[BTreeValidator] Verifying recovery success...")

	report, err := v.ValidateStructure()
	if err != nil {
		return fmt.Errorf("recovery verification failed: %w", err)
	}

	if report.IsCorrupted {
		return fmt.Errorf("recovery incomplete: still have %d corrupted pages and %d orphaned pages",
			len(report.CorruptedPages), len(report.OrphanedPages))
	}

	log.Printf("[BTreeValidator] Recovery verification passed")
	return nil
}

func (r *CorruptionReport) GetOrphanedPageCount() int {
	return len(r.OrphanedPages)
}

func (r *CorruptionReport) GetCorruptedPageCount() int {
	return len(r.CorruptedPages)
}

func (r *CorruptionReport) GetBrokenChainCount() int {
	return len(r.BrokenOverflowChains)
}
