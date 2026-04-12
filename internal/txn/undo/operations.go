package undo

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/page"
)

type Operation interface {
	Undo(bufferMgr buffer.IBufferPoolManager) error

	GetID() uint64
}

type PageWriteOp struct {
	opID       uint64
	pageID     page.ResourcePageID
	offset     uint16
	beforeData []byte
}

func NewPageWriteOp(opID uint64, pageID page.ResourcePageID, offset uint16, beforeData []byte) *PageWriteOp {
	dataCopy := make([]byte, len(beforeData))
	copy(dataCopy, beforeData)

	return &PageWriteOp{
		opID:       opID,
		pageID:     pageID,
		offset:     offset,
		beforeData: dataCopy,
	}
}

func (op *PageWriteOp) Undo(bufferMgr buffer.IBufferPoolManager) error {
	page, err := bufferMgr.PinPage(op.pageID)
	if err != nil {
		return fmt.Errorf("failed to fetch page %d for undo: %w", op.pageID, err)
	}
	defer bufferMgr.UnpinPage(op.pageID, true)

	pageData := page.GetData()
	if op.offset+uint16(len(op.beforeData)) > uint16(len(pageData)) {
		return fmt.Errorf("undo offset %d + length %d exceeds page size %d",
			op.offset, len(op.beforeData), len(pageData))
	}

	copy(pageData[op.offset:], op.beforeData)
	return nil
}

func (op *PageWriteOp) GetID() uint64 {
	return op.opID
}

func (op *PageWriteOp) GetPageID() page.ResourcePageID {
	return op.pageID
}

func (op *PageWriteOp) GetOffset() uint16 {
	return op.offset
}

func (op *PageWriteOp) GetBeforeImage() []byte {
	dataCopy := make([]byte, len(op.beforeData))
	copy(dataCopy, op.beforeData)
	return dataCopy
}

type CompositeOp struct {
	opID       uint64
	operations []Operation
}

func NewCompositeOp(opID uint64, ops ...Operation) *CompositeOp {
	opsCopy := make([]Operation, len(ops))
	copy(opsCopy, ops)

	return &CompositeOp{
		opID:       opID,
		operations: opsCopy,
	}
}

func (cop *CompositeOp) Undo(bufferMgr buffer.IBufferPoolManager) error {
	for i := len(cop.operations) - 1; i >= 0; i-- {
		op := cop.operations[i]
		if err := op.Undo(bufferMgr); err != nil {
			return fmt.Errorf("composite op undo failed at index %d (op_id=%d): %w",
				i, op.GetID(), err)
		}
	}
	return nil
}

func (cop *CompositeOp) GetID() uint64 {
	return cop.opID
}

func (cop *CompositeOp) GetOperationCount() int {
	return len(cop.operations)
}
