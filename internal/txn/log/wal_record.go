package log

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/page"
)

type WALRecord struct {
	LSN     uint64
	PrevLSN uint64
	TxnID   uint64
	Type    RecordType
	PageID  page.ResourcePageID
	Offset  uint16
	PageLSN uint64
	Before  []byte
	After   []byte
}

func NewWALRecord(lsn, prevLSN, txnID uint64, recordType RecordType, pageID page.ResourcePageID, offset uint16, pageLSN uint64, before, after []byte) WALRecord {
	return WALRecord{
		LSN:     lsn,
		PrevLSN: prevLSN,
		TxnID:   txnID,
		Type:    recordType,
		PageID:  pageID,
		Offset:  offset,
		PageLSN: pageLSN,
		Before:  before,
		After:   after,
	}
}

func (rec WALRecord) String() string {
	return fmt.Sprintf("WALRecord{LSN: %d, PrevLSN: %d, TxnID: %d, Type: %v, PageID: %d, Offset: %d, PageLSN: %d, Before: %v, After: %v}",
		rec.LSN, rec.PrevLSN, rec.TxnID, rec.Type, rec.PageID, rec.Offset, rec.PageLSN, rec.Before, rec.After)
}

func (rec WALRecord) GetLSN() uint64 {
	return rec.LSN
}

func (rec WALRecord) GetPrevLSN() uint64 {
	return rec.PrevLSN
}

func (rec WALRecord) GetTxnID() uint64 {
	return rec.TxnID
}

func (rec WALRecord) GetType() RecordType {
	return rec.Type
}

func (rec WALRecord) GetPageID() page.ResourcePageID {
	return rec.PageID
}

func (rec WALRecord) GetOffset() uint16 {
	return rec.Offset
}

func (rec WALRecord) GetPageLSN() uint64 {
	return rec.PageLSN
}

func (rec WALRecord) GetBeforeImage() []byte {
	return rec.Before
}

func (rec WALRecord) GetAfterImage() []byte {
	return rec.After
}

func (rec *WALRecord) SetLSN(lsn uint64) {
	rec.LSN = lsn
}

func (rec *WALRecord) SetPrevLSN(prevLSN uint64) {
	rec.PrevLSN = prevLSN
}

func (rec *WALRecord) SetTxnID(txnID uint64) {
	rec.TxnID = txnID
}

func (rec *WALRecord) SetType(recordType RecordType) {
	rec.Type = recordType
}

func (rec *WALRecord) SetPageID(pageID page.ResourcePageID) {
	rec.PageID = pageID
}

func (rec *WALRecord) SetOffset(offset uint16) {
	rec.Offset = offset
}

func (rec *WALRecord) SetPageLSN(pageLSN uint64) {
	rec.PageLSN = pageLSN
}

func (rec *WALRecord) SetBeforeImage(before []byte) {
	rec.Before = before
}

func (rec *WALRecord) SetAfterImage(after []byte) {
	rec.After = after
}
