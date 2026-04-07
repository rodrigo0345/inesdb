package transaction_manager

import "github.com/samborkent/uuidv7"

type TxnState int

const (
	ACTIVE TxnState = iota
	COMMITTED
	ABORTED
)

type UndoEntry []byte // Placeholder for an actual undo log entry structure

type Transaction struct {
	txnID          uuidv7.UUID
	state          TxnState
	sharedLocks    [][]byte
	exclusiveLocks [][]byte
	undoLog        []UndoEntry
	isolationLevel uint8
}

func (t *Transaction) GetID() uint64 {
	return t.txnID
}

func (t *Transaction) AddUndo(entry UndoEntry) {
	t.undoLog = append(t.undoLog, entry)
}

func (t *Transaction) Abort() {
	t.state = ABORTED
}

func (t *Transaction) GetState() TxnState {
	return t.state
}
