package transaction_manager

type TxnState int

const (
	ACTIVE TxnState = iota
	COMMITTED
	ABORTED
)

type UndoEntry []byte // Placeholder for an actual undo log entry structure

type Transaction struct {
	txnID          uint64
	state          TxnState
	sharedLocks    [][]byte
	exclusiveLocks [][]byte
	undoLog        []UndoEntry
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
