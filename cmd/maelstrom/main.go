package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/rodrigo0345/omag/internal/storage/btree"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/isolation"
	txnlog "github.com/rodrigo0345/omag/internal/txn/log"
)

// MaelstromMessage represents a Maelstrom protocol message
type MaelstromMessage struct {
	ID   *int           `json:"id,omitempty"`
	Src  string         `json:"src"`
	Dest string         `json:"dest"`
	Body map[string]any `json:"body"`
}

// Node represents the Maelstrom node
type Node struct {
	nodeID       string
	msgID        int
	msgIDMu      sync.Mutex
	isolationMgr txn.IIsolationManager
	logger       *log.Logger
}

// NewNode creates a new Maelstrom node
func NewNode() (*Node, error) {
	// Initialize disk manager
	dm, err := buffer.NewDiskManager("maelstrom.wal")
	if err != nil {
		return nil, fmt.Errorf("failed to create disk manager: %w", err)
	}

	// Initialize buffer pool manager
	bpm := buffer.NewBufferPoolManager(128, dm)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffer pool manager: %w", err)
	}

	// Initialize BTree storage backend
	storageEngine, err := btree.NewBPlusTreeBackend(bpm, dm)
	if err != nil {
		return nil, fmt.Errorf("failed to create BTree backend: %w", err)
	}

	// Initialize WAL (log) manager
	logManager, err := txnlog.NewWALManager("transaction.wal")
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL manager: %w", err)
	}

	// Initialize rollback manager
	rollbackManager := txn.NewRollbackManager(bpm)

	// Initialize write handler
	writeHandler := txn.NewDefaultWriteHandler(storageEngine, rollbackManager, bpm, logManager)

	// Initialize 2PL isolation manager
	isolationMgr := isolation.NewTwoPhaseLockingManager(
		logManager,
		bpm,
		writeHandler,
		rollbackManager,
		storageEngine,
	)

	return &Node{
		msgID:        0,
		isolationMgr: isolationMgr,
		logger:       log.New(os.Stderr, "[maelstrom] ", log.LstdFlags),
	}, nil
}

// Start begins listening for messages
func (n *Node) Start() error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var msg MaelstromMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			n.logger.Printf("failed to parse message: %v", err)
			continue
		}

		// Handle initialization
		if msg.Body["type"] == "init" {
			n.handleInit(msg)
			continue
		}

		// Handle transaction operations
		if opType, ok := msg.Body["type"].(string); ok {
			go n.handleOperation(msg, opType)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}
	return nil
}

// handleInit handles the Maelstrom init message
func (n *Node) handleInit(msg MaelstromMessage) {
	nodeID, _ := msg.Body["node_id"].(string)
	n.nodeID = nodeID

	// Send init acknowledgement
	response := MaelstromMessage{
		ID:   msg.ID,
		Src:  n.nodeID,
		Dest: msg.Src,
		Body: map[string]any{
			"type": "init_ok",
		},
	}
	n.send(response)
}

// handleOperation dispatches transaction operations
func (n *Node) handleOperation(msg MaelstromMessage, opType string) {
	var response MaelstromMessage
	var err error

	switch opType {
	case "begin_txn":
		response, err = n.handleBeginTxn(msg)
	case "read":
		response, err = n.handleRead(msg)
	case "write":
		response, err = n.handleWrite(msg)
	case "commit":
		response, err = n.handleCommit(msg)
	case "abort":
		response, err = n.handleAbort(msg)
	default:
		err = fmt.Errorf("unknown operation type: %s", opType)
	}

	if err != nil {
		response = n.errorResponse(msg, err)
	}

	n.send(response)
}

// handleBeginTxn starts a new transaction
func (n *Node) handleBeginTxn(msg MaelstromMessage) (MaelstromMessage, error) {
	isolationLevel := uint8(txn.SERIALIZABLE) // Default to SERIALIZABLE
	if level, ok := msg.Body["isolation_level"].(float64); ok {
		isolationLevel = uint8(level)
	}

	txnID := n.isolationMgr.BeginTransaction(isolationLevel)

	return MaelstromMessage{
		ID:   msg.ID,
		Src:  n.nodeID,
		Dest: msg.Src,
		Body: map[string]any{
			"type":   "begin_txn_ok",
			"txn_id": txnID,
		},
	}, nil
}

// handleRead reads a value within a transaction
func (n *Node) handleRead(msg MaelstromMessage) (MaelstromMessage, error) {
	txnID, ok := msg.Body["txn_id"].(float64)
	if !ok {
		return MaelstromMessage{}, fmt.Errorf("missing txn_id")
	}

	key, ok := msg.Body["key"].(string)
	if !ok {
		return MaelstromMessage{}, fmt.Errorf("missing key")
	}

	value, err := n.isolationMgr.Read(int64(txnID), []byte(key))
	if err != nil {
		return MaelstromMessage{}, fmt.Errorf("read failed: %w", err)
	}

	var valueParsed any = nil
	if value != nil {
		// Try to parse as JSON first, otherwise return as string
		var parsed any
		if err := json.Unmarshal(value, &parsed); err == nil {
			valueParsed = parsed
		} else {
			valueParsed = string(value)
		}
	}

	return MaelstromMessage{
		ID:   msg.ID,
		Src:  n.nodeID,
		Dest: msg.Src,
		Body: map[string]any{
			"type":  "read_ok",
			"value": valueParsed,
		},
	}, nil
}

// handleWrite writes a value within a transaction
func (n *Node) handleWrite(msg MaelstromMessage) (MaelstromMessage, error) {
	txnID, ok := msg.Body["txn_id"].(float64)
	if !ok {
		return MaelstromMessage{}, fmt.Errorf("missing txn_id")
	}

	key, ok := msg.Body["key"].(string)
	if !ok {
		return MaelstromMessage{}, fmt.Errorf("missing key")
	}

	value, ok := msg.Body["value"]
	if !ok {
		return MaelstromMessage{}, fmt.Errorf("missing value")
	}

	// Serialize value to JSON bytes
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return MaelstromMessage{}, fmt.Errorf("failed to serialize value: %w", err)
	}

	err = n.isolationMgr.Write(int64(txnID), []byte(key), valueBytes)
	if err != nil {
		return MaelstromMessage{}, fmt.Errorf("write failed: %w", err)
	}

	return MaelstromMessage{
		ID:   msg.ID,
		Src:  n.nodeID,
		Dest: msg.Src,
		Body: map[string]any{
			"type": "write_ok",
		},
	}, nil
}

// handleCommit commits a transaction
func (n *Node) handleCommit(msg MaelstromMessage) (MaelstromMessage, error) {
	txnID, ok := msg.Body["txn_id"].(float64)
	if !ok {
		return MaelstromMessage{}, fmt.Errorf("missing txn_id")
	}

	err := n.isolationMgr.Commit(int64(txnID))
	if err != nil {
		return MaelstromMessage{}, fmt.Errorf("commit failed: %w", err)
	}

	return MaelstromMessage{
		ID:   msg.ID,
		Src:  n.nodeID,
		Dest: msg.Src,
		Body: map[string]any{
			"type": "commit_ok",
		},
	}, nil
}

// handleAbort aborts a transaction
func (n *Node) handleAbort(msg MaelstromMessage) (MaelstromMessage, error) {
	txnID, ok := msg.Body["txn_id"].(float64)
	if !ok {
		return MaelstromMessage{}, fmt.Errorf("missing txn_id")
	}

	err := n.isolationMgr.Abort(int64(txnID))
	if err != nil {
		return MaelstromMessage{}, fmt.Errorf("abort failed: %w", err)
	}

	return MaelstromMessage{
		ID:   msg.ID,
		Src:  n.nodeID,
		Dest: msg.Src,
		Body: map[string]any{
			"type": "abort_ok",
		},
	}, nil
}

// errorResponse creates an error response message
func (n *Node) errorResponse(msg MaelstromMessage, err error) MaelstromMessage {
	return MaelstromMessage{
		ID:   msg.ID,
		Src:  n.nodeID,
		Dest: msg.Src,
		Body: map[string]any{
			"type":  "error",
			"error": err.Error(),
		},
	}
}

// send sends a message to stdout
func (n *Node) send(msg MaelstromMessage) {
	n.msgIDMu.Lock()
	n.msgID++
	id := n.msgID
	n.msgIDMu.Unlock()

	msg.ID = &id
	data, err := json.Marshal(msg)
	if err != nil {
		n.logger.Printf("failed to marshal message: %v", err)
		return
	}

	fmt.Println(string(data))
}

func main() {
	node, err := NewNode()
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		log.Fatalf("node error: %v", err)
	}
}
