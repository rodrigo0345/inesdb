package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

// MaelstromMessage represents a Maelstrom protocol message
type MaelstromMessage struct {
	Src  string         `json:"src"`
	Dest string         `json:"dest"`
	Body map[string]any `json:"body"`
}

// Node represents the Maelstrom node
type Node struct {
	nodeID  string
	msgID   int
	msgIDMu sync.Mutex

	db       database.Database
	txnManager txn.IIsolationManager
}

// NewNode creates a new Maelstrom node
func NewNode() *Node {
	return &Node{
		msgID: 0,
	}
}

// Start begins listening for messages
func (n *Node) Start() error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var msg MaelstromMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}

		msgType, ok := msg.Body["type"].(string)
		if !ok {
			continue
		}

		if msgType == "init" {

			engine, err := database.OpenMVCCLSM(database.Options{
				DBPath:     "./test.db",
				LSMDataDir: "./lsm_data",
				WALPath:    "./test.wal",
			})
			if err != nil {
				continue
			}
			n.db = engine
			n.txnManager = engine.IsolationManager()

			nodeID, _ := msg.Body["node_id"].(string)
			n.nodeID = nodeID
			response := MaelstromMessage{
				Src:  n.nodeID,
				Dest: msg.Src,
				Body: map[string]any{
					"type":        "init_ok",
					"in_reply_to": msg.Body["msg_id"],
				},
			}
			n.send(response)
		} else if msgType == "txn" {
			txnRaw, ok := msg.Body["txn"].([]any)
			if !ok {
				continue
			}

			// Execute transaction
			resultTxn := n.executeTxn(txnRaw)

			response := MaelstromMessage{
				Src:  n.nodeID,
				Dest: msg.Src,
				Body: map[string]any{
					"type":        "txn_ok",
					"in_reply_to": msg.Body["msg_id"],
					"txn":         resultTxn,
				},
			}
			n.send(response)
		}
	}

	return scanner.Err()
}

func (n *Node) executeTxn(txnOps []any) []any {
	results := make([]any, 0, len(txnOps))

	if n.db == nil {
		return results
	}

	txnID := n.db.BeginTransaction(txn_unit.SERIALIZABLE, "", nil)
	committed := false
	defer func() {
		if !committed {
			_ = n.db.Abort(txnID)
		}
	}()

	for _, opAny := range txnOps {
		op, ok := opAny.([]any)
		if !ok || len(op) < 3 {
			continue
		}

		f, ok := op[0].(string)
		if !ok {
			continue
		}

		k := fmt.Sprint(op[1])
		v := op[2]

		switch f {
		case "r":
			val, err := n.db.Read(txnID, []byte(k))
			if err != nil {
				results = append(results, []any{"r", op[1], nil})
				continue
			}

			var decoded any
			if err := json.Unmarshal(val, &decoded); err != nil {
				results = append(results, []any{"r", op[1], nil})
				continue
			}
			results = append(results, []any{"r", op[1], decoded})
		case "w":
			byteData, err := json.Marshal(v)
			if err != nil {
				results = append(results, []any{"w", op[1], nil})
				continue
			}
			err = n.db.Write(txnID, []byte(k), byteData)
			if err != nil {
				results = append(results, []any{"w", op[1], nil})
				continue
			}
			results = append(results, []any{"w", op[1], v})
		default:
			results = append(results, []any{f, op[1], nil})
		}
	}
	if err := n.db.Commit(txnID); err != nil {
		return []any{}
	}
	committed = true

	return results
}

// send sends a message to stdout
func (n *Node) send(msg MaelstromMessage) {
	n.msgIDMu.Lock()
	n.msgID++
	id := n.msgID
	n.msgIDMu.Unlock()

	if msg.Body == nil {
		msg.Body = make(map[string]any)
	}
	msg.Body["msg_id"] = id

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	fmt.Println(string(data))
	os.Stdout.Sync()
}

func main() {
	node := NewNode()
	node.Start()
}
