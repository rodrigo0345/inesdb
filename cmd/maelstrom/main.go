package main

import (
"bufio"
"encoding/json"
"fmt"
"os"
"sync"
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

	// Local state for transactions
	stateMu sync.Mutex
	state   map[string]any
}

// NewNode creates a new Maelstrom node
func NewNode() *Node {
	return &Node{
		msgID: 0,
		state: make(map[string]any),
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
	n.stateMu.Lock()
	defer n.stateMu.Unlock()

	var results []any

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
			// Read operation
			if val, exists := n.state[k]; exists {
				results = append(results, []any{"r", op[1], val})
			} else {
				results = append(results, []any{"r", op[1], nil})
			}
		case "w":
			// Write operation
			n.state[k] = v
			results = append(results, []any{"w", op[1], v})
		case "append":
			// Append operation (for list-append test)
			if current, exists := n.state[k]; exists {
				list, ok := current.([]any)
				if ok {
					// We must append to a new slice to avoid mutating shared array references during json serialization
					newList := make([]any, len(list), len(list)+1)
					copy(newList, list)
					newList = append(newList, v)
					n.state[k] = newList
				}
			} else {
				n.state[k] = []any{v}
			}
			results = append(results, []any{"append", op[1], v})
		}
	}

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
