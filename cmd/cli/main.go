package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/rodrigo0345/omag/internal/storage/btree"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/isolation"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

const (
	dbPath  = "./test.db"
	walPath = "./test.wal"
)

// DatabaseTUI represents the database with 2PL isolation
type DatabaseTUI struct {
	storageEngine txn.StorageEngine     // BPlusTreeBackend
	isolationMgr  txn.IIsolationManager // 2PL isolation
	bufferPool    *buffer.BufferPoolManager
	diskMgr       *buffer.DiskManager
	walMgr        log.ILogManager
}

// NewDatabaseTUI initializes database with 2PL + BTree
func NewDatabaseTUI() (*DatabaseTUI, error) {
	// Disk & Buffer
	diskMgr, err := buffer.NewDiskManager(dbPath)
	if err != nil {
		return nil, fmt.Errorf("disk manager: %w", err)
	}

	bufferPool := buffer.NewBufferPoolManager(50, diskMgr)

	// WAL
	walMgr, err := log.NewWALManager(walPath)
	if err != nil {
		return nil, fmt.Errorf("WAL manager: %w", err)
	}

	// Storage Engine (pure - no transaction awareness)
	storageEngine, err := btree.NewBPlusTreeBackend(bufferPool, diskMgr)
	if err != nil {
		return nil, fmt.Errorf("storage engine: %w", err)
	}

	// Write Coordination
	rollbackMgr := txn.NewRollbackManager(bufferPool)
	writeHandler := txn.NewDefaultWriteHandler(
		storageEngine,
		rollbackMgr,
		bufferPool,
		walMgr,
	)

	// 2PL Isolation Manager
	isolationMgr := isolation.NewTwoPhaseLockingManager(
		walMgr,
		bufferPool,
		writeHandler,
		rollbackMgr,
		storageEngine,
	)

	return &DatabaseTUI{
		storageEngine: storageEngine,
		isolationMgr:  isolationMgr,
		bufferPool:    bufferPool,
		diskMgr:       diskMgr,
		walMgr:        walMgr,
	}, nil
}

func (db *DatabaseTUI) Close() error {
	// Save metadata before closing
	if engine, ok := db.storageEngine.(*btree.BPlusTreeBackend); ok {
		if err := engine.SaveMetadataToDisk(); err != nil {
			fmt.Printf("Warning: SaveMetadataToDisk: %v\n", err)
		}
	}

	if err := db.isolationMgr.Close(); err != nil {
		fmt.Printf("Warning: isolationMgr.Close: %v\n", err)
	}
	if err := db.bufferPool.FlushAll(); err != nil {
		fmt.Printf("Warning: bufferPool.FlushAll: %v\n", err)
	}
	if err := db.diskMgr.Sync(); err != nil {
		fmt.Printf("Warning: diskMgr.Sync: %v\n", err)
	}
	if err := db.walMgr.Close(); err != nil {
		fmt.Printf("Warning: walMgr.Close: %v\n", err)
	}
	if err := db.diskMgr.Close(); err != nil {
		fmt.Printf("Warning: diskMgr.Close: %v\n", err)
	}
	return nil
}

// Set key=value
func (db *DatabaseTUI) set(key, value string) error {
	txnID := db.isolationMgr.BeginTransaction(txn.SERIALIZABLE)
	if err := db.isolationMgr.Write(txnID, []byte(key), []byte(value)); err != nil {
		db.isolationMgr.Abort(txnID)
		return fmt.Errorf("write failed: %w", err)
	}
	if err := db.isolationMgr.Commit(txnID); err != nil {
		db.isolationMgr.Abort(txnID)
		return fmt.Errorf("commit failed: %w", err)
	}
	fmt.Printf("✓ Set %s = %s\n", key, value)
	return nil
}

// Get key
func (db *DatabaseTUI) get(key string) error {
	txnID := db.isolationMgr.BeginTransaction(txn.READ_COMMITTED)
	defer db.isolationMgr.Commit(txnID)

	value, err := db.isolationMgr.Read(txnID, []byte(key))
	if err != nil {
		return fmt.Errorf("read failed: %w", err)
	}
	if value == nil || len(value) == 0 {
		fmt.Printf("✗ Key not found: %s\n", key)
		return nil
	}
	fmt.Printf("✓ %s = %s\n", key, string(value))
	return nil
}

// Delete key
func (db *DatabaseTUI) del(key string) error {
	txnID := db.isolationMgr.BeginTransaction(txn.SERIALIZABLE)
	if err := db.isolationMgr.Write(txnID, []byte(key), []byte("")); err != nil {
		db.isolationMgr.Abort(txnID)
		return fmt.Errorf("delete failed: %w", err)
	}
	if err := db.isolationMgr.Commit(txnID); err != nil {
		db.isolationMgr.Abort(txnID)
		return fmt.Errorf("commit failed: %w", err)
	}
	fmt.Printf("✓ Deleted %s\n", key)
	return nil
}

// List all key-value pairs
func (db *DatabaseTUI) list() error {
	engine, ok := db.storageEngine.(*btree.BPlusTreeBackend)
	if !ok {
		return fmt.Errorf("storage engine is not BPlusTreeBackend")
	}

	results, err := engine.Scan()
	if err != nil {
		return fmt.Errorf("scan failed: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("Database is empty")
		return nil
	}

	fmt.Printf("=== All Keys (%d entries) ===\n", len(results))
	for i, entry := range results {
		fmt.Printf("%d. %s = %s\n", i+1, string(entry.Key), string(entry.Value))
	}
	fmt.Println()
	return nil
}

// Show database statistics
func (db *DatabaseTUI) stats() error {
	engine, ok := db.storageEngine.(*btree.BPlusTreeBackend)
	if !ok {
		return fmt.Errorf("storage engine is not BPlusTreeBackend")
	}

	fileSize, err := db.diskMgr.GetFileSize()
	if err != nil {
		return fmt.Errorf("get file size failed: %w", err)
	}

	results, err := engine.Scan()
	if err != nil {
		return fmt.Errorf("scan failed: %w", err)
	}

	fmt.Println("\n=== Database Statistics ===")
	fmt.Printf("File Size: %d bytes (%.2f MB)\n", fileSize, float64(fileSize)/1024/1024)
	fmt.Printf("Total Entries: %d\n", len(results))
	fmt.Printf("Page Size: 4096 bytes\n")
	fmt.Printf("Allocated Pages: %d\n", fileSize/4096)
	fmt.Println()
	return nil
}

func (db *DatabaseTUI) Run() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("\n=== OMAG Database (2PL + BTree) ===")
	fmt.Println("Commands: set <key> <val>, get <key>, del <key>, list, stats, exit")
	fmt.Println()

	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		cmd := parts[0]

		switch cmd {
		case "set":
			if len(parts) < 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			if err := db.set(key, value); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			if err := db.get(parts[1]); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "del":
			if len(parts) < 2 {
				fmt.Println("Usage: del <key>")
				continue
			}
			if err := db.del(parts[1]); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "list":
			if err := db.list(); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "stats":
			if err := db.stats(); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "exit", "quit":
			fmt.Println("Closing database...")
			if err := db.Close(); err != nil {
				fmt.Printf("✗ Close error: %v\n", err)
			}
			return

		default:
			fmt.Println("Unknown command. Try: set, get, del, list, stats, exit")
		}
	}
}

func main() {
	db, err := NewDatabaseTUI()
	if err != nil {
		fmt.Printf("✗ Failed to init database: %v\n", err)
		os.Exit(1)
	}
	db.Run()
}
