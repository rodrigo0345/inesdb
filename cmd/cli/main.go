package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	btree "github.com/rodrigo0345/omag/b_tree"
	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/logmanager"
	"github.com/rodrigo0345/omag/resource_page"
	"github.com/rodrigo0345/omag/transaction_manager"
)

const (
	dbPath  = "./test.db"
	walPath = "./test.wal"
)

type DatabaseTUI struct {
	tree          *btree.BTree
	txnMgr        *transaction_manager.TransactionManager
	bufferPool    *buffermanager.BufferPoolManager
	diskMgr       *buffermanager.DiskManager
	walMgr        logmanager.ILogManager
	lockMgr       *transaction_manager.LockManager
	statsInserts  int64
	statsDeletes  int64
	statsSearches int64
}

func NewDatabaseTUI() (*DatabaseTUI, error) {
	// Clean up old files if they exist
	// os.Remove(dbPath)
	// os.Remove(walPath)

	// Create disk manager
	diskMgr, err := buffermanager.NewDiskManager(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk manager: %w", err)
	}

	// Create buffer pool manager (50 frames for interactive use)
	bufferPool := buffermanager.NewBufferPoolManager(50, diskMgr)

	// Create lock manager
	lockMgr := transaction_manager.NewLockManager()

	// Create WAL manager
	walMgr, err := logmanager.NewWALManager(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL manager: %w", err)
	}

	// Allocate first page for the disk (meta)
	diskMgr.AllocatePage()

	// CRITICAL: Perform crash recovery if WAL file exists
	// This ensures the database is restored to a consistent state
	recoveryState, err := walMgr.Recover()
	if err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	// Apply recovered pages to disk to ensure no data loss
	if len(recoveryState.PageStates) > 0 {
		// Find the maximum page ID we need to recover
		maxPageID := uint32(0)
		for pageID := range recoveryState.PageStates {
			if uint32(pageID) > maxPageID {
				maxPageID = uint32(pageID)
			}
		}

		// Allocate pages sequentially until we cover all recovered page IDs
		// AllocatePage() returns the pageID that was allocated
		for {
			allocatedPageID := diskMgr.AllocatePage()
			if uint32(allocatedPageID) > maxPageID {
				break
			}
		}

		// Now write the recovered pages back to disk
		for pageID, pageData := range recoveryState.PageStates {
			// Pad page data to PageSize (4096 bytes)
			paddedData := make([]byte, 4096)
			copy(paddedData, pageData)

			if err := diskMgr.WritePage(resource_page.ResourcePageID(pageID), paddedData); err != nil {
				return nil, fmt.Errorf("failed to write recovered page %d: %w", pageID, err)
			}
		}

		fmt.Printf("Recovery: Restored %d pages, Committed Txns: %d, Aborted Txns: %d\n",
			len(recoveryState.PageStates),
			len(recoveryState.CommittedTxns),
			len(recoveryState.AbortedTxns))
	}

	// Create BTree
	tree, err := btree.NewBTree(bufferPool, lockMgr, walMgr, 4096)
	if err != nil {
		return nil, fmt.Errorf("failed to create BTree: %w", err)
	}

	// Create transaction manager
	txnMgr := transaction_manager.NewTransactionManager(walMgr)

	// CRITICAL: Set the buffer pool on transaction manager so commits flush pages to disk
	txnMgr.SetBufferPool(bufferPool)

	tui := &DatabaseTUI{
		tree:       tree,
		txnMgr:     txnMgr,
		bufferPool: bufferPool,
		diskMgr:    diskMgr,
		walMgr:     walMgr,
		lockMgr:    lockMgr,
	}

	return tui, nil
}

func (db *DatabaseTUI) Close() error {
	fmt.Println("\nClosing database and creating final checkpoint...")

	// Flush all dirty pages one final time
	if err := db.bufferPool.FlushAll(); err != nil {
		return err
	}

	// Sync disk to ensure everything is persisted
	if err := db.diskMgr.Sync(); err != nil {
		return err
	}

	// Create a final checkpoint to enable fast recovery on next startup
	// This resets the dirty page table and writes checkpoint metadata to the WAL
	if err := db.walMgr.Checkpoint(); err != nil {
		fmt.Printf("Warning: Failed to create final checkpoint: %v\n", err)
		// Don't fail on checkpoint - continue with close
	}

	// Close disk manager
	if err := db.diskMgr.Close(); err != nil {
		return err
	}

	// Close WAL manager
	if err := db.walMgr.Close(); err != nil {
		return err
	}

	return nil
}

func (db *DatabaseTUI) printWelcome() {
	clearScreen()
	fmt.Println("\n" + colorBold(colorCyan("╔════════════════════════════════════════════════════════════╗")))
	fmt.Println(colorBold(colorCyan("║")) + "          " + colorBold(colorMagenta("Database TUI")) + " - B+Tree Testing Interface           " + colorBold(colorCyan("║")))
	fmt.Println(colorBold(colorCyan("╚════════════════════════════════════════════════════════════╝")))
	fmt.Println()
	fmt.Println(colorBold("Available Commands:"))
	fmt.Println(colorGreen("  [i]") + " Insert     - Add a new key-value pair")
	fmt.Println(colorGreen("  [d]") + " Delete     - Remove a key")
	fmt.Println(colorGreen("  [s]") + " Search     - Find a value by key")
	fmt.Println(colorGreen("  [l]") + " List       - Display all entries")
	fmt.Println(colorGreen("  [c]") + " Count      - Count total entries")
	fmt.Println(colorGreen("  [t]") + " Stats      - Show database statistics")
	fmt.Println(colorGreen("  [w]") + " WAL List    - Inspect the Write-Ahead Log")
	fmt.Println(colorGreen("  [p]") + " Checkpoint - Create WAL checkpoint for fast recovery")
	fmt.Println(colorGreen("  [h]") + " Help       - Show this help message")
	fmt.Println(colorYellow("  [q]") + " Quit       - Exit the program")
	fmt.Println()
}

// ANSI color codes
func colorBold(s string) string {
	return "\033[1m" + s + "\033[0m"
}

func colorCyan(s string) string {
	return "\033[36m" + s + "\033[0m"
}

func colorMagenta(s string) string {
	return "\033[35m" + s + "\033[0m"
}

func colorGreen(s string) string {
	return "\033[32m" + s + "\033[0m"
}

func colorRed(s string) string {
	return "\033[31m" + s + "\033[0m"
}

func colorYellow(s string) string {
	return "\033[33m" + s + "\033[0m"
}

func (db *DatabaseTUI) insert(key, value string) error {
	txn := db.txnMgr.Begin()
	defer func() {
		if r := recover(); r != nil {
			db.txnMgr.Abort(txn)
		}
	}()

	err := db.tree.Insert(txn, []byte(key), []byte(value))
	if err != nil {
		db.txnMgr.Abort(txn)
		return err
	}

	db.txnMgr.Commit(txn)
	db.statsInserts++
	fmt.Printf("%s Inserted: %s → %s\n", colorGreen("✓"), colorBold(key), value)
	return nil
}

func (db *DatabaseTUI) delete(key string) error {
	txn := db.txnMgr.Begin()
	defer func() {
		if r := recover(); r != nil {
			db.txnMgr.Abort(txn)
		}
	}()

	err := db.tree.Delete(txn, []byte(key))
	if err != nil {
		db.txnMgr.Abort(txn)
		return err
	}

	db.txnMgr.Commit(txn)
	db.statsDeletes++
	fmt.Printf("%s Deleted: %s\n", colorGreen("✓"), colorBold(key))
	return nil
}

func (db *DatabaseTUI) search(key string) error {
	txn := db.txnMgr.Begin()
	defer func() {
		if r := recover(); r != nil {
			db.txnMgr.Abort(txn)
		}
	}()

	value, err := db.tree.Find(txn, []byte(key))
	db.txnMgr.Commit(txn)
	db.statsSearches++

	if err == btree.ErrKeyNotFound {
		fmt.Printf("%s Key not found: %s\n", colorRed("✗"), colorBold(key))
		return nil
	}
	if err != nil {
		return err
	}

	fmt.Printf("%s Found: %s → %s\n", colorGreen("✓"), colorBold(key), string(value))
	return nil
}

func (db *DatabaseTUI) list() error {
	txn := db.txnMgr.Begin()
	defer func() {
		if r := recover(); r != nil {
			db.txnMgr.Abort(txn)
		}
	}()

	cursor, err := db.tree.Cursor()
	if err != nil {
		db.txnMgr.Commit(txn)
		return err
	}

	err = cursor.First()
	if err != nil {
		db.txnMgr.Commit(txn)
		return err
	}

	count := 0
	fmt.Println("\n" + colorBold(colorCyan("+─────────────────────────────────────────────────────────+")))
	fmt.Println(colorBold(colorCyan("| Key")) + "                              " + colorBold(colorCyan("| Value")) + "               " + colorBold(colorCyan("|")))
	fmt.Println(colorBold(colorCyan("+─────────────────────────────────────────────────────────+")))

	for cursor.Valid() {
		key := string(cursor.Key())
		value := string(cursor.Value())
		if len(key) > 31 {
			key = key[:28] + "..."
		}
		if len(value) > 19 {
			value = value[:16] + "..."
		}
		fmt.Printf(colorCyan("| ")+"%-32s "+colorCyan("| ")+"%-19s "+colorCyan("|\n"), key, value)
		count++
		cursor.Next()
	}

	fmt.Println(colorBold(colorCyan("+─────────────────────────────────────────────────────────+")))
	fmt.Printf("Total entries: %s\n", colorBold(fmt.Sprintf("%d", count)))

	db.txnMgr.Commit(txn)
	return nil
}

func (db *DatabaseTUI) count() (int, error) {
	txn := db.txnMgr.Begin()
	defer func() {
		if r := recover(); r != nil {
			db.txnMgr.Abort(txn)
		}
	}()

	cursor, err := db.tree.Cursor()
	if err != nil {
		db.txnMgr.Commit(txn)
		return 0, err
	}

	err = cursor.First()
	if err != nil {
		db.txnMgr.Commit(txn)
		return 0, err
	}

	count := 0
	for cursor.Valid() {
		count++
		cursor.Next()
	}

	db.txnMgr.Commit(txn)
	return count, nil
}

func (db *DatabaseTUI) listWAL() error {
	records, err := db.walMgr.ReadAllRecords()
	if err != nil {
		return err
	}

	fmt.Println("\n" + colorBold(colorCyan("╔════════════════════════════════════════════════════════════╗")))
	fmt.Println(colorBold(colorCyan("║")) + "                 " + colorBold(colorMagenta("WAL (Write-Ahead Log) Contents")) + "               " + colorBold(colorCyan("║")))
	fmt.Println(colorBold(colorCyan("╠══════╤══════╤════════╤════════╤════════════════════════════╣")))
	fmt.Println(colorBold(colorCyan("║ LSN  │ TXID │ TYPE   │ PAGE   │ DATA PREVIEW               ║")))
	fmt.Println(colorBold(colorCyan("╟──────┼──────┼────────┼────────┼────────────────────────────╢")))

	for _, r := range records {
		typeName := ""
		switch r.Type {
		case logmanager.UPDATE:
			typeName = "UPDATE"
		case logmanager.COMMIT:
			typeName = "COMMIT"
		case logmanager.ABORT:
			typeName = "ABORT"
		case logmanager.CHECKPOINT:
			typeName = "CHKPT"
		}

		preview := ""
		if len(r.After) > 0 {
			preview = string(r.After)
			if len(preview) > 20 {
				preview = preview[:17] + "..."
			}
		}

		fmt.Printf(colorCyan("║ ")+"%-4d "+colorCyan("│ ")+"%-4d "+colorCyan("│ ")+"%-6s "+colorCyan("│ ")+"%-6d "+colorCyan("│ ")+"%-26s "+colorCyan("║\n"),
			r.LSN, r.TxnID, typeName, r.PageID, preview)
	}

	fmt.Println(colorBold(colorCyan("╚══════╧══════╧════════╧════════╧════════════════════════════╝")))
	return nil
}

func (db *DatabaseTUI) createCheckpoint() error {
	// Flush all dirty pages to disk before creating checkpoint
	if err := db.bufferPool.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush buffer pool: %w", err)
	}

	// Sync disk to ensure all pages are on disk
	if err := db.diskMgr.Sync(); err != nil {
		return fmt.Errorf("failed to sync disk: %w", err)
	}

	// Create checkpoint in WAL
	// This writes a CHECKPOINT record and resets the dirty page table
	// On recovery, the database can start from this checkpoint instead of the log beginning
	if err := db.walMgr.Checkpoint(); err != nil {
		return fmt.Errorf("failed to create checkpoint: %w", err)
	}

	return nil
}

func (db *DatabaseTUI) stats() error {
	count, err := db.count()
	if err != nil {
		return err
	}

	fileInfo, err := os.Stat(dbPath)
	var fileSize int64
	if err == nil {
		fileSize = fileInfo.Size()
	}

	walInfo, err := os.Stat(walPath)
	var walSize int64
	if err == nil {
		walSize = walInfo.Size()
	}

	fmt.Println("\n" + colorBold(colorCyan("╔════════════════════════════════════════════════════════════╗")))
	fmt.Println(colorBold(colorCyan("║")) + "                " + colorBold(colorMagenta("Database Statistics")) + "                  " + colorBold(colorCyan("║")))
	fmt.Println(colorBold(colorCyan("╠════════════════════════════════════════════════════════════╣")))
	fmt.Printf("%s %-56s %s\n", colorBold(colorCyan("║")), "", colorBold(colorCyan("║")))
	fmt.Printf("%s Total Entries:        %-44s %s\n", colorBold(colorCyan("║")), colorBold(fmt.Sprintf("%d", count)), colorBold(colorCyan("║")))
	fmt.Printf("%s Database File Size:   %-43s %s\n", colorBold(colorCyan("║")), colorBold(fmt.Sprintf("%d B", fileSize)), colorBold(colorCyan("║")))
	fmt.Printf("%s WAL File Size:        %-43s %s\n", colorBold(colorCyan("║")), colorBold(fmt.Sprintf("%d B", walSize)), colorBold(colorCyan("║")))
	fmt.Printf("%s %-56s %s\n", colorBold(colorCyan("║")), "", colorBold(colorCyan("║")))
	fmt.Printf("%s Total Inserts:        %-44s %s\n", colorBold(colorCyan("║")), colorGreen(fmt.Sprintf("%d", db.statsInserts)), colorBold(colorCyan("║")))
	fmt.Printf("%s Total Deletes:        %-44s %s\n", colorBold(colorCyan("║")), colorRed(fmt.Sprintf("%d", db.statsDeletes)), colorBold(colorCyan("║")))
	fmt.Printf("%s Total Searches:       %-44s %s\n", colorBold(colorCyan("║")), fmt.Sprintf("%d", db.statsSearches), colorBold(colorCyan("║")))
	fmt.Printf("%s %-56s %s\n", colorBold(colorCyan("║")), "", colorBold(colorCyan("║")))
	fmt.Printf("%s Buffer Pool Frames:   %-44s %s\n", colorBold(colorCyan("║")), "50", colorBold(colorCyan("║")))
	fmt.Printf("%s Page Size:            %-43s %s\n", colorBold(colorCyan("║")), "4096 B", colorBold(colorCyan("║")))
	fmt.Println(colorBold(colorCyan("╚════════════════════════════════════════════════════════════╝")))

	return nil
}

func clearScreen() {
	fmt.Print("\033[H\033[2J")
}

func (db *DatabaseTUI) Run() {
	reader := bufio.NewReader(os.Stdin)

	db.printWelcome()

	for {
		fmt.Print(colorBold(colorMagenta("> ")))
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		cmd := strings.ToLower(string(input[0]))

		switch cmd {
		case "i":
			fmt.Print(colorCyan("Key: "))
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			fmt.Print(colorCyan("Value: "))
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)

			if key != "" && val != "" {
				if err := db.insert(key, val); err != nil {
					fmt.Printf("%s Error: %v\n", colorRed("✗"), err)
				}
			} else {
				fmt.Printf("%s Key and value cannot be empty\n", colorRed("✗"))
			}

		case "d":
			fmt.Print(colorCyan("Key to delete: "))
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			if key != "" {
				if err := db.delete(key); err != nil {
					if err == btree.ErrKeyNotFound {
						fmt.Printf("%s Key not found\n", colorRed("✗"))
					} else {
						fmt.Printf("%s Error: %v\n", colorRed("✗"), err)
					}
				}
			} else {
				fmt.Printf("%s Key cannot be empty\n", colorRed("✗"))
			}

		case "s":
			fmt.Print(colorCyan("Key to search: "))
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			if key != "" {
				if err := db.search(key); err != nil {
					fmt.Printf("%s Error: %v\n", colorRed("✗"), err)
				}
			} else {
				fmt.Printf("%s Key cannot be empty\n", colorRed("✗"))
			}

		case "l":
			if err := db.list(); err != nil {
				fmt.Printf("%s Error: %v\n", colorRed("✗"), err)
			}

		case "c":
			if count, err := db.count(); err != nil {
				fmt.Printf("%s Error: %v\n", colorRed("✗"), err)
			} else {
				fmt.Printf("Total entries: %s\n", colorBold(fmt.Sprintf("%d", count)))
			}

		case "t":
			if err := db.stats(); err != nil {
				fmt.Printf("%s Error: %v\n", colorRed("✗"), err)
			}

		case "w":
			if err := db.listWAL(); err != nil {
				fmt.Printf("%s Error reading WAL: %v\n", colorRed("✗"), err)
			}

		case "p":
			if err := db.createCheckpoint(); err != nil {
				fmt.Printf("%s Error creating checkpoint: %v\n", colorRed("✗"), err)
			} else {
				fmt.Printf("%s Checkpoint created successfully\n", colorGreen("✓"))
			}

		case "h":
			clearScreen()
			db.printWelcome()

		case "q":
			fmt.Println("\n" + colorYellow("💾 Syncing and shutting down..."))
			if err := db.Close(); err != nil {
				fmt.Printf("%s Error during shutdown: %v\n", colorRed("✗"), err)
			}
			fmt.Println(colorGreen("✓ Goodbye!"))
			time.Sleep(500 * time.Millisecond)
			return

		default:
			fmt.Printf("%s Unknown command. Type '%s' for help.\n", colorRed("✗"), colorGreen("h"))
		}

		fmt.Println()
	}
}

func main() {
	db, err := NewDatabaseTUI()
	if err != nil {
		fmt.Printf("Failed to initialize database: %v\n", err)
		os.Exit(1)
	}

	db.Run()
}
