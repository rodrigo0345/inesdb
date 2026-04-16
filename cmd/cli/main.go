package cli

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/rodrigo0345/omag/internal/concurrency"
	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/btree"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/lsm"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/isolation"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/recovery"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	"github.com/rodrigo0345/omag/internal/txn/write_handler"
)

const (
	dbPath  = "./test.db"
	walPath = "./test.wal"
)

// RecoveryConfig controls recovery behavior
type RecoveryConfig struct {
	SkipRecovery bool // Skip recovery on startup
	RecoveryOnly bool // Perform recovery then exit (for inspection)
	ValidateOnly bool // Validate without detailed recovery
}

// DatabaseTUI represents the database with 2PL isolation
type DatabaseTUI struct {
	storageEngine  storage.IStorageEngine // BPlusTreeBackend or LSMTreeBackend
	isolationMgr   txn.IIsolationManager  // 2PL isolation
	bufferPool     buffer.IBufferPoolManager
	diskMgr        *buffer.DiskManager
	walMgr         log.ILogManager
	schemaManager  *schema.SchemaManager                    // Schema storage
	indexManagers  map[string]*schema.SecondaryIndexManager // Index managers per table
	recoveryConfig RecoveryConfig                           // Recovery configuration
}

// NewDatabaseTUI initializes database with 2PL + BTree
func NewDatabaseTUI() (*DatabaseTUI, error) {
	return NewDatabaseTUIWithConfig(RecoveryConfig{})
}

// NewDatabaseTUIWithConfig initializes database with recovery configuration
func NewDatabaseTUIWithConfig(recoveryConfig RecoveryConfig) (*DatabaseTUI, error) {
	// Disk & Buffer
	diskMgr, err := buffer.NewDiskManager(dbPath)
	if err != nil {
		return nil, fmt.Errorf("disk manager: %w", err)
	}

	replacer := concurrency.NewClockReplacer(128)

	bufferPool := buffer.NewBufferPoolManagerWithReplacer(
		50,
		diskMgr,
		replacer,
	)

	// WAL
	walMgr, err := log.NewWALManager(walPath)
	if err != nil {
		return nil, fmt.Errorf("WAL manager: %w", err)
	}

	// Storage Engine (pure - no transaction awareness)
	// storageEngine, err := btree.NewBPlusTreeBackend(bufferPool, diskMgr)
	storageEngine := lsm.NewLSMTreeBackend(walMgr, bufferPool)
	if err != nil {
		return nil, fmt.Errorf("storage engine: %w", err)
	}

	// Write Coordination
	rollbackMgr := rollback.NewRollbackManager(bufferPool)

	// RECOVER PHASE: Crash recovery and state validation
	var recoveryState *log.RecoveryState
	var recoveryStats recovery.RecoveryStats

	if !recoveryConfig.SkipRecovery {
		fmt.Printf("\n=== CRASH RECOVERY PHASE ===\n")

		recoveryCoordinator := recovery.NewDefaultRecoveryCoordinator(walMgr, storageEngine, bufferPool, rollbackMgr)
		recState, err := recoveryCoordinator.RecoverFromCrash(nil)
		if err != nil {
			fmt.Printf("⚠ Crash recovery encountered issues: %v\n", err)
		}
		recoveryState = recState
		recoveryStats = recoveryCoordinator.GetRecoveryStats()

		validator := recovery.NewRecoveryValidator(storageEngine, bufferPool)
		validationResult, err := validator.ValidateRecoveredState(recoveryState)
		if err != nil {
			fmt.Printf("⚠ State validation failed: %v\n", err)
		}

		if validationResult != nil {
			fmt.Printf("%s", validationResult.GetValidationSummary())
			if !validationResult.IsValid {
				fmt.Printf("⚠ Recovery validation warnings detected (continuing anyway)\n")
			}
		}

		if recoveryState != nil {
			fmt.Printf("\nRecovery Summary:\n")
			fmt.Printf("  Committed transactions: %d\n", len(recoveryState.CommittedTxns))
			fmt.Printf("  Aborted transactions: %d\n", len(recoveryState.AbortedTxns))
			fmt.Printf("  Recovered pages: %d\n", len(recoveryState.PageStates))
			fmt.Printf("  Dirty pages at crash: %d\n", len(recoveryState.DirtyPages))
			fmt.Printf("  Recovery statistics:\n")
			fmt.Printf("    Total records processed: %d\n", recoveryStats.TotalRecords)
			fmt.Printf("    Records redone: %d\n", recoveryStats.RecordsRedo)
			fmt.Printf("    Records undone: %d\n", recoveryStats.RecordsUndo)
			fmt.Printf("    Duration: %dms\n", recoveryStats.Duration)
		}

		if recoveryConfig.RecoveryOnly {
			fmt.Printf("\n=== RECOVERY COMPLETE (exiting due to --recovery-only flag) ===\n")
			return &DatabaseTUI{
				storageEngine:  storageEngine,
				bufferPool:     bufferPool,
				diskMgr:        diskMgr,
				walMgr:         walMgr,
				recoveryConfig: recoveryConfig,
			}, nil
		}

		fmt.Printf("=== RECOVERY PHASE COMPLETE ===\n\n")
	}
	writeHandler := write_handler.NewDefaultWriteHandler(
		storageEngine,
		rollbackMgr,
		bufferPool,
		walMgr,
	)

	// 2PL Isolation Manager
	// Create empty index managers map (indexes will be created when tables are created)
	indexManagers := make(map[string]*schema.SecondaryIndexManager)

	isolationMgr := isolation.NewMVCCManager(
		walMgr,
		bufferPool,
		writeHandler,
		rollbackMgr,
		storageEngine,
		indexManagers,
	)

	// Schema Manager
	schemaManager := schema.NewSchemaManager(storageEngine)

	return &DatabaseTUI{
		storageEngine:  storageEngine,
		isolationMgr:   isolationMgr,
		bufferPool:     bufferPool,
		diskMgr:        diskMgr,
		walMgr:         walMgr,
		schemaManager:  schemaManager,
		indexManagers:  indexManagers,
		recoveryConfig: recoveryConfig,
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
	txnID := db.isolationMgr.BeginTransaction(txn_unit.SERIALIZABLE, "", nil)
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
	txnID := db.isolationMgr.BeginTransaction(txn_unit.READ_COMMITTED, "", nil)
	defer db.isolationMgr.Commit(txnID)

	value, err := db.isolationMgr.Read(txnID, []byte(key))
	if err != nil {
		return fmt.Errorf("read failed: %w", err)
	}
	if len(value) == 0 {
		fmt.Printf("✗ Key not found: %s\n", key)
		return nil
	}
	fmt.Printf("✓ %s = %s\n", key, string(value))
	return nil
}

// Delete key
func (db *DatabaseTUI) del(key string) error {
	txnID := db.isolationMgr.BeginTransaction(txn_unit.SERIALIZABLE, "", nil)
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
	results, err := db.storageEngine.Scan()
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
	results, err := db.storageEngine.Scan()
	if err != nil {
		return fmt.Errorf("scan failed: %w", err)
	}

	fileSize, _ := db.diskMgr.GetFileSize()

	fmt.Println("\n=== Database Statistics ===")
	if fileSize > 0 {
		fmt.Printf("File Size: %d bytes (%.2f MB)\n", fileSize, float64(fileSize)/1024/1024)
		fmt.Printf("Allocated Pages: %d\n", fileSize/4096)
	}
	fmt.Printf("Total Entries: %d\n", len(results))
	fmt.Printf("Page Size: 4096 bytes\n")
	fmt.Println()
	return nil
}

// createTable creates a new table with the specified schema
func (db *DatabaseTUI) createTable(name string, primaryKey string, columnDefs []string) error {
	// Validate table doesn't already exist
	if db.schemaManager.TableExists(name) {
		return fmt.Errorf("table %q already exists", name)
	}

	// Create schema
	tableSchema := schema.NewTableSchema(name, primaryKey)

	// Add primary key column (as string type, not nullable)
	if err := tableSchema.AddColumn(primaryKey, schema.DataTypeString, false); err != nil {
		return err
	}

	// Add other columns
	for _, colDef := range columnDefs {
		parts := strings.Split(colDef, ":")
		if len(parts) < 2 {
			return fmt.Errorf("invalid column definition: %q (expected name:type[:nullable])", colDef)
		}

		colName := parts[0]
		colType := parts[1]
		nullable := len(parts) > 2 && parts[2] == "nullable"

		// Parse data type
		dataType := schema.DataType(colType)
		if err := tableSchema.AddColumn(colName, dataType, nullable); err != nil {
			return err
		}
	}

	// Add primary key index
	pkIndexName := primaryKey + "_pk"
	if err := tableSchema.AddIndex(pkIndexName, schema.IndexTypePrimary, []string{primaryKey}, false); err != nil {
		return err
	}

	// Create the table
	if err := db.schemaManager.CreateTable(tableSchema); err != nil {
		return err
	}

	fmt.Printf("✓ Created table %q with primary key %q\n", name, primaryKey)
	return nil
}

// dropTable drops a table and all its data
func (db *DatabaseTUI) dropTable(name string) error {
	if !db.schemaManager.TableExists(name) {
		return fmt.Errorf("table %q not found", name)
	}

	if err := db.schemaManager.DropTable(name); err != nil {
		return err
	}

	fmt.Printf("✓ Dropped table %q\n", name)
	return nil
}

// describeTable shows the schema of a table
func (db *DatabaseTUI) describeTable(name string) error {
	tableSchema, err := db.schemaManager.GetTable(name)
	if err != nil {
		return err
	}

	fmt.Println(tableSchema.String())
	return nil
}

// listTables lists all tables
func (db *DatabaseTUI) listTables() error {
	tables := db.schemaManager.ListTables()

	if len(tables) == 0 {
		fmt.Println("No tables found")
		return nil
	}

	fmt.Printf("=== Tables (%d) ===\n", len(tables))
	for i, t := range tables {
		schema, _ := db.schemaManager.GetTable(t)
		numCols := len(schema.Columns)
		numIndexes := len(schema.Indexes)
		fmt.Printf("%d. %s (cols: %d, indexes: %d)\n", i+1, t, numCols, numIndexes)
	}
	fmt.Println()
	return nil
}

// createIndex creates a secondary index on a table
func (db *DatabaseTUI) createIndex(tableName string, indexName string, indexType string, columns []string) error {
	tableSchema, err := db.schemaManager.GetTable(tableName)
	if err != nil {
		return err
	}

	// Parse index type
	idxType := schema.IndexType(indexType)
	if idxType != schema.IndexTypeSecondary && idxType != schema.IndexTypeUnique && idxType != schema.IndexTypePrimary {
		return fmt.Errorf("invalid index type: %q", indexType)
	}

	// Add the index to schema
	if err := db.schemaManager.AddIndex(tableName, indexName, idxType, columns, idxType == schema.IndexTypeUnique); err != nil {
		return err
	}

	// Create and register SecondaryIndexManager if not already exists
	if idxType == schema.IndexTypeSecondary || idxType == schema.IndexTypeUnique {
		if db.indexManagers[tableName] == nil {
			// Create new SecondaryIndexManager for this table
			db.indexManagers[tableName] = schema.NewSecondaryIndexManager(
				tableName,
				tableSchema,
				db.storageEngine,
			)
		}
	}

	fmt.Printf("✓ Created index %q on table %q for columns: %v\n", indexName, tableName, columns)
	return nil
}

// dropIndex drops an index from a table
func (db *DatabaseTUI) dropIndex(tableName string, indexName string) error {
	if !db.schemaManager.TableExists(tableName) {
		return fmt.Errorf("table %q not found", tableName)
	}

	if err := db.schemaManager.RemoveIndex(tableName, indexName); err != nil {
		return err
	}

	fmt.Printf("✓ Dropped index %q from table %q\n", indexName, tableName)
	return nil
}

// setTable sets a value in a specific table (table-aware operation)
func (db *DatabaseTUI) setTable(table string, key string, value string) error {
	// Validate table exists
	if !db.schemaManager.TableExists(table) {
		return fmt.Errorf("table %q not found", table)
	}

	// Get table schema
	tableSchema, err := db.schemaManager.GetTable(table)
	if err != nil {
		return err
	}

	// Create composite key: "table:key"
	compositeKey := fmt.Sprintf("%s:%s", table, key)

	txnID := db.isolationMgr.BeginTransaction(txn_unit.SERIALIZABLE, table, tableSchema)
	if err := db.isolationMgr.Write(txnID, []byte(compositeKey), []byte(value)); err != nil {
		db.isolationMgr.Abort(txnID)
		return fmt.Errorf("write failed: %w", err)
	}
	if err := db.isolationMgr.Commit(txnID); err != nil {
		db.isolationMgr.Abort(txnID)
		return fmt.Errorf("commit failed: %w", err)
	}

	fmt.Printf("✓ Set %s.%s = %s\n", table, key, value)
	return nil
}

// getTable gets a value from a specific table (table-aware operation)
func (db *DatabaseTUI) getTable(table string, key string) error {
	// Validate table exists
	if !db.schemaManager.TableExists(table) {
		return fmt.Errorf("table %q not found", table)
	}

	// Get table schema
	tableSchema, err := db.schemaManager.GetTable(table)
	if err != nil {
		return err
	}

	// Create composite key: "table:key"
	compositeKey := fmt.Sprintf("%s:%s", table, key)

	txnID := db.isolationMgr.BeginTransaction(txn_unit.READ_COMMITTED, table, tableSchema)
	defer db.isolationMgr.Commit(txnID)

	value, err := db.isolationMgr.Read(txnID, []byte(compositeKey))
	if err != nil {
		return fmt.Errorf("read failed: %w", err)
	}

	if len(value) == 0 {
		fmt.Printf("✗ Key not found: %s.%s\n", table, key)
		return nil
	}

	fmt.Printf("✓ %s.%s = %s\n", table, key, string(value))
	return nil
}

// delTable deletes a value from a specific table (table-aware operation)
func (db *DatabaseTUI) delTable(table string, key string) error {
	// Validate table exists
	if !db.schemaManager.TableExists(table) {
		return fmt.Errorf("table %q not found", table)
	}

	// Get table schema
	tableSchema, err := db.schemaManager.GetTable(table)
	if err != nil {
		return err
	}

	// Create composite key: "table:key"
	compositeKey := fmt.Sprintf("%s:%s", table, key)

	txnID := db.isolationMgr.BeginTransaction(txn_unit.SERIALIZABLE, table, tableSchema)
	if err := db.isolationMgr.Write(txnID, []byte(compositeKey), []byte("")); err != nil {
		db.isolationMgr.Abort(txnID)
		return fmt.Errorf("delete failed: %w", err)
	}
	if err := db.isolationMgr.Commit(txnID); err != nil {
		db.isolationMgr.Abort(txnID)
		return fmt.Errorf("commit failed: %w", err)
	}

	fmt.Printf("✓ Deleted %s.%s\n", table, key)
	return nil
}

func (db *DatabaseTUI) queryIndex(table string, indexName string, indexValue string) error {
	if !db.schemaManager.TableExists(table) {
		return fmt.Errorf("table %q not found", table)
	}

	tableSchema, err := db.schemaManager.GetTable(table)
	if err != nil {
		return err
	}

	indexMgr, exists := db.indexManagers[table]
	if !exists || indexMgr == nil {
		return fmt.Errorf("no indexes exist for table %q", table)
	}

	_, err = tableSchema.GetIndex(indexName)
	if err != nil {
		return fmt.Errorf("index %q not found: %w", indexName, err)
	}

	txnID := db.isolationMgr.BeginTransaction(txn_unit.READ_COMMITTED, table, tableSchema)
	defer db.isolationMgr.Commit(txnID)

	primaryKeys, err := indexMgr.GetPrimaryKeysForIndexValue(indexName, []byte(indexValue))
	if err != nil {
		return fmt.Errorf("index lookup failed: %w", err)
	}

	if len(primaryKeys) == 0 {
		fmt.Printf("✗ No rows found with %s.%s = %q\n", table, indexName, indexValue)
		return nil
	}

	fmt.Printf("✓ Found %d row(s) with %s.%s = %q:\n", len(primaryKeys), table, indexName, indexValue)

	for i, pk := range primaryKeys {
		compositeKey := fmt.Sprintf("%s:%s", table, string(pk))
		value, _ := db.isolationMgr.Read(txnID, []byte(compositeKey))
		if len(value) > 0 {
			fmt.Printf("  [%d] %s (pk=%s)\n", i+1, string(value), string(pk))
		}
	}

	return nil
}

func (db *DatabaseTUI) queryIndexRange(table string, indexName string, start string, end string) error {
	fmt.Println("✗ Range queries not yet implemented (requires storage engine iterator)")
	return nil
}

func (db *DatabaseTUI) Run() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("\n=== OMAG Database (2PL + Schema-Aware) ===")
	fmt.Println("\n--- Schema Commands ---")
	fmt.Println("  create_table <name> <pk> [col:type:nullable ...]")
	fmt.Println("  drop_table <name>")
	fmt.Println("  describe <table>")
	fmt.Println("  list_tables")
	fmt.Println("  create_index <table> <name> <type(secondary|unique)> [cols...]")
	fmt.Println("  drop_index <table> <index_name>")
	fmt.Println("\n--- Table Data Commands ---")
	fmt.Println("  set_table <table> <key> <value>")
	fmt.Println("  get_table <table> <key>")
	fmt.Println("  del_table <table> <key>")
	fmt.Println("  query_index <table> <index_name> <value>")
	fmt.Println("  query_index_range <table> <index_name> <start> <end>")
	fmt.Println("\n--- Legacy Commands ---")
	fmt.Println("  set <key> <value>")
	fmt.Println("  get <key>")
	fmt.Println("  del <key>")
	fmt.Println("  list, stats")
	fmt.Println("\n  exit, quit")
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
		// Schema commands
		case "create_table":
			if len(parts) < 3 {
				fmt.Println("Usage: create_table <name> <pk_column> [col:type:nullable ...]")
				fmt.Println("Example: create_table users id name:string age:int64:nullable email:string")
				continue
			}
			tableName := parts[1]
			pk := parts[2]
			columns := parts[3:]
			if err := db.createTable(tableName, pk, columns); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "drop_table":
			if len(parts) < 2 {
				fmt.Println("Usage: drop_table <table_name>")
				continue
			}
			if err := db.dropTable(parts[1]); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "describe":
			if len(parts) < 2 {
				fmt.Println("Usage: describe <table_name>")
				continue
			}
			if err := db.describeTable(parts[1]); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "list_tables":
			if err := db.listTables(); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "create_index":
			if len(parts) < 4 {
				fmt.Println("Usage: create_index <table> <index_name> <type> [column_names...]")
				fmt.Println("Types: secondary, unique")
				fmt.Println("Example: create_index users email_idx secondary email")
				continue
			}
			table := parts[1]
			idxName := parts[2]
			idxType := parts[3]
			columns := parts[4:]
			if err := db.createIndex(table, idxName, idxType, columns); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "drop_index":
			if len(parts) < 3 {
				fmt.Println("Usage: drop_index <table> <index_name>")
				continue
			}
			if err := db.dropIndex(parts[1], parts[2]); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		// Table data commands
		case "set_table":
			if len(parts) < 4 {
				fmt.Println("Usage: set_table <table> <key> <value>")
				continue
			}
			table := parts[1]
			key := parts[2]
			value := strings.Join(parts[3:], " ")
			if err := db.setTable(table, key, value); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "get_table":
			if len(parts) < 3 {
				fmt.Println("Usage: get_table <table> <key>")
				continue
			}
			if err := db.getTable(parts[1], parts[2]); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "del_table":
			if len(parts) < 3 {
				fmt.Println("Usage: del_table <table> <key>")
				continue
			}
			if err := db.delTable(parts[1], parts[2]); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "query_index":
			if len(parts) < 4 {
				fmt.Println("Usage: query_index <table> <index_name> <value>")
				continue
			}
			if err := db.queryIndex(parts[1], parts[2], parts[3]); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		case "query_index_range":
			if len(parts) < 5 {
				fmt.Println("Usage: query_index_range <table> <index_name> <start> <end>")
				continue
			}
			if err := db.queryIndexRange(parts[1], parts[2], parts[3], parts[4]); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			}

		// Legacy commands
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
			fmt.Println("Unknown command. Type 'help' or scroll up for commands.")
		}
	}
}

func main() {
	// Parse command-line flags for recovery control
	skipRecoveryFlag := flag.Bool("skip-recovery", false, "Skip crash recovery on startup")
	recoveryOnlyFlag := flag.Bool("recovery-only", false, "Perform recovery then exit (for inspection)")
	flag.Parse()

	// Create recovery configuration
	recoveryConfig := RecoveryConfig{
		SkipRecovery: *skipRecoveryFlag,
		RecoveryOnly: *recoveryOnlyFlag,
	}

	db, err := NewDatabaseTUIWithConfig(recoveryConfig)
	if err != nil {
		fmt.Printf("✗ Failed to init database: %v\n", err)
		os.Exit(1)
	}

	// If recovery-only mode, exit after database init (recovery done in NewDatabaseTUIWithConfig)
	if recoveryConfig.RecoveryOnly {
		return
	}

	db.Run()
}
