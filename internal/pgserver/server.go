package pgserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	applog "github.com/rodrigo0345/omag/pkg/log"
)

const (
	pgTextOID         uint32 = 25
	rowPayloadPrefix          = "OMAGROW1"
)

var (
	reCreateTable  = regexp.MustCompile(`(?is)^CREATE\s+TABLE\s+(?:(IF\s+NOT\s+EXISTS)\s+)?([^(\s]+)\s*\((.*)\)$`)
	reDropTable    = regexp.MustCompile(`(?is)^DROP\s+TABLE\s+(?:(IF\s+EXISTS)\s+)?([^(\s;]+)$`)
	reCreateIndex  = regexp.MustCompile(`(?is)^CREATE\s+(UNIQUE\s+)?INDEX\s+(?:(IF\s+NOT\s+EXISTS)\s+)?([^(\s]+)\s+ON\s+([^(\s]+)\s*\((.*)\)$`)
	reDropIndex    = regexp.MustCompile(`(?is)^DROP\s+INDEX\s+(?:(IF\s+EXISTS)\s+)?([^(\s;]+)$`)
	reInsert       = regexp.MustCompile(`(?is)^INSERT\s+INTO\s+([^(\s]+)\s*(?:\((.*)\))?\s+VALUES\s*\((.*)\)$`)
	reUpdate       = regexp.MustCompile(`(?is)^UPDATE\s+([^(\s]+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$`)
	reDelete       = regexp.MustCompile(`(?is)^DELETE\s+FROM\s+([^(\s;]+)(?:\s+WHERE\s+(.*))?$`)
	reSelect       = regexp.MustCompile(`(?is)^SELECT\s+(.*)\s+FROM\s+([^(\s]+)(?:\s+WHERE\s+(.*))?$`)
	reBegin        = regexp.MustCompile(`(?is)^BEGIN(?:\s+TRANSACTION)?$`)
	reCommit       = regexp.MustCompile(`(?is)^COMMIT$`)
	reRollback     = regexp.MustCompile(`(?is)^ROLLBACK$`)
	reSet          = regexp.MustCompile(`(?is)^SET\s+([^(\s=]+)\s*=\s*(.+)$`)
	reShow         = regexp.MustCompile(`(?is)^SHOW\s+([^(\s;]+)$`)
	reSimpleSelect = regexp.MustCompile(`(?is)^SELECT\s+(.+)$`)
)

type Server struct {
	db                       database.Database
	enablePrimaryKeyFastPath bool
	cacheMu                  sync.RWMutex
	tableSchemaCache         map[string]*schema.TableSchema
}

type connSession struct {
	inTransaction  bool
	txnID          int64
	txnTableName   string
	txnTableSchema *schema.TableSchema
	status         byte
}

type statementResult struct {
	messages []pgproto3.BackendMessage
	status   byte
}

func New(db database.Database) *Server {
	return &Server{db: db, enablePrimaryKeyFastPath: true, tableSchemaCache: make(map[string]*schema.TableSchema)}
}

func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()

	applog.Info("[PGSERVER] listening on %s", addr)

	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				wg.Wait()
				return nil
			}
			return err
		}
		applog.Debug("[PGSERVER] accepted connection remote=%s", conn.RemoteAddr().String())

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.serveConn(conn); err != nil {
				applog.Warn("[PGSERVER] connection closed with error remote=%s err=%v", conn.RemoteAddr().String(), err)
			} else {
				applog.Debug("[PGSERVER] connection closed remote=%s", conn.RemoteAddr().String())
			}
		}()
	}
}

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	go func() {
		defer close(errCh)
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				errCh <- err
				return
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = s.serveConn(conn)
			}()
		}
	}()

	select {
	case err := <-errCh:
		wg.Wait()
		return err
	case <-ctx.Done():
		wg.Wait()
		return ctx.Err()
	}
}

func (s *Server) serveConn(conn net.Conn) error {
	defer func() { _ = conn.Close() }()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
	sess := &connSession{status: 'I'}

	if err := s.handshake(backend, conn); err != nil {
		return err
	}

	for {
		msg, err := backend.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "unexpected EOF") {
				return nil
			}
			return err
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			applog.Debug("[PGSERVER] query remote=%s sql=%q", conn.RemoteAddr().String(), strings.TrimSpace(m.String))
			res, err := s.handleQuery(sess, m.String)
			if err != nil {
				applog.Debug("[PGSERVER] query error remote=%s err=%v", conn.RemoteAddr().String(), err)
				if sendErr := s.sendError(backend, sess.status, err); sendErr != nil {
					return sendErr
				}
				continue
			}
			for _, message := range res.messages {
				if err := backend.Send(message); err != nil {
					return err
				}
			}
			if res.status != 0 {
				sess.status = res.status
			}
			if err := backend.Send(&pgproto3.ReadyForQuery{TxStatus: sess.status}); err != nil {
				return err
			}
		case *pgproto3.Terminate:
			return nil
		case *pgproto3.Sync:
			if err := backend.Send(&pgproto3.ReadyForQuery{TxStatus: sess.status}); err != nil {
				return err
			}
		default:
			applog.Debug("[PGSERVER] unsupported frontend message remote=%s type=%T", conn.RemoteAddr().String(), msg)
			if err := s.sendError(backend, sess.status, fmt.Errorf("unsupported frontend message %T", msg)); err != nil {
				return err
			}
		}
	}
}

func (s *Server) handshake(backend *pgproto3.Backend, conn net.Conn) error {
	for {
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			return err
		}
		applog.Debug("[PGSERVER] startup message remote=%s type=%T", conn.RemoteAddr().String(), msg)

		switch msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return err
			}
			continue
		case *pgproto3.CancelRequest:
			return nil
		case *pgproto3.StartupMessage:
			break
		default:
			return fmt.Errorf("unsupported startup message %T", msg)
		}
		break
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"},
		&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"},
		&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"},
		&pgproto3.BackendKeyData{ProcessID: uint32(time.Now().Unix()), SecretKey: 1},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	} {
		if err := backend.Send(msg); err != nil {
			return err
		}
	}
	applog.Debug("[PGSERVER] handshake complete remote=%s", conn.RemoteAddr().String())

	return nil
}

func (s *Server) handleQuery(sess *connSession, sqlText string) (statementResult, error) {
	stmts := splitStatements(sqlText)
	result := statementResult{status: sess.status}

	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		res, err := s.executeStatement(sess, stmt)
		if err != nil {
			return statementResult{}, err
		}
		result.messages = append(result.messages, res.messages...)
		if res.status != 0 {
			result.status = res.status
		}
	}

	return result, nil
}

func (s *Server) executeStatement(sess *connSession, stmt string) (statementResult, error) {
	switch {
	case reBegin.MatchString(stmt):
		if sess.inTransaction {
			return statementResult{}, fmt.Errorf("transaction already in progress")
		}
		sess.inTransaction = true
		sess.txnID = 0
		sess.txnTableName = ""
		sess.txnTableSchema = nil
		return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")}}, status: 'T'}, nil
	case reCommit.MatchString(stmt):
		if !sess.inTransaction {
			return statementResult{}, fmt.Errorf("no transaction is in progress")
		}
		if sess.txnID != 0 {
			if err := s.db.Commit(sess.txnID); err != nil {
				return statementResult{}, err
			}
		}
		sess.inTransaction = false
		sess.txnID = 0
		sess.txnTableName = ""
		sess.txnTableSchema = nil
		return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")}}, status: 'I'}, nil
	case reRollback.MatchString(stmt):
		if !sess.inTransaction {
			return statementResult{}, fmt.Errorf("no transaction is in progress")
		}
		if sess.txnID != 0 {
			if err := s.db.Abort(sess.txnID); err != nil {
				return statementResult{}, err
			}
		}
		sess.inTransaction = false
		sess.txnID = 0
		sess.txnTableName = ""
		sess.txnTableSchema = nil
		return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")}}, status: 'I'}, nil
	case reSet.MatchString(stmt):
		return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("SET")}}}, nil
	case reShow.MatchString(stmt):
		return s.execShow(stmt)
	case reCreateTable.MatchString(stmt):
		return s.execCreateTable(sess, stmt)
	case reDropTable.MatchString(stmt):
		return s.execDropTable(sess, stmt)
	case reCreateIndex.MatchString(stmt):
		return s.execCreateIndex(sess, stmt)
	case reDropIndex.MatchString(stmt):
		return s.execDropIndex(sess, stmt)
	case reInsert.MatchString(stmt):
		return s.execInsert(sess, stmt)
	case reUpdate.MatchString(stmt):
		return s.execUpdate(sess, stmt)
	case reDelete.MatchString(stmt):
		return s.execDelete(sess, stmt)
	case reSelect.MatchString(stmt):
		return s.execSelect(stmt)
	case reSimpleSelect.MatchString(stmt):
		return s.execLiteralSelect(stmt)
	default:
		return statementResult{}, fmt.Errorf("unsupported SQL statement: %s", stmt)
	}
}

func (s *Server) beginMutationTxn(sess *connSession, tableName string, tableSchema *schema.TableSchema) (txnID int64, started bool, err error) {
	if sess == nil {
		return 0, false, fmt.Errorf("session is nil")
	}

	if sess.txnID != 0 {
		if sess.txnTableName != "" && tableName != "" && sess.txnTableName != tableName {
			return 0, false, fmt.Errorf("transaction already bound to table %q", sess.txnTableName)
		}
		if sess.txnTableName == "" && tableName != "" {
			sess.txnTableName = tableName
			sess.txnTableSchema = tableSchema
		}
		return sess.txnID, false, nil
	}

	txnID = s.db.BeginTransaction(txn_unit.READ_COMMITTED, tableName, tableSchema)
	if txnID <= 0 {
		return 0, false, fmt.Errorf("failed to begin transaction")
	}

	started = true
	if sess.inTransaction {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}

	return txnID, started, nil
}

func (s *Server) execCreateTable(sess *connSession, stmt string) (statementResult, error) {
	m := reCreateTable.FindStringSubmatch(stmt)
	if len(m) != 4 {
		return statementResult{}, fmt.Errorf("invalid CREATE TABLE statement")
	}

	ifNotExists := strings.TrimSpace(m[1]) != ""
	tableName := normalizeIdent(m[2])
	defs := splitTopLevel(m[3], ',')
	if len(defs) == 0 {
		return statementResult{}, fmt.Errorf("CREATE TABLE requires at least one column")
	}

	var primaryKey string
	tableSchema := schema.NewTableSchema(tableName, "")
	for _, def := range defs {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}

		upper := strings.ToUpper(def)
		if strings.HasPrefix(upper, "PRIMARY KEY") {
			cols, err := parseParenList(def)
			if err != nil {
				return statementResult{}, err
			}
			if len(cols) != 1 {
				return statementResult{}, fmt.Errorf("PRIMARY KEY must specify exactly one column")
			}
			primaryKey = normalizeIdent(cols[0])
			continue
		}

		parts := strings.Fields(def)
		if len(parts) < 2 {
			return statementResult{}, fmt.Errorf("invalid column definition %q", def)
		}

		colName := normalizeIdent(parts[0])
		dataType := parseDataType(parts[1])
		nullable := !strings.Contains(upper, "NOT NULL")
		if err := tableSchema.AddColumn(colName, dataType, nullable); err != nil {
			return statementResult{}, err
		}
		if strings.Contains(upper, "PRIMARY KEY") {
			primaryKey = colName
		}
	}

	if primaryKey == "" {
		return statementResult{}, fmt.Errorf("CREATE TABLE requires a primary key")
	}
	// Rebuild schema with the actual primary key value.
	tableSchema.PrimaryKey = primaryKey
	if pkCol, ok := tableSchema.Columns[primaryKey]; ok {
		pkCol.Nullable = false
	}
	if err := tableSchema.Validate(); err != nil {
		return statementResult{}, err
	}

	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return statementResult{}, err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}
	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}
		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()

	createErr := s.db.CreateTable(tableSchema)
	if createErr != nil {
		if ifNotExists {
			// IF NOT EXISTS: suppress error if table already exists (common case: "table already exists")
			applog.Debug("[PGSERVER] CREATE TABLE IF NOT EXISTS: table %q already exists, continuing", tableName)
		} else {
			err = createErr
			return statementResult{}, createErr
		}
	} else {
		if schemaJSON, marshalErr := tableSchema.ToJSON(); marshalErr == nil {
			s.recordTransactionOperation(txnID, tableName, log.CREATE_TABLE, nil, schemaJSON)
		}
	}
	s.storeTableSchema(tableName, tableSchema)

	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("CREATE TABLE")}}}, nil
}

func (s *Server) execDropTable(sess *connSession, stmt string) (statementResult, error) {
	m := reDropTable.FindStringSubmatch(stmt)
	if len(m) != 3 {
		return statementResult{}, fmt.Errorf("invalid DROP TABLE statement")
	}

	ifExists := strings.TrimSpace(m[1]) != ""
	tableName := normalizeIdent(m[2])
	tableSchema, err := s.getTableSchema(tableName)
	if err != nil && !ifExists {
		return statementResult{}, err
	}
	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return statementResult{}, err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}
	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}
		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()

	dropErr := s.db.DropTable(tableName)
	if dropErr != nil {
		if ifExists {
			// IF EXISTS: suppress error if table doesn't exist
			applog.Debug("[PGSERVER] DROP TABLE IF EXISTS: table %q does not exist, continuing", tableName)
		} else {
			err = dropErr
			return statementResult{}, dropErr
		}
	} else {
		s.recordTransactionOperation(txnID, tableName, log.DROP_TABLE, nil, nil)
		s.dropTableSchema(tableName)
	}
	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("DROP TABLE")}}}, nil
}

func (s *Server) execCreateIndex(sess *connSession, stmt string) (statementResult, error) {
	m := reCreateIndex.FindStringSubmatch(stmt)
	if len(m) != 6 {
		return statementResult{}, fmt.Errorf("invalid CREATE INDEX statement")
	}

	isUnique := strings.TrimSpace(strings.ToUpper(m[1])) != ""
	ifNotExists := strings.TrimSpace(m[2]) != ""
	indexName := normalizeIdent(m[3])
	tableName := normalizeIdent(m[4])
	cols, err := parseIdentList(m[5])
	if err != nil {
		return statementResult{}, err
	}
	if len(cols) != 1 {
		return statementResult{}, fmt.Errorf("only single-column indexes are supported")
	}

	indexType := schema.IndexTypeSecondary
	if isUnique {
		indexType = schema.IndexTypeUnique
	}
	tableSchema, err := s.getTableSchema(tableName)
	if err != nil {
		return statementResult{}, err
	}
	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return statementResult{}, err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}
	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}
		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()
	indexJSON, marshalErr := json.Marshal(struct {
		IndexName string   `json:"indexName"`
		IndexType string   `json:"indexType"`
		Columns   []string `json:"columns"`
		Unique    bool     `json:"unique"`
	}{IndexName: indexName, IndexType: string(indexType), Columns: cols, Unique: isUnique})
	if marshalErr != nil {
		return statementResult{}, marshalErr
	}
	createIndexErr := s.db.CreateIndex(tableName, indexName, indexType, cols, isUnique)
	if createIndexErr != nil {
		if ifNotExists {
			// IF NOT EXISTS: suppress error if index already exists
			applog.Debug("[PGSERVER] CREATE INDEX IF NOT EXISTS: index %q already exists, continuing", indexName)
		} else {
			err = createIndexErr
			return statementResult{}, createIndexErr
		}
	} else {
		s.recordTransactionOperation(txnID, tableName, log.CREATE_INDEX, []byte(indexName), indexJSON)
	}
	if ts, err := s.getTableSchema(tableName); err == nil {
		s.storeTableSchema(tableName, ts)
	}

	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("CREATE INDEX")}}, status: 'I'}, nil
}

func (s *Server) execDropIndex(sess *connSession, stmt string) (statementResult, error) {
	m := reDropIndex.FindStringSubmatch(stmt)
	if len(m) != 3 {
		return statementResult{}, fmt.Errorf("invalid DROP INDEX statement")
	}

	ifExists := strings.TrimSpace(m[1]) != ""
	indexName := normalizeIdent(m[2])
	tables := s.db.SchemaManager().ListTables()
	var tableName string
	var tableSchema *schema.TableSchema
	for _, candidate := range tables {
		if ts, err := s.db.GetTableSchema(candidate); err == nil {
			if _, exists := ts.Indexes[indexName]; exists {
				tableName = candidate
				tableSchema = ts
				break
			}
		}
	}
	if tableName == "" {
		if ifExists {
			applog.Debug("[PGSERVER] DROP INDEX IF EXISTS: index %q does not exist, continuing", indexName)
			return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("DROP INDEX")}}}, nil
		}
		return statementResult{}, fmt.Errorf("index %q not found", indexName)
	}
	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return statementResult{}, err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}
	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}
		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()
	dropIndexErr := s.db.DropIndex(tableName, indexName)
	if dropIndexErr != nil {
		if ifExists {
			applog.Debug("[PGSERVER] DROP INDEX IF EXISTS: index %q does not exist, continuing", indexName)
			return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("DROP INDEX")}}}, nil
		}
		err = dropIndexErr
		return statementResult{}, dropIndexErr
	}
	s.recordTransactionOperation(txnID, tableName, log.DROP_INDEX, []byte(indexName), nil)
	if ts, err := s.getTableSchema(tableName); err == nil {
		s.storeTableSchema(tableName, ts)
	}
	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("DROP INDEX")}}}, nil
}

func (s *Server) recordTransactionOperation(txnID int64, tableName string, opType log.RecordType, key []byte, value []byte) {
	if recorder, ok := s.db.(interface {
		RecordTransactionOperation(txnID uint64, tableName string, opType log.RecordType, key []byte, value []byte)
	}); ok {
		recorder.RecordTransactionOperation(uint64(txnID), tableName, opType, key, value)
	}
}

func (s *Server) execInsert(sess *connSession, stmt string) (statementResult, error) {
	m := reInsert.FindStringSubmatch(stmt)
	if len(m) != 4 {
		return statementResult{}, fmt.Errorf("invalid INSERT statement")
	}

	tableName := normalizeIdent(m[1])
	tableSchema, err := s.getTableSchema(tableName)
	if err != nil {
		return statementResult{}, err
	}

	cols := tableSchema.ColumnList
	if strings.TrimSpace(m[2]) != "" {
		cols, err = parseIdentList(m[2])
		if err != nil {
			return statementResult{}, err
		}
	}

	values, err := parseValueList(m[3])
	if err != nil {
		return statementResult{}, err
	}
	if len(cols) != len(values) {
		return statementResult{}, fmt.Errorf("column/value count mismatch")
	}

	engine := s.db.TableStorageEngine(tableName)
	if engine == nil {
		return statementResult{}, fmt.Errorf("table %q not found", tableName)
	}
	pkIdx := indexOfColumn(cols, tableSchema.PrimaryKey)
	if len(cols) == len(tableSchema.ColumnList) && columnsMatchNormalized(cols, tableSchema.ColumnList) {
		if pkIdx < 0 {
			return statementResult{}, fmt.Errorf("primary key %q must be included", tableSchema.PrimaryKey)
		}
		pkValue := values[pkIdx]
		if _, err := engine.Get([]byte(pkValue)); err == nil {
			return statementResult{}, fmt.Errorf("duplicate primary key value %q", pkValue)
		}
		if err := s.writeOrderedRow(sess, tableName, tableSchema, values); err != nil {
			return statementResult{}, err
		}
		return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")}}}, nil
	}
	row := make(map[string]string, len(cols))
	for i, col := range cols {
		row[normalizeIdent(col)] = values[i]
	}
	if _, ok := row[tableSchema.PrimaryKey]; !ok {
		return statementResult{}, fmt.Errorf("primary key %q must be included", tableSchema.PrimaryKey)
	}
	if _, err := engine.Get([]byte(row[tableSchema.PrimaryKey])); err == nil {
		return statementResult{}, fmt.Errorf("duplicate primary key value %q", row[tableSchema.PrimaryKey])
	}

	if err := s.writeRow(sess, tableName, tableSchema, row); err != nil {
		return statementResult{}, err
	}
	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")}}}, nil
}

func (s *Server) execUpdate(sess *connSession, stmt string) (statementResult, error) {
	m := reUpdate.FindStringSubmatch(stmt)
	if len(m) != 4 {
		return statementResult{}, fmt.Errorf("invalid UPDATE statement")
	}

	tableName := normalizeIdent(m[1])
	tableSchema, err := s.getTableSchema(tableName)
	if err != nil {
		return statementResult{}, err
	}

	setMap, err := parseAssignments(m[2])
	if err != nil {
		return statementResult{}, err
	}
	whereMap, err := parseWhereClause(m[3])
	if err != nil {
		return statementResult{}, err
	}

	count, err := s.updateRows(sess, tableName, tableSchema, setMap, whereMap)
	if err != nil {
		return statementResult{}, err
	}

	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("UPDATE %d", count))}}}, nil
}

func (s *Server) execDelete(sess *connSession, stmt string) (statementResult, error) {
	m := reDelete.FindStringSubmatch(stmt)
	if len(m) != 3 {
		return statementResult{}, fmt.Errorf("invalid DELETE statement")
	}

	tableName := normalizeIdent(m[1])
	tableSchema, err := s.getTableSchema(tableName)
	if err != nil {
		return statementResult{}, err
	}

	whereMap := map[string]string{}
	if strings.TrimSpace(m[2]) != "" {
		whereMap, err = parseWhereClause(m[2])
		if err != nil {
			return statementResult{}, err
		}
	}

	if s.enablePrimaryKeyFastPath && len(whereMap) == 1 {
		if pkValue, ok := whereMap[tableSchema.PrimaryKey]; ok {
			count, err := s.deleteRowByPrimaryKey(sess, tableName, tableSchema, pkValue)
			if err != nil {
				return statementResult{}, err
			}
			return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("DELETE %d", count))}}}, nil
		}
	}

	count, err := s.deleteRows(sess, tableName, tableSchema, whereMap)
	if err != nil {
		return statementResult{}, err
	}

	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("DELETE %d", count))}}}, nil
}

func (s *Server) deleteRowByPrimaryKey(sess *connSession, tableName string, tableSchema *schema.TableSchema, primaryKeyValue string) (count int, err error) {
	engine := s.db.TableStorageEngine(tableName)
	if engine == nil {
		return 0, fmt.Errorf("table %q not found", tableName)
	}
	if _, getErr := engine.Get([]byte(primaryKeyValue)); getErr != nil {
		return 0, nil
	}

	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return 0, err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}

	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}

		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()

	if err = s.db.Delete(txnID, []byte(primaryKeyValue)); err != nil {
		return 0, err
	}

	return 1, nil
}

func (s *Server) execSelect(stmt string) (statementResult, error) {
	m := reSelect.FindStringSubmatch(stmt)
	if len(m) != 4 {
		return statementResult{}, fmt.Errorf("invalid SELECT statement")
	}

	columnExpr := strings.TrimSpace(m[1])
	tableName := normalizeIdent(m[2])
	tableSchema, err := s.getTableSchema(tableName)
	if err != nil {
		return statementResult{}, err
	}

	cols, err := s.resolveSelectColumns(tableSchema, columnExpr)
	if err != nil {
		return statementResult{}, err
	}

	whereMap := map[string]string{}
	if strings.TrimSpace(m[3]) != "" {
		whereMap, err = parseWhereClause(m[3])
		if err != nil {
			return statementResult{}, err
		}
	}

	if row, found, fastPath, err := s.readRowByPrimaryKey(tableName, tableSchema, whereMap); err != nil {
		return statementResult{}, err
	} else if fastPath {
		rows := make([]map[string]string, 0, 1)
		if found {
			rows = append(rows, row)
		}
		return buildSelectResult(cols, rows), nil
	}

	if len(whereMap) > 0 {
		rows, err := s.scanRows(tableName, tableSchema)
		if err != nil {
			return statementResult{}, err
		}
		rows = filterRows(rows, whereMap)
		return buildSelectResult(cols, rows), nil
	}

	rows, err := s.readRows(tableName, tableSchema)
	if err != nil {
		return statementResult{}, err
	}

	return buildSelectResult(cols, rows), nil
}

func buildSelectResult(cols []string, rows []map[string]string) statementResult {
	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, col := range cols {
		fields[i] = pgproto3.FieldDescription{
			Name:         []byte(col),
			DataTypeOID:  pgTextOID,
			DataTypeSize: -1,
			TypeModifier: -1,
			Format:       pgproto3.TextFormat,
		}
	}

	messages := make([]pgproto3.BackendMessage, 0, len(rows)+2)
	messages = append(messages, &pgproto3.RowDescription{Fields: fields})
	for _, row := range rows {
		values := make([][]byte, len(cols))
		for i, col := range cols {
			values[i] = []byte(row[col])
		}
		messages = append(messages, &pgproto3.DataRow{Values: values})
	}
	messages = append(messages, &pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(rows)))})
	return statementResult{messages: messages}
}

func (s *Server) readRowByPrimaryKey(tableName string, tableSchema *schema.TableSchema, whereMap map[string]string) (row map[string]string, found bool, fastPath bool, err error) {
	if tableSchema == nil || len(whereMap) != 1 {
		return nil, false, false, nil
	}
	pkValue, ok := whereMap[tableSchema.PrimaryKey]
	if !ok {
		return nil, false, false, nil
	}

	engine := s.db.TableStorageEngine(tableName)
	if engine == nil {
		return nil, false, true, fmt.Errorf("table %q not found", tableName)
	}

	payload, getErr := engine.Get([]byte(pkValue))
	if getErr != nil {
		return nil, false, true, nil
	}

	decoded, decErr := decodeRow(payload, tableSchema)
	if decErr != nil {
		return nil, false, true, decErr
	}
	if _, exists := decoded[tableSchema.PrimaryKey]; !exists {
		decoded[tableSchema.PrimaryKey] = pkValue
	}
	return decoded, true, true, nil
}

func (s *Server) execLiteralSelect(stmt string) (statementResult, error) {
	// Support simple expressions like `SELECT 1` or `SELECT 'ok'`.
	m := reSimpleSelect.FindStringSubmatch(stmt)
	if len(m) != 2 {
		return statementResult{}, fmt.Errorf("invalid SELECT statement")
	}

	expr := strings.TrimSpace(m[1])
	if strings.Contains(strings.ToUpper(expr), " FROM ") {
		return statementResult{}, fmt.Errorf("unsupported SELECT form")
	}

	value, err := parseSQLValue(expr)
	if err != nil {
		return statementResult{}, err
	}

	messages := []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("?column?"), DataTypeOID: pgTextOID, DataTypeSize: -1, TypeModifier: -1, Format: pgproto3.TextFormat}}},
		&pgproto3.DataRow{Values: [][]byte{[]byte(value)}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
	}
	return statementResult{messages: messages}, nil
}

func (s *Server) execShow(stmt string) (statementResult, error) {
	m := reShow.FindStringSubmatch(stmt)
	if len(m) != 2 {
		return statementResult{}, fmt.Errorf("invalid SHOW statement")
	}

	name := normalizeIdent(m[1])
	value := map[string]string{
		"client_encoding":             "UTF8",
		"server_encoding":             "UTF8",
		"server_version":              "15.0",
		"standard_conforming_strings": "on",
		"application_name":            "omag",
		"search_path":                 "public",
		"transaction_isolation":       "read committed",
		"transaction_read_only":       "off",
		"datestyle":                   "ISO, MDY",
		"integer_datetimes":           "on",
	}[name]
	if value == "" {
		value = ""
	}

	messages := []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte(name), DataTypeOID: pgTextOID, DataTypeSize: -1, TypeModifier: -1, Format: pgproto3.TextFormat}}},
		&pgproto3.DataRow{Values: [][]byte{[]byte(value)}},
		&pgproto3.CommandComplete{CommandTag: []byte("SHOW")},
	}
	return statementResult{messages: messages}, nil
}

func (s *Server) resolveSelectColumns(tableSchema *schema.TableSchema, expr string) ([]string, error) {
	expr = strings.TrimSpace(expr)
	if expr == "*" {
		return append([]string(nil), tableSchema.ColumnList...), nil
	}
	parts, err := parseIdentList(expr)
	if err != nil {
		return nil, err
	}
	for _, col := range parts {
		if _, ok := tableSchema.Columns[normalizeIdent(col)]; !ok {
			return nil, fmt.Errorf("column %q does not exist", col)
		}
	}
	return normalizeIdents(parts), nil
}

func (s *Server) readRows(tableName string, tableSchema *schema.TableSchema) ([]map[string]string, error) {
	rows, err := s.scanRows(tableName, tableSchema)
	if err != nil {
		return nil, err
	}
	pkType := schema.DataTypeString
	if colType, err := tableSchema.ColumnDataType(tableSchema.PrimaryKey); err == nil {
		pkType = colType
	}
	sort.Slice(rows, func(i, j int) bool {
		return compareRowValues(rows[i][tableSchema.PrimaryKey], rows[j][tableSchema.PrimaryKey], pkType) < 0
	})
	return rows, nil
}

func (s *Server) scanRows(tableName string, tableSchema *schema.TableSchema) ([]map[string]string, error) {
	engine := s.db.TableStorageEngine(tableName)
	if engine == nil {
		return nil, fmt.Errorf("table %q not found", tableName)
	}

	entries, err := engine.Scan(nil, nil)
	if err != nil {
		return nil, err
	}

	rows := make([]map[string]string, 0, len(entries))
	for _, entry := range entries {
		row, err := decodeRow(entry.Value, tableSchema)
		if err != nil {
			return nil, err
		}
		if _, ok := row[tableSchema.PrimaryKey]; !ok {
			row[tableSchema.PrimaryKey] = string(entry.Key)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (s *Server) getTableSchema(tableName string) (*schema.TableSchema, error) {
	s.cacheMu.RLock()
	if ts, ok := s.tableSchemaCache[tableName]; ok && ts != nil {
		s.cacheMu.RUnlock()
		return ts, nil
	}
	s.cacheMu.RUnlock()

	ts, err := s.db.GetTableSchema(tableName)
	if err != nil {
		return nil, err
	}
	s.storeTableSchema(tableName, ts)
	return ts, nil
}

func (s *Server) storeTableSchema(tableName string, ts *schema.TableSchema) {
	if tableName == "" || ts == nil {
		return
	}
	s.cacheMu.Lock()
	if s.tableSchemaCache == nil {
		s.tableSchemaCache = make(map[string]*schema.TableSchema)
	}
	s.tableSchemaCache[tableName] = ts
	s.cacheMu.Unlock()
}

func (s *Server) dropTableSchema(tableName string) {
	if tableName == "" {
		return
	}
	s.cacheMu.Lock()
	delete(s.tableSchemaCache, tableName)
	s.cacheMu.Unlock()
}

func (s *Server) writeRow(sess *connSession, tableName string, tableSchema *schema.TableSchema, row map[string]string) (err error) {
	if tableSchema == nil {
		return fmt.Errorf("table schema is nil")
	}

	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}

	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}

		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()

	payload, marshalErr := encodeRowPayload(tableSchema, row)
	if marshalErr != nil {
		return marshalErr
	}
	if err = s.db.Write(txnID, []byte(row[tableSchema.PrimaryKey]), payload); err != nil {
		return err
	}
	return nil
}

func (s *Server) writeOrderedRow(sess *connSession, tableName string, tableSchema *schema.TableSchema, values []string) (err error) {
	if tableSchema == nil {
		return fmt.Errorf("table schema is nil")
	}
	if len(values) != len(tableSchema.ColumnList) {
		return fmt.Errorf("column/value count mismatch")
	}
	pkIdx := indexOfColumn(tableSchema.ColumnList, tableSchema.PrimaryKey)
	if pkIdx < 0 {
		return fmt.Errorf("primary key %q must be included", tableSchema.PrimaryKey)
	}

	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}

	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}

		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()

	payload, payloadErr := encodeRowPayloadOrdered(tableSchema, values)
	if payloadErr != nil {
		return payloadErr
	}
	if err = s.db.Write(txnID, []byte(values[pkIdx]), payload); err != nil {
		return err
	}
	return nil
}

func (s *Server) updateRowByPrimaryKey(sess *connSession, tableName string, tableSchema *schema.TableSchema, whereMap, setMap map[string]string) (count int, err error) {
	row, found, fastPath, err := s.readRowByPrimaryKey(tableName, tableSchema, whereMap)
	if err != nil {
		return 0, err
	}
	if !fastPath || !found {
		return 0, nil
	}

	for col, val := range setMap {
		if _, ok := tableSchema.Columns[col]; !ok {
			return 0, fmt.Errorf("column %q does not exist", col)
		}
		row[col] = val
	}

	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return 0, err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}
	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}

		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()

	payload, marshalErr := encodeRowPayload(tableSchema, row)
	if marshalErr != nil {
		return 0, marshalErr
	}
	if err = s.db.Write(txnID, []byte(row[tableSchema.PrimaryKey]), payload); err != nil {
		return 0, err
	}
	return 1, nil
}

func (s *Server) updateRows(sess *connSession, tableName string, tableSchema *schema.TableSchema, setMap, whereMap map[string]string) (count int, err error) {
	if _, updatesPrimaryKey := setMap[tableSchema.PrimaryKey]; updatesPrimaryKey {
		return 0, fmt.Errorf("updating primary key %q is not supported", tableSchema.PrimaryKey)
	}
	if s.enablePrimaryKeyFastPath && len(whereMap) == 1 {
		if _, ok := whereMap[tableSchema.PrimaryKey]; ok {
			return s.updateRowByPrimaryKey(sess, tableName, tableSchema, whereMap, setMap)
		}
	}

	rows, err := s.readRows(tableName, tableSchema)
	if err != nil {
		return 0, err
	}

	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return 0, err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}
	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}

		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()

	for _, row := range rows {
		if !rowMatches(row, whereMap) {
			continue
		}
		for col, val := range setMap {
			if _, ok := tableSchema.Columns[col]; !ok {
				return 0, fmt.Errorf("column %q does not exist", col)
			}
			row[col] = val
		}
		payload, marshalErr := encodeRowPayload(tableSchema, row)
		if marshalErr != nil {
			return 0, marshalErr
		}
		if err = s.db.Write(txnID, []byte(row[tableSchema.PrimaryKey]), payload); err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}

func (s *Server) deleteRows(sess *connSession, tableName string, tableSchema *schema.TableSchema, whereMap map[string]string) (count int, err error) {
	if s.enablePrimaryKeyFastPath && len(whereMap) == 1 {
		if pkValue, ok := whereMap[tableSchema.PrimaryKey]; ok {
			return s.deleteRowByPrimaryKey(sess, tableName, tableSchema, pkValue)
		}
	}

	rows, err := s.readRows(tableName, tableSchema)
	if err != nil {
		return 0, err
	}

	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}

		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}

	for _, row := range rows {
		if !rowMatches(row, whereMap) {
			continue
		}
		if err = s.db.Delete(txnID, []byte(row[tableSchema.PrimaryKey])); err != nil {
			return 0, err
		}
		count++
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}
	return count, nil
}

func (s *Server) sendError(backend *pgproto3.Backend, status byte, err error) error {
	if sendErr := backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42601", Message: err.Error()}); sendErr != nil {
		return sendErr
	}
	return backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
}

func splitStatements(sqlText string) []string {
	var parts []string
	var buf strings.Builder
	var inSingle, inDouble bool

	for i := 0; i < len(sqlText); i++ {
		c := sqlText[i]
		switch c {
		case '\'':
			if !inDouble {
				buf.WriteByte(c)
				if inSingle && i+1 < len(sqlText) && sqlText[i+1] == '\'' {
					buf.WriteByte(sqlText[i+1])
					i++
					continue
				}
				inSingle = !inSingle
				continue
			}
		case '"':
			if !inSingle {
				buf.WriteByte(c)
				if inDouble && i+1 < len(sqlText) && sqlText[i+1] == '"' {
					buf.WriteByte(sqlText[i+1])
					i++
					continue
				}
				inDouble = !inDouble
				continue
			}
		case ';':
			if inSingle || inDouble {
				buf.WriteByte(c)
				continue
			}
			part := strings.TrimSpace(buf.String())
			if part != "" {
				parts = append(parts, part)
			}
			buf.Reset()
			continue
		}
		buf.WriteByte(c)
	}

	if part := strings.TrimSpace(buf.String()); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func splitTopLevel(s string, sep rune) []string {
	var parts []string
	var buf strings.Builder
	depth := 0
	var inSingle, inDouble bool
	sepByte := byte(sep)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\'':
			if !inDouble {
				buf.WriteByte(c)
				if inSingle && i+1 < len(s) && s[i+1] == '\'' {
					buf.WriteByte(s[i+1])
					i++
					continue
				}
				inSingle = !inSingle
				continue
			}
		case '"':
			if !inSingle {
				buf.WriteByte(c)
				if inDouble && i+1 < len(s) && s[i+1] == '"' {
					buf.WriteByte(s[i+1])
					i++
					continue
				}
				inDouble = !inDouble
				continue
			}
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && depth > 0 {
				depth--
			}
		default:
			if c == sepByte && !inSingle && !inDouble && depth == 0 {
				part := strings.TrimSpace(buf.String())
				if part != "" {
					parts = append(parts, part)
				}
				buf.Reset()
				continue
			}
		}
		buf.WriteByte(c)
	}
	if part := strings.TrimSpace(buf.String()); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func parseParenList(s string) ([]string, error) {
	start := strings.Index(s, "(")
	end := strings.LastIndex(s, ")")
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid parenthesized list: %s", s)
	}
	inner := s[start+1 : end]
	return parseIdentList(inner)
}

func parseIdentList(s string) ([]string, error) {
	parts := splitTopLevel(s, ',')
	idents := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idents = append(idents, normalizeIdent(part))
	}
	return idents, nil
}

func normalizeIdents(idents []string) []string {
	out := make([]string, len(idents))
	for i, ident := range idents {
		out[i] = normalizeIdent(ident)
	}
	return out
}

func parseAssignments(s string) (map[string]string, error) {
	assignments := make(map[string]string)
	for _, part := range splitTopLevel(s, ',') {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid assignment %q", part)
		}
		key := normalizeIdent(kv[0])
		val, err := parseSQLValue(kv[1])
		if err != nil {
			return nil, err
		}
		assignments[key] = val
	}
	return assignments, nil
}

func parseWhereClause(s string) (map[string]string, error) {
	conds := make(map[string]string)
	for _, part := range splitOnAnd(s) {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("unsupported WHERE clause %q", part)
		}
		key := normalizeIdent(kv[0])
		val, err := parseSQLValue(kv[1])
		if err != nil {
			return nil, err
		}
		conds[key] = val
	}
	return conds, nil
}

func splitOnAnd(s string) []string {
	var parts []string
	var buf strings.Builder
	var inSingle, inDouble bool
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\'':
			if !inDouble {
				buf.WriteByte(c)
				if inSingle && i+1 < len(s) && s[i+1] == '\'' {
					buf.WriteByte(s[i+1])
					i++
					continue
				}
				inSingle = !inSingle
				continue
			}
		case '"':
			if !inSingle {
				buf.WriteByte(c)
				if inDouble && i+1 < len(s) && s[i+1] == '"' {
					buf.WriteByte(s[i+1])
					i++
					continue
				}
				inDouble = !inDouble
				continue
			}
		}

		if !inSingle && !inDouble && i+3 <= len(s) && strings.EqualFold(s[i:i+3], "AND") {
			prevOK := i == 0 || isWhitespaceOrParen(s[i-1])
			nextOK := i+3 == len(s) || isWhitespaceOrParen(s[i+3])
			if prevOK && nextOK {
				part := strings.TrimSpace(buf.String())
				if part != "" {
					parts = append(parts, part)
				}
				buf.Reset()
				i += 2
				continue
			}
		}

		buf.WriteByte(c)
	}
	if part := strings.TrimSpace(buf.String()); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func isWhitespaceOrParen(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '(', ')':
		return true
	default:
		return false
	}
}

func rowMatches(row map[string]string, whereMap map[string]string) bool {
	for col, val := range whereMap {
		if row[col] != val {
			return false
		}
	}
	return true
}

func filterRows(rows []map[string]string, whereMap map[string]string) []map[string]string {
	if len(whereMap) == 0 {
		return rows
	}
	filtered := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		if rowMatches(row, whereMap) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func parseSQLValue(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "NULL" || raw == "null" {
		return "", nil
	}
	if len(raw) >= 2 {
		if (raw[0] == '\'' && raw[len(raw)-1] == '\'') || (raw[0] == '"' && raw[len(raw)-1] == '"') {
			return unescapeSQLString(raw[1 : len(raw)-1]), nil
		}
	}
	return raw, nil
}

func parseValueList(s string) ([]string, error) {
	parts := splitTopLevel(s, ',')
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		val, err := parseSQLValue(part)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func unescapeSQLString(s string) string {
	s = strings.ReplaceAll(s, "''", "'")
	s = strings.ReplaceAll(s, `""`, `"`)
	return s
}

func compareRowValues(a, b string, dataType schema.DataType) int {
	switch dataType {
	case schema.DataTypeInt64:
		if ai, err := strconv.ParseInt(strings.TrimSpace(a), 10, 64); err == nil {
			if bi, err := strconv.ParseInt(strings.TrimSpace(b), 10, 64); err == nil {
				switch {
				case ai < bi:
					return -1
				case ai > bi:
					return 1
				default:
					return 0
				}
			}
		}
	case schema.DataTypeFloat64:
		if af, err := strconv.ParseFloat(strings.TrimSpace(a), 64); err == nil {
			if bf, err := strconv.ParseFloat(strings.TrimSpace(b), 64); err == nil {
				switch {
				case af < bf:
					return -1
				case af > bf:
					return 1
				default:
					return 0
				}
			}
		}
	case schema.DataTypeBool:
		if ab, err := strconv.ParseBool(strings.TrimSpace(a)); err == nil {
			if bb, err := strconv.ParseBool(strings.TrimSpace(b)); err == nil {
				switch {
				case !ab && bb:
					return -1
				case ab && !bb:
					return 1
				default:
					return 0
				}
			}
		}
	}
	return strings.Compare(a, b)
}

func normalizeIdent(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "`\"[]")
	return strings.ToLower(s)
}

func parseDataType(token string) schema.DataType {
	switch strings.ToUpper(strings.TrimSpace(token)) {
	case "INT", "INT4", "INTEGER", "BIGINT", "SMALLINT", "SERIAL":
		return schema.DataTypeInt64
	case "BOOL", "BOOLEAN":
		return schema.DataTypeBool
	case "FLOAT", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL":
		return schema.DataTypeFloat64
	case "BYTEA":
		return schema.DataTypeBytes
	default:
		return schema.DataTypeString
	}
}

func encodeRowPayload(tableSchema *schema.TableSchema, row map[string]string) ([]byte, error) {
	if tableSchema == nil {
		return json.Marshal(row)
	}

	buf := make([]byte, 0, len(tableSchema.ColumnList)*16+16)
	buf = append(buf, rowPayloadPrefix...)
	buf = appendUint16(buf, uint16(len(tableSchema.ColumnList)))
	for _, col := range tableSchema.ColumnList {
		val, ok := row[col]
		if !ok {
			buf = append(buf, 0)
			continue
		}
		buf = append(buf, 1)
		buf = appendUint32(buf, uint32(len(val)))
		buf = append(buf, val...)
	}
	return buf, nil
}

func encodeRowPayloadOrdered(tableSchema *schema.TableSchema, values []string) ([]byte, error) {
	if tableSchema == nil {
		return nil, fmt.Errorf("table schema is nil")
	}
	if len(values) != len(tableSchema.ColumnList) {
		return nil, fmt.Errorf("column/value count mismatch")
	}

	buf := make([]byte, 0, len(values)*16+16)
	buf = append(buf, rowPayloadPrefix...)
	buf = appendUint16(buf, uint16(len(values)))
	for _, val := range values {
		buf = append(buf, 1)
		buf = appendUint32(buf, uint32(len(val)))
		buf = append(buf, val...)
	}
	return buf, nil
}

func decodeRow(data []byte, tableSchema *schema.TableSchema) (map[string]string, error) {
	if tableSchema != nil && bytes.HasPrefix(data, []byte(rowPayloadPrefix)) {
		offset := len(rowPayloadPrefix)
		if offset+2 > len(data) {
			return nil, fmt.Errorf("invalid row payload: truncated header")
		}
		colCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if colCount != len(tableSchema.ColumnList) {
			return nil, fmt.Errorf("invalid row payload: column count mismatch")
		}

		row := make(map[string]string, colCount)
		for _, col := range tableSchema.ColumnList {
			if offset >= len(data) {
				return nil, fmt.Errorf("invalid row payload: truncated column flag")
			}
			present := data[offset]
			offset++
			if present == 0 {
				continue
			}
			if offset+4 > len(data) {
				return nil, fmt.Errorf("invalid row payload: truncated value length")
			}
			valueLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
			offset += 4
			if offset+valueLen > len(data) {
				return nil, fmt.Errorf("invalid row payload: truncated value bytes")
			}
			row[col] = string(data[offset : offset+valueLen])
			offset += valueLen
		}
		return row, nil
	}

	var row map[string]string
	if err := json.Unmarshal(data, &row); err != nil {
		return nil, err
	}
	if row == nil {
		row = make(map[string]string)
	}
	return row, nil
}

func appendUint16(dst []byte, v uint16) []byte {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	return append(dst, buf[:]...)
}

func appendUint32(dst []byte, v uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return append(dst, buf[:]...)
}

func columnsMatchNormalized(cols []string, schemaCols []string) bool {
	if len(cols) != len(schemaCols) {
		return false
	}
	for i := range cols {
		if normalizeIdent(cols[i]) != normalizeIdent(schemaCols[i]) {
			return false
		}
	}
	return true
}

func indexOfColumn(cols []string, target string) int {
	for i, col := range cols {
		if normalizeIdent(col) == normalizeIdent(target) {
			return i
		}
	}
	return -1
}

