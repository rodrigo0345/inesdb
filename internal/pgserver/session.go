package pgserver

import (
	"net"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/gosqlx"
	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/jackc/pgproto3/v2"
	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/executor"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	"github.com/rodrigo0345/omag/pkg/pkglog"
)

type session struct {
	conn       net.Conn
	be         *pgproto3.Backend
	db         database.Database
	txnID      int64
	autoCommit bool
	telemetry  *pkglog.TelemetryCollector // cached once per connection
}

func newSession(conn net.Conn, db database.Database) *session {
	be := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
	return &session{
		conn:       conn,
		be:         be,
		db:         db,
		autoCommit: true,
		telemetry:  db.GetTelemetry(),
	}
}

// Run handles the full lifecycle of a single psql connection.
func (s *session) Run() error {
	defer s.conn.Close()

	if err := s.handshake(); err != nil {
		return err
	}

	for {
		msg, err := s.be.Receive()
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case *pgproto3.Query:
			s.handleQuery(m.String)
		case *pgproto3.Terminate:
			return nil
		default:
			_ = s.be.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "0A000",
				Message:  "extended query protocol not supported",
			})
			_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
		}
	}
}

// handshake performs the PostgreSQL startup sequence.
func (s *session) handshake() error {
	startupMsg, err := s.be.ReceiveStartupMessage()
	if err != nil {
		return err
	}
	if _, isSSL := startupMsg.(*pgproto3.SSLRequest); isSSL {
		if _, wErr := s.conn.Write([]byte{'N'}); wErr != nil {
			return wErr
		}
		startupMsg, err = s.be.ReceiveStartupMessage()
		if err != nil {
			return err
		}
	}
	_ = startupMsg

	_ = s.be.Send(&pgproto3.AuthenticationOk{})
	_ = s.be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0"})
	_ = s.be.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	_ = s.be.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
	_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return nil
}

// handleQuery processes a simple-query protocol message.
func (s *session) handleQuery(sql string) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		_ = s.be.Send(&pgproto3.EmptyQueryResponse{})
		_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
		return
	}

	upper := strings.ToUpper(strings.TrimRight(sql, "; \t\n"))

	switch upper {
	case "BEGIN", "START TRANSACTION", "BEGIN TRANSACTION":
		s.handleBegin()
		return
	case "COMMIT", "COMMIT TRANSACTION", "COMMIT WORK":
		s.handleCommit()
		return
	case "ROLLBACK", "ROLLBACK TRANSACTION", "ROLLBACK WORK":
		s.handleRollback()
		return
	}

	// ANALYZE is not parsed by GoSQLX; handle it directly.
	if strings.HasPrefix(upper, "ANALYZE") {
		tableArg := strings.TrimSpace(sql[7:]) // strip "ANALYZE"
		tableArg = strings.TrimRight(tableArg, "; \t\n")
		s.handleAnalyze(tableArg)
		return
	}

	if isNoop(upper) {
		_ = s.be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SET")})
		_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
		return
	}

	parsed, err := gosqlx.Parse(sql)
	if err != nil {
		sendError(s.be, err)
		_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
		return
	}

	for _, stmt := range parsed.Statements {
		if err := s.execStmt(stmt); err != nil {
			sendError(s.be, err)
			if s.autoCommit && s.txnID != 0 {
				_ = s.db.Abort(s.txnID)
				s.txnID = 0
			}
			_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
			return
		}
	}
	_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
}

// execStmt executes one parsed statement, managing implicit transactions and
// recording a telemetry span.
func (s *session) execStmt(stmt ast.Statement) error {
	stmtType, table := classifyStmt(stmt)
	span := s.telemetry.StartSpan(s.txnID, stmt.TokenLiteral(), stmtType, table)

	if s.autoCommit && s.txnID == 0 {
		s.txnID = s.db.BeginTransaction(txn_unit.SERIALIZABLE)
		span.TxnID = s.txnID // update after txn is allocated
	}

	result, err := executor.Execute(s.db, s.txnID, stmt)
	if err != nil {
		s.telemetry.EndSpan(span, 0, err)
		if s.autoCommit && s.txnID != 0 {
			_ = s.db.Abort(s.txnID)
			s.txnID = 0
		}
		return err
	}

	rows := int64(0)
	if result != nil {
		rows = result.RowsAffected
	}
	s.telemetry.EndSpan(span, rows, nil)

	if sendErr := sendResult(s.be, result); sendErr != nil {
		pkglog.Warn("[pgserver] send result: %v", sendErr)
	}

	if s.autoCommit {
		if commitErr := s.db.Commit(s.txnID); commitErr != nil {
			s.txnID = 0
			return commitErr
		}
		s.txnID = 0
	}
	return nil
}

// handleAnalyze runs ANALYZE outside an explicit transaction (auto-commit).
func (s *session) handleAnalyze(tableArg string) {
	span := s.telemetry.StartSpan(s.txnID, "ANALYZE "+tableArg, "ANALYZE", tableArg)

	txnID := s.txnID
	autoTxn := s.autoCommit && txnID == 0
	if autoTxn {
		txnID = s.db.BeginTransaction(txn_unit.READ_COMMITTED)
		span.TxnID = txnID
	}

	result, err := executor.ExecAnalyze(s.db, txnID, tableArg)
	if err != nil {
		s.telemetry.EndSpan(span, 0, err)
		if autoTxn {
			_ = s.db.Abort(txnID)
		}
		sendError(s.be, err)
		_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
		return
	}

	s.telemetry.EndSpan(span, result.RowsAffected, nil)

	if autoTxn {
		_ = s.db.Commit(txnID)
	}

	if sendErr := sendResult(s.be, result); sendErr != nil {
		pkglog.Warn("[pgserver] send analyze result: %v", sendErr)
	}
	_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
}

func (s *session) handleBegin() {
	if s.txnID != 0 {
		_ = s.be.Send(&pgproto3.NoticeResponse{
			Severity: "WARNING",
			Message:  "there is already a transaction in progress",
		})
	} else {
		s.txnID = s.db.BeginTransaction(txn_unit.SERIALIZABLE)
		s.autoCommit = false
	}
	_ = s.be.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
	_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
}

func (s *session) handleCommit() {
	if s.txnID != 0 {
		if err := s.db.Commit(s.txnID); err != nil {
			_ = s.db.Abort(s.txnID)
			s.txnID = 0
			s.autoCommit = true
			sendError(s.be, err)
			_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			return
		}
		s.txnID = 0
	}
	s.autoCommit = true
	_ = s.be.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
	_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func (s *session) handleRollback() {
	if s.txnID != 0 {
		_ = s.db.Abort(s.txnID)
		s.txnID = 0
	}
	s.autoCommit = true
	_ = s.be.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
	_ = s.be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func (s *session) txStatus() byte {
	if s.txnID != 0 {
		return 'T'
	}
	return 'I'
}

// classifyStmt returns a (statementType, tableName) pair for telemetry.
func classifyStmt(stmt ast.Statement) (stmtType, table string) {
	switch q := stmt.(type) {
	case *ast.SelectStatement:
		return "SELECT", q.TableName
	case *ast.InsertStatement:
		return "INSERT", q.TableName
	case *ast.UpdateStatement:
		return "UPDATE", q.TableName
	case *ast.DeleteStatement:
		return "DELETE", q.TableName
	case *ast.CreateTableStatement:
		return "DDL", q.Name
	case *ast.DropStatement:
		name := ""
		if len(q.Names) > 0 {
			name = q.Names[0]
		}
		return "DDL", name
	default:
		return "UNKNOWN", ""
	}
}

func isNoop(upper string) bool {
	return strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "SHOW ") ||
		upper == "RESET ALL" ||
		strings.HasPrefix(upper, "RESET ") ||
		upper == "DISCARD ALL"
}
