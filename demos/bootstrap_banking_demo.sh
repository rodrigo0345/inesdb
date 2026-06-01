#!/usr/bin/env bash
set -euo pipefail

# Bootstrap a banking-like dataset for local transaction tests.
# Usage:
#   ./bootstrap_banking_demo.sh
#   PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGDATABASE=postgres ./bootstrap_banking_demo.sh

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
PGSSLMODE="${PGSSLMODE:-disable}"

PSQL=(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1)

echo "[banking-bootstrap] target: $PGUSER@$PGHOST:$PGPORT/$PGDATABASE"

# Best-effort cleanup (ignore errors if tables do not exist).
for tbl in loan_installments loans ledger_entries transfers beneficiaries cards account_holders accounts customers; do
  "${PSQL[@]}" -c "DROP TABLE $tbl;" >/dev/null 2>&1 || true
done

"${PSQL[@]}" <<'SQL'
CREATE TABLE customers (
  customer_id TEXT PRIMARY KEY,
  full_name TEXT,
  email TEXT,
  tax_id TEXT,
  phone TEXT,
  risk_profile TEXT,
  status TEXT,
  created_at TEXT
);

CREATE TABLE accounts (
  account_id TEXT PRIMARY KEY,
  account_number TEXT,
  account_type TEXT,
  currency TEXT,
  available_balance TEXT,
  ledger_balance TEXT,
  status TEXT,
  opened_at TEXT
);

CREATE TABLE account_holders (
  account_holder_id TEXT PRIMARY KEY,
  account_id TEXT,
  customer_id TEXT,
  role TEXT,
  linked_at TEXT
);

CREATE TABLE cards (
  card_id TEXT PRIMARY KEY,
  account_id TEXT,
  customer_id TEXT,
  card_type TEXT,
  masked_pan TEXT,
  card_status TEXT,
  daily_limit TEXT,
  issued_at TEXT
);

CREATE TABLE beneficiaries (
  beneficiary_id TEXT PRIMARY KEY,
  customer_id TEXT,
  nickname TEXT,
  bank_name TEXT,
  beneficiary_account TEXT,
  created_at TEXT
);

CREATE TABLE transfers (
  transfer_id TEXT PRIMARY KEY,
  from_account_id TEXT,
  to_account_id TEXT,
  beneficiary_id TEXT,
  amount TEXT,
  currency TEXT,
  transfer_type TEXT,
  transfer_status TEXT,
  reference_text TEXT,
  created_at TEXT,
  completed_at TEXT
);

CREATE TABLE ledger_entries (
  entry_id TEXT PRIMARY KEY,
  account_id TEXT,
  transfer_id TEXT,
  entry_type TEXT,
  amount TEXT,
  balance_after TEXT,
  description TEXT,
  created_at TEXT
);

CREATE TABLE loans (
  loan_id TEXT PRIMARY KEY,
  customer_id TEXT,
  linked_account_id TEXT,
  principal_amount TEXT,
  outstanding_amount TEXT,
  interest_rate_annual TEXT,
  installment_count TEXT,
  loan_status TEXT,
  started_at TEXT
);

CREATE TABLE loan_installments (
  installment_id TEXT PRIMARY KEY,
  loan_id TEXT,
  installment_number TEXT,
  due_date TEXT,
  amount_due TEXT,
  paid_amount TEXT,
  installment_status TEXT,
  paid_at TEXT
);

INSERT INTO customers (customer_id, full_name, email, tax_id, phone, risk_profile, status, created_at)
VALUES ('cus_001', 'Ana Martins', 'ana.martins@example.com', '11111111111', '+55-11-90000-0001', 'LOW', 'ACTIVE', '2026-01-05T10:00:00Z');
INSERT INTO customers (customer_id, full_name, email, tax_id, phone, risk_profile, status, created_at)
VALUES ('cus_002', 'Bruno Lima', 'bruno.lima@example.com', '22222222222', '+55-11-90000-0002', 'MEDIUM', 'ACTIVE', '2026-01-08T12:10:00Z');
INSERT INTO customers (customer_id, full_name, email, tax_id, phone, risk_profile, status, created_at)
VALUES ('cus_003', 'Carla Souza', 'carla.souza@example.com', '33333333333', '+55-11-90000-0003', 'HIGH', 'ACTIVE', '2026-01-10T09:30:00Z');
INSERT INTO customers (customer_id, full_name, email, tax_id, phone, risk_profile, status, created_at)
VALUES ('cus_004', 'Diego Rocha', 'diego.rocha@example.com', '44444444444', '+55-11-90000-0004', 'LOW', 'ACTIVE', '2026-01-11T14:20:00Z');

INSERT INTO accounts (account_id, account_number, account_type, currency, available_balance, ledger_balance, status, opened_at)
VALUES ('acc_001', '0001-01', 'CHECKING', 'BRL', '12500.00', '12500.00', 'ACTIVE', '2026-01-05T10:10:00Z');
INSERT INTO accounts (account_id, account_number, account_type, currency, available_balance, ledger_balance, status, opened_at)
VALUES ('acc_002', '0001-02', 'SAVINGS', 'BRL', '45200.50', '45200.50', 'ACTIVE', '2026-01-05T10:12:00Z');
INSERT INTO accounts (account_id, account_number, account_type, currency, available_balance, ledger_balance, status, opened_at)
VALUES ('acc_003', '0001-03', 'CHECKING', 'BRL', '7800.00', '7800.00', 'ACTIVE', '2026-01-08T12:20:00Z');
INSERT INTO accounts (account_id, account_number, account_type, currency, available_balance, ledger_balance, status, opened_at)
VALUES ('acc_004', '0001-04', 'CHECKING', 'BRL', '1580.10', '1580.10', 'ACTIVE', '2026-01-10T09:45:00Z');
INSERT INTO accounts (account_id, account_number, account_type, currency, available_balance, ledger_balance, status, opened_at)
VALUES ('acc_005', '0001-05', 'CHECKING', 'BRL', '21999.90', '21999.90', 'ACTIVE', '2026-01-11T14:35:00Z');

INSERT INTO account_holders (account_holder_id, account_id, customer_id, role, linked_at)
VALUES ('ah_001', 'acc_001', 'cus_001', 'PRIMARY', '2026-01-05T10:15:00Z');
INSERT INTO account_holders (account_holder_id, account_id, customer_id, role, linked_at)
VALUES ('ah_002', 'acc_002', 'cus_001', 'PRIMARY', '2026-01-05T10:16:00Z');
INSERT INTO account_holders (account_holder_id, account_id, customer_id, role, linked_at)
VALUES ('ah_003', 'acc_003', 'cus_002', 'PRIMARY', '2026-01-08T12:21:00Z');
INSERT INTO account_holders (account_holder_id, account_id, customer_id, role, linked_at)
VALUES ('ah_004', 'acc_004', 'cus_003', 'PRIMARY', '2026-01-10T09:46:00Z');
INSERT INTO account_holders (account_holder_id, account_id, customer_id, role, linked_at)
VALUES ('ah_005', 'acc_005', 'cus_004', 'PRIMARY', '2026-01-11T14:36:00Z');

INSERT INTO cards (card_id, account_id, customer_id, card_type, masked_pan, card_status, daily_limit, issued_at)
VALUES ('card_001', 'acc_001', 'cus_001', 'DEBIT', '4111********0001', 'ACTIVE', '2500.00', '2026-01-06T08:00:00Z');
INSERT INTO cards (card_id, account_id, customer_id, card_type, masked_pan, card_status, daily_limit, issued_at)
VALUES ('card_002', 'acc_003', 'cus_002', 'DEBIT', '5111********0002', 'ACTIVE', '1800.00', '2026-01-09T08:10:00Z');
INSERT INTO cards (card_id, account_id, customer_id, card_type, masked_pan, card_status, daily_limit, issued_at)
VALUES ('card_003', 'acc_004', 'cus_003', 'DEBIT', '6111********0003', 'ACTIVE', '900.00', '2026-01-11T07:40:00Z');

INSERT INTO beneficiaries (beneficiary_id, customer_id, nickname, bank_name, beneficiary_account, created_at)
VALUES ('ben_001', 'cus_001', 'Bruno Pessoal', 'OMAG Bank', '0001-03', '2026-01-20T10:00:00Z');
INSERT INTO beneficiaries (beneficiary_id, customer_id, nickname, bank_name, beneficiary_account, created_at)
VALUES ('ben_002', 'cus_002', 'Carla Casa', 'OMAG Bank', '0001-04', '2026-01-22T11:00:00Z');
INSERT INTO beneficiaries (beneficiary_id, customer_id, nickname, bank_name, beneficiary_account, created_at)
VALUES ('ben_003', 'cus_003', 'Diego Servicos', 'OMAG Bank', '0001-05', '2026-01-25T09:50:00Z');

INSERT INTO transfers (transfer_id, from_account_id, to_account_id, beneficiary_id, amount, currency, transfer_type, transfer_status, reference_text, created_at, completed_at)
VALUES ('tr_001', 'acc_001', 'acc_003', 'ben_001', '230.00', 'BRL', 'PIX', 'COMPLETED', 'Almoco equipe', '2026-02-01T12:00:00Z', '2026-02-01T12:00:03Z');
INSERT INTO transfers (transfer_id, from_account_id, to_account_id, beneficiary_id, amount, currency, transfer_type, transfer_status, reference_text, created_at, completed_at)
VALUES ('tr_002', 'acc_003', 'acc_004', 'ben_002', '120.50', 'BRL', 'PIX', 'COMPLETED', 'Reembolso internet', '2026-02-02T08:10:00Z', '2026-02-02T08:10:01Z');
INSERT INTO transfers (transfer_id, from_account_id, to_account_id, beneficiary_id, amount, currency, transfer_type, transfer_status, reference_text, created_at, completed_at)
VALUES ('tr_003', 'acc_004', 'acc_005', 'ben_003', '75.40', 'BRL', 'TED', 'PENDING', 'Pagamento fornecedor', '2026-02-03T18:05:00Z', '');

INSERT INTO ledger_entries (entry_id, account_id, transfer_id, entry_type, amount, balance_after, description, created_at)
VALUES ('le_001', 'acc_001', 'tr_001', 'DEBIT', '230.00', '12270.00', 'PIX enviado para Bruno', '2026-02-01T12:00:03Z');
INSERT INTO ledger_entries (entry_id, account_id, transfer_id, entry_type, amount, balance_after, description, created_at)
VALUES ('le_002', 'acc_003', 'tr_001', 'CREDIT', '230.00', '8030.00', 'PIX recebido de Ana', '2026-02-01T12:00:03Z');
INSERT INTO ledger_entries (entry_id, account_id, transfer_id, entry_type, amount, balance_after, description, created_at)
VALUES ('le_003', 'acc_003', 'tr_002', 'DEBIT', '120.50', '7909.50', 'PIX enviado para Carla', '2026-02-02T08:10:01Z');
INSERT INTO ledger_entries (entry_id, account_id, transfer_id, entry_type, amount, balance_after, description, created_at)
VALUES ('le_004', 'acc_004', 'tr_002', 'CREDIT', '120.50', '1700.60', 'PIX recebido de Bruno', '2026-02-02T08:10:01Z');

INSERT INTO loans (loan_id, customer_id, linked_account_id, principal_amount, outstanding_amount, interest_rate_annual, installment_count, loan_status, started_at)
VALUES ('loan_001', 'cus_002', 'acc_003', '12000.00', '9130.75', '1.85', '24', 'ACTIVE', '2026-01-15T10:00:00Z');
INSERT INTO loans (loan_id, customer_id, linked_account_id, principal_amount, outstanding_amount, interest_rate_annual, installment_count, loan_status, started_at)
VALUES ('loan_002', 'cus_003', 'acc_004', '4500.00', '4500.00', '2.10', '12', 'ACTIVE', '2026-02-01T09:00:00Z');

INSERT INTO loan_installments (installment_id, loan_id, installment_number, due_date, amount_due, paid_amount, installment_status, paid_at)
VALUES ('inst_001', 'loan_001', '1', '2026-02-15', '615.40', '615.40', 'PAID', '2026-02-15T11:20:00Z');
INSERT INTO loan_installments (installment_id, loan_id, installment_number, due_date, amount_due, paid_amount, installment_status, paid_at)
VALUES ('inst_002', 'loan_001', '2', '2026-03-15', '615.40', '0.00', 'PENDING', '');
INSERT INTO loan_installments (installment_id, loan_id, installment_number, due_date, amount_due, paid_amount, installment_status, paid_at)
VALUES ('inst_003', 'loan_002', '1', '2026-03-01', '410.00', '0.00', 'PENDING', '');
SQL

echo "[banking-bootstrap] done"

echo "[banking-bootstrap] quick sanity checks:"
"${PSQL[@]}" -c "SELECT customer_id, full_name, status FROM customers;"
"${PSQL[@]}" -c "SELECT account_id, account_number, available_balance FROM accounts;"
"${PSQL[@]}" -c "SELECT transfer_id, transfer_status, amount, from_account_id, to_account_id FROM transfers;"
"${PSQL[@]}" -c "SELECT loan_id, outstanding_amount, loan_status FROM loans;"

