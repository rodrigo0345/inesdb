package wal

// ReplicationIntentLogger allows write paths to persist replication intent metadata.
type ReplicationIntentLogger interface {
	LogReplicationIntent(txnID uint64, tableName string, opType RecordType, key []byte, value []byte)
}
