package storage

// ScanEntry represents a key-value pair from a scan operation
type ScanEntry struct {
	Key   []byte
	Value []byte
}

type IStorageEngine interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Scan() ([]ScanEntry, error)
}
