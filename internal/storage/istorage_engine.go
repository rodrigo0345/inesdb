package storage

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
