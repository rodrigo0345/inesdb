package storage

type ScanEntry struct {
	Key   []byte
	Value []byte
}

type RowFilterFunction func(row ScanEntry) bool

type ScanOptions struct {
	LowerBound    []byte
	UpperBound    []byte
	Inclusive     bool // Should the upper bound be included?
	Reverse       bool // DESC ordering
	Limit         int  // Stop after N rows
	Offset        int  // Skip N rows
	KeyOnly       bool // Optimization for EXISTS or index-only scans
	Projection    []string
	ComplexFilter RowFilterFunction
}

type ICursor interface {
	Next() bool
	Entry() ScanEntry
	Error() error
	Close() error
}

type IStorageEngine interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Scan(opts ScanOptions) (ICursor, error)
}
