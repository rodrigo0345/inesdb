package btree

type DBStore interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Cursor() (Cursor, error)
}

type Cursor interface {
	Seek(key []byte) error
	First() error
	Next() error
	Valid() bool
	Key() []byte
	Value() []byte
}
