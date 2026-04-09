package lsm

import (
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type LSMTreeBackend struct {
	logManager    log.ILogManager
	bufferManager buffer.IBufferPoolManager
}

func (l *LSMTreeBackend) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (l *LSMTreeBackend) Put(key []byte, value []byte) error {
	return nil
}

func (l *LSMTreeBackend) Delete(key []byte) error {
	return nil
}
