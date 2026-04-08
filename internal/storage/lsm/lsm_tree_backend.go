package lsmtreebackend

import (
	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/logmanager"
)

type LSMTreeBackend struct {
	logManager logmanager.ILogManager
	bufferManager buffermanager.IBufferPoolManager
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
