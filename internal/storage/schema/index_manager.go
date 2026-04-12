package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

type SecondaryIndexEntry struct {
	IndexKey    []byte
	PrimaryKeys [][]byte
}

type SecondaryIndexManager struct {
	tableName      string
	tableSchema    *TableSchema
	storageBackend interface {
		Put(key []byte, value []byte) error
		Get(key []byte) ([]byte, error)
		Delete(key []byte) error
	}

	indexCache map[string]map[string][]byte
	cacheSize  int
}

func NewSecondaryIndexManager(
	tableName string,
	tableSchema *TableSchema,
	storageBackend interface {
		Put(key []byte, value []byte) error
		Get(key []byte) ([]byte, error)
		Delete(key []byte) error
	},
) *SecondaryIndexManager {
	return &SecondaryIndexManager{
		tableName:      tableName,
		tableSchema:    tableSchema,
		storageBackend: storageBackend,
		indexCache:     make(map[string]map[string][]byte),
	}
}

func (sim *SecondaryIndexManager) IsSecondaryIndexKey(key []byte) bool {
	prefix := sim.getIndexKeyPrefix("")
	return bytes.HasPrefix(key, []byte(prefix))
}

func (sim *SecondaryIndexManager) AddToIndex(indexName string, indexValue []byte, primaryKey []byte) error {
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return err
	}

	key := sim.getIndexKey(indexName, indexValue)

	existingData, _ := sim.storageBackend.Get(key)

	var entry *SecondaryIndexEntry
	if existingData != nil {
		var err error
		entry, err = DecodeSecondaryIndexEntry(existingData)
		if err != nil {
			return fmt.Errorf("failed to decode index entry: %w", err)
		}
	} else {
		entry = &SecondaryIndexEntry{
			IndexKey:    indexValue,
			PrimaryKeys: make([][]byte, 0),
		}
	}

	for _, pk := range entry.PrimaryKeys {
		if bytes.Equal(pk, primaryKey) {
			return nil
		}
	}

	entry.PrimaryKeys = append(entry.PrimaryKeys, primaryKey)

	encoded, err := EncodeSecondaryIndexEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to encode index entry: %w", err)
	}

	return sim.storageBackend.Put(key, encoded)
}

func (sim *SecondaryIndexManager) RemoveFromIndex(indexName string, indexValue []byte, primaryKey []byte) error {
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return err
	}

	key := sim.getIndexKey(indexName, indexValue)

	existingData, err := sim.storageBackend.Get(key)
	if err != nil {
		return nil
	}

	entry, err := DecodeSecondaryIndexEntry(existingData)
	if err != nil {
		return fmt.Errorf("failed to decode index entry: %w", err)
	}

	newPrimaryKeys := make([][]byte, 0)
	for _, pk := range entry.PrimaryKeys {
		if !bytes.Equal(pk, primaryKey) {
			newPrimaryKeys = append(newPrimaryKeys, pk)
		}
	}

	if len(newPrimaryKeys) == 0 {
		return sim.storageBackend.Delete(key)
	}

	entry.PrimaryKeys = newPrimaryKeys
	encoded, err := EncodeSecondaryIndexEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to encode index entry: %w", err)
	}

	return sim.storageBackend.Put(key, encoded)
}

func (sim *SecondaryIndexManager) GetPrimaryKeysForIndexValue(indexName string, indexValue []byte) ([][]byte, error) {
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	key := sim.getIndexKey(indexName, indexValue)

	existingData, err := sim.storageBackend.Get(key)
	if err != nil || existingData == nil {
		return [][]byte{}, nil
	}

	entry, err := DecodeSecondaryIndexEntry(existingData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode index entry: %w", err)
	}

	return entry.PrimaryKeys, nil
}

func (sim *SecondaryIndexManager) RangeQueryIndex(
	indexName string,
	startValue []byte,
	endValue []byte,
	inclusive bool,
) ([][]byte, error) {
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	result := make([][]byte, 0)


	if bytes.Equal(startValue, endValue) && inclusive {
		return sim.GetPrimaryKeysForIndexValue(indexName, startValue)
	}

	return result, fmt.Errorf("range queries require storage engine iterator support")
}

func (sim *SecondaryIndexManager) ClearIndex(indexName string) error {
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return err
	}

	return fmt.Errorf("clear index requires storage engine iterator support")
}

func (sim *SecondaryIndexManager) getIndexKey(indexName string, indexValue []byte) []byte {
	prefix := sim.getIndexKeyPrefix(indexName)
	return append([]byte(prefix), indexValue...)
}

func (sim *SecondaryIndexManager) getIndexKeyPrefix(indexName string) string {
	return fmt.Sprintf("__index:%s:%s:", sim.tableName, indexName)
}

func EncodeSecondaryIndexEntry(entry *SecondaryIndexEntry) ([]byte, error) {
	var buf bytes.Buffer

	numKeys := uint32(len(entry.PrimaryKeys))
	if err := binary.Write(&buf, binary.BigEndian, numKeys); err != nil {
		return nil, err
	}

	for _, pk := range entry.PrimaryKeys {
		pkLen := uint32(len(pk))
		if err := binary.Write(&buf, binary.BigEndian, pkLen); err != nil {
			return nil, err
		}

		if _, err := buf.Write(pk); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func DecodeSecondaryIndexEntry(data []byte) (*SecondaryIndexEntry, error) {
	buf := bytes.NewReader(data)

	var numKeys uint32
	if err := binary.Read(buf, binary.BigEndian, &numKeys); err != nil {
		return nil, err
	}

	primaryKeys := make([][]byte, numKeys)

	for i := uint32(0); i < numKeys; i++ {
		var pkLen uint32
		if err := binary.Read(buf, binary.BigEndian, &pkLen); err != nil {
			return nil, err
		}

		pk := make([]byte, pkLen)
		if _, err := buf.Read(pk); err != nil {
			return nil, err
		}

		primaryKeys[i] = pk
	}

	return &SecondaryIndexEntry{
		PrimaryKeys: primaryKeys,
	}, nil
}

func (sim *SecondaryIndexManager) ValidateIndex(indexName string) error {
	return fmt.Errorf("index validation requires storage engine iterator support")
}

func (sim *SecondaryIndexManager) RebuildIndex(indexName string) error {
	return fmt.Errorf("index rebuild requires storage engine iterator support")
}

func (sim *SecondaryIndexManager) CompactIndex(indexName string) error {
	return fmt.Errorf("index compaction requires storage engine iterator support")
}

type IndexStats struct {
	IndexName  string
	TableName  string
	NumEntries int64
	AvgKeySize int64
	SizeBytes  int64
}

func (sim *SecondaryIndexManager) GetIndexStats(indexName string) (*IndexStats, error) {
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("index statistics require storage engine iterator support")
}

func (sim *SecondaryIndexManager) GetAllIndexNames() []string {
	var names []string
	for indexName, idx := range sim.tableSchema.Indexes {
		if idx.Type != IndexTypePrimary {
			names = append(names, indexName)
		}
	}
	sort.Strings(names)
	return names
}
