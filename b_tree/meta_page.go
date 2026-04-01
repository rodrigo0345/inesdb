package btree

import (
	"encoding/binary"
	"os"
)

type MetaPage struct {
	data []byte
}

func NewMetaPage() *MetaPage {
	pageSize := uint32(os.Getpagesize())
	if pageSize < 4096 {
		pageSize = DefaultPageSize
	}

	m := &MetaPage{
		data: make([]byte, pageSize),
	}

	m.SetPageType(TypeMeta)
	m.SetMagicNumber(MagicNumber)
	m.SetVersion(1)
	m.SetPageSize(pageSize)
	m.SetRootPage(1)

	return m
}

func (m *MetaPage) PageType() PageType {
	return PageType(binary.LittleEndian.Uint16(m.data[MetaTypeOffset:]))
}

func (m *MetaPage) MagicNumber() uint32 {
	return binary.LittleEndian.Uint32(m.data[MetaMagicOffset:])
}

func (m *MetaPage) Version() uint16 {
	return binary.LittleEndian.Uint16(m.data[MetaVersionOffset:])
}

func (m *MetaPage) PageSize() uint32 {
	return binary.LittleEndian.Uint32(m.data[MetaPageSizeOffset:])
}

func (m *MetaPage) RootPage() uint64 {
	return binary.LittleEndian.Uint64(m.data[MetaRootPageOffset:])
}

func (m *MetaPage) SetPageType(ptype PageType) {
	binary.LittleEndian.PutUint16(m.data[MetaTypeOffset:], uint16(ptype))
}

func (m *MetaPage) SetMagicNumber(magic uint32) {
	binary.LittleEndian.PutUint32(m.data[MetaMagicOffset:], magic)
}

func (m *MetaPage) SetVersion(version uint16) {
	binary.LittleEndian.PutUint16(m.data[MetaVersionOffset:], version)
}

func (m *MetaPage) SetPageSize(size uint32) {
	binary.LittleEndian.PutUint32(m.data[MetaPageSizeOffset:], size)
}

func (m *MetaPage) SetRootPage(rootID uint64) {
	binary.LittleEndian.PutUint64(m.data[MetaRootPageOffset:], rootID)
}
