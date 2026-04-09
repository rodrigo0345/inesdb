package btree

import (
	"encoding/binary"
	"os"
)

const (
	MetaTypeOffset     = 0  // 2 bytes (PageType)
	MetaMagicOffset    = 2  // 4 bytes (uint32)
	MetaVersionOffset  = 6  // 2 bytes (uint16)
	MetaPageSizeOffset = 8  // 4 bytes (uint32)
	MetaRootPageOffset = 12 // 8 bytes (uint64)
)

type MetaLogicPage struct {
	data []byte
}

func NewMetaPage() *MetaLogicPage {
	return NewMetaPageWithSize(0)
}

func NewMetaPageWithSize(pageSize uint32) *MetaLogicPage {
	if pageSize == 0 {
		pageSize = uint32(os.Getpagesize())
		if pageSize < DefaultPageSize {
			pageSize = DefaultPageSize
		}
	}

	m := &MetaLogicPage{
		data: make([]byte, pageSize),
	}

	m.SetPageType(TypeMeta)
	m.SetMagicNumber(MagicNumber)
	m.SetVersion(1)
	m.SetPageSize(pageSize)
	m.SetRootPage(0) // 0 means tree is empty, will be initialized on first put

	return m
}

func (m *MetaLogicPage) PageType() LogicPageType {
	return LogicPageType(binary.LittleEndian.Uint16(m.data[MetaTypeOffset:]))
}

func (m *MetaLogicPage) MagicNumber() uint32 {
	return binary.LittleEndian.Uint32(m.data[MetaMagicOffset:])
}

func (m *MetaLogicPage) Version() uint16 {
	return binary.LittleEndian.Uint16(m.data[MetaVersionOffset:])
}

func (m *MetaLogicPage) PageSize() uint32 {
	return binary.LittleEndian.Uint32(m.data[MetaPageSizeOffset:])
}

func (m *MetaLogicPage) RootPage() uint64 {
	return binary.LittleEndian.Uint64(m.data[MetaRootPageOffset:])
}

func (m *MetaLogicPage) SetPageType(ptype LogicPageType) {
	binary.LittleEndian.PutUint16(m.data[MetaTypeOffset:], uint16(ptype))
}

func (m *MetaLogicPage) SetMagicNumber(magic uint32) {
	binary.LittleEndian.PutUint32(m.data[MetaMagicOffset:], magic)
}

func (m *MetaLogicPage) SetVersion(version uint16) {
	binary.LittleEndian.PutUint16(m.data[MetaVersionOffset:], version)
}

func (m *MetaLogicPage) SetPageSize(size uint32) {
	binary.LittleEndian.PutUint32(m.data[MetaPageSizeOffset:], size)
}

func (m *MetaLogicPage) SetRootPage(rootID uint64) {
	binary.LittleEndian.PutUint64(m.data[MetaRootPageOffset:], rootID)
}
