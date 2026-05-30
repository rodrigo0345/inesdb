package btree



type LogicPageType uint16

const (
	DefaultPageSize        = 4096
	MagicNumber     uint32 = 0x6F6D6167
	SlotSize               = 2

	TypeLeaf     LogicPageType = 1
	TypeInternal LogicPageType = 2
	TypeMeta     LogicPageType = 3

	CellKeyLenSize     = 2
	CellValLenSize     = 4
	CellOverflowIDSize = 8
	CellHeaderSize     = 14
)
