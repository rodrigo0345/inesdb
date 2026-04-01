package btree

/***
Memory Offset (Bytes)
+-------------------------+ 0
| PAGE HEADER             |
| - Type (2 bytes)        |
| - Cell Count (2 bytes)  |
| - Free SpacePtr (2 bytes)|
| - RightSiblingID(8 bytes)|
+-------------------------+ 14
| SLOT ARRAY (Grows Down) |
| [Slot 0 Offset] (2 bytes)| -> Points to Cell 0
| [Slot 1 Offset] (2 bytes)| -> Points to Cell 1
| [Slot 2 Offset] (2 bytes)| -> Points to Cell 2
| ... (More Slots)        |
| ↓                       |
+-------------------------+ Free Space Offset (Dynamic)
|      FREE SPACE         |
|                         |
|      (Unallocated)      |
|                         |
+-------------------------+ Cell Boundary (Dynamic)
| ↑                       |
| ... (More Cells)        |
| [CELL 2 DATA]           |
| [CELL 1 DATA]           |
| [CELL 0 DATA]           |
+-------------------------+ 4096
*/

type PageType uint16

const (
	DefaultPageSize        = 4096       // 4KB by default
	MagicNumber     uint32 = 0x6F6D6167 // "omag" in ASCII

	TypeLeaf     PageType = 1
	TypeInternal PageType = 2
	TypeMeta     PageType = 3

	// meta page header layout
	MetaTypeOffset     = 0  // 2 bytes (PageType)
	MetaMagicOffset    = 2  // 4 bytes (uint32)
	MetaVersionOffset  = 6  // 2 bytes (uint16)
	MetaPageSizeOffset = 8  // 4 bytes (uint32)
	MetaRootPageOffset = 12 // 8 bytes (uint64)

	LeafHeaderTypeOffset      = 0  // 2 bytes (PageType)
	LeafHeaderCellsOffset     = 2  // 2 bytes (Cell Count)
	LeafHeaderFreeSpaceOffset = 4  // 2 bytes (Free Space Pointer)
	LeafHeaderSiblingOffset   = 6  // 8 bytes (Right Sibling Page ID)
	LeafHeaderSize            = 14 // Total header size for leaf pages
	SlotSize                  = 2  // Each slot is 2 bytes, pointing to the cell's starting offset

	// internal cell header layout
	CellKeyLenSize = 2 // 2 bytes for Key Length
	CellValLenSize = 4 // 4 bytes for Value Length
	CellHeaderSize = 6 // Total cell metadata size
)
