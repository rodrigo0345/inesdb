package bplus_tree_backend

/***
Memory Offset (Bytes) - Slotted Pages
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

type LogicPageType uint16

const (
	DefaultPageSize        = 4096       // 4KB by default
	MagicNumber     uint32 = 0x6F6D6167 // "omag" in ASCII
	SlotSize               = 2          // Each slot is 2 bytes, pointing to the cell's starting offset

	TypeLeaf     LogicPageType = 1
	TypeInternal LogicPageType = 2
	TypeMeta     LogicPageType = 3

	// internal cell header layout
	// used to be able to store variable size records
	CellKeyLenSize = 2 // 2 bytes for Key Length
	CellValLenSize = 4 // 4 bytes for Value Length
	CellHeaderSize = 6 // Total cell metadata size
)
