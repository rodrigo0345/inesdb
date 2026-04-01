package btree

import (
	"errors"
	"io"
	"os"
)

// Logic responsible for managing page storage location and retrieving
var (
	ErrPageBounds      = errors.New("page ID out of bounds")
	ErrInvalidPageSize = errors.New("invalid page size")
)

type Pager struct {
	file     *os.File
	pageSize uint32
	numPages uint64
	inMemory bool
	memPages [][]byte
}

func NewPager(filePath string, pageSize uint32, inMemory bool) (*Pager, error) {
	if pageSize == 0 {
		return nil, ErrInvalidPageSize
	}

	p := &Pager{
		pageSize: pageSize,
		inMemory: inMemory,
	}

	if inMemory {
		p.memPages = make([][]byte, 0)
		return p, nil
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	p.file = file

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	p.numPages = uint64(stat.Size()) / uint64(pageSize)
	return p, nil
}

func (p *Pager) FetchPage(pageID uint64) ([]byte, error) {
	if pageID >= p.numPages {
		return nil, ErrPageBounds
	}

	pageData := make([]byte, p.pageSize)

	if p.inMemory {
		copy(pageData, p.memPages[pageID])
		return pageData, nil
	}

	offset := int64(pageID) * int64(p.pageSize)
	_, err := p.file.ReadAt(pageData, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return pageData, nil
}

func (p *Pager) WritePage(pageID uint64, data []byte) error {
	if len(data) != int(p.pageSize) {
		return ErrInvalidPageSize
	}

	if pageID >= p.numPages {
		return ErrPageBounds
	}

	if p.inMemory {
		copy(p.memPages[pageID], data)
		return nil
	}

	offset := int64(pageID) * int64(p.pageSize)
	_, err := p.file.WriteAt(data, offset)
	return err
}

func (p *Pager) AllocatePage() (uint64, []byte, error) {
	pageID := p.numPages
	p.numPages++

	pageData := make([]byte, p.pageSize)

	if p.inMemory {
		p.memPages = append(p.memPages, pageData)
		return pageID, pageData, nil
	}

	err := p.WritePage(pageID, pageData)
	if err != nil {
		p.numPages--
		return 0, nil, err
	}

	return pageID, pageData, nil
}

// write all dirty (pages not saved on storage) pages to disk. For in-memory, this is a no-op.
func (p *Pager) Sync() error {
	if p.inMemory {
		return nil
	}
	return p.file.Sync()
}

func (p *Pager) Close() error {
	if p.inMemory {
		p.memPages = nil
		p.numPages = 0
		return nil
	}
	if p.file != nil {
		return p.file.Close()
	}
	return nil
}

func (p *Pager) PageCount() uint64 {
	return p.numPages
}
