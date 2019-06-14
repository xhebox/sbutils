package blockfile

import (
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

type BlockFile struct {
	hdrsz    int
	blksz    int
	blks     uint
	filesize int64
	file     *os.File
	fmap     mmap.MMap
}

func NewBlockFile(filename string, hdrsz int) (h *BlockFile, e error) {
	h = &BlockFile{
		hdrsz: hdrsz,
		blks:  0,
		fmap:  nil,
	}

	h.file, e = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		return nil, errors.Wrapf(e, "fail to read")
	}

	fileinfo, e := h.file.Stat()
	if e != nil {
		return nil, errors.Wrapf(e, "fail to stat")
	}

	h.filesize = fileinfo.Size()

	if h.filesize < int64(hdrsz) {
		h.filesize = int64(hdrsz)

		if e = h.file.Truncate(h.filesize); e != nil {
			return nil, errors.Wrapf(e, "fail to truncate")
		}
	}

	h.fmap, e = mmap.Map(h.file, mmap.RDWR, 0)
	if e != nil {
		return nil, errors.Wrapf(e, "fail to mmap")
	}

	return h, nil
}

func (h *BlockFile) SetBlksz(blksz int) {
	h.blksz = blksz
	h.blks = uint((len(h.fmap) - h.hdrsz) / h.blksz)

	if (len(h.fmap)-h.hdrsz)%h.blksz != 0 {
		panic("block is not a multiple")
	}
}

func (h *BlockFile) Grow(blks uint) error {
	var e error

	h.filesize += int64(int(blks)) * int64(h.blksz)

	if e := h.fmap.Unmap(); e != nil {
		return errors.Wrapf(e, "fail to flush")
	}

	if e = h.file.Truncate(h.filesize); e != nil {
		return errors.Wrapf(e, "fail to truncate")
	}

	h.fmap, e = mmap.Map(h.file, mmap.RDWR, 0)
	if e != nil {
		return errors.Wrapf(e, "fail to mmap")
	}

	h.blks += blks
	return nil
}

func (h *BlockFile) Resize(blks uint) error {
	var e error

	h.filesize = int64(h.hdrsz) + int64(int(blks))*int64(h.blksz)

	if e := h.fmap.Unmap(); e != nil {
		return errors.Wrapf(e, "fail to flush")
	}

	if e = h.file.Truncate(h.filesize); e != nil {
		return errors.Wrapf(e, "fail to truncate")
	}

	h.fmap, e = mmap.Map(h.file, mmap.RDWR, 0)
	if e != nil {
		return errors.Wrapf(e, "fail to mmap")
	}

	h.blks = blks
	return nil
}

func (h *BlockFile) Cap() uint {
	return h.blks
}

func (h *BlockFile) Size() int64 {
	return int64(h.hdrsz) + int64(h.blks)*int64(h.blksz)
}

func (h *BlockFile) Header() []byte {
	return h.fmap[:h.hdrsz]
}

func (h *BlockFile) Block(ptr uint) []byte {
	if ptr > h.blks {
		panic("overflow")
	}

	off := int64(h.hdrsz)
	off += int64(ptr) * int64(h.blksz)
	offend := off + int64(h.blksz)
	return h.fmap[off:offend]
}

func (h *BlockFile) Flush() error {
	return h.fmap.Flush()
}

func (h *BlockFile) Close() error {
	e := h.fmap.Unmap()
	if e != nil {
		return e
	}

	return h.file.Close()
}
