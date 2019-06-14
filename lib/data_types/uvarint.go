package data_types

import (
	"errors"
	"io"

	"github.com/xhebox/bstruct/byteorder"
)

type UVarint uint64

func (this *UVarint) Read(rd io.Reader, endian byteorder.ByteOrder) error {
	n, e := byteorder.UVarint(rd, endian)
	*this = UVarint(n)
	return e
}

func (this *UVarint) ReadBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	n, l, e := endian.UVarint(buf)
	*this = UVarint(n)
	return l, e
}

func (this *UVarint) Write(wt io.Writer, endian byteorder.ByteOrder) error {
	return byteorder.PutUVarint(wt, endian, uint64(*this))
}

func (this *UVarint) WriteBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	if len(buf) < byteorder.VMAXLEN {
		return 0, errors.New("space not enough")
	}

	return endian.PutUVarint(buf, uint64(*this)), nil
}
