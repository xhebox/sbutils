package data_types

import (
	"io"

	"github.com/xhebox/bstruct/byteorder"
)

type Varint int64

func (this *Varint) Read(rd io.Reader, endian byteorder.ByteOrder) error {
	i, e := byteorder.Varint(rd, endian)
	*this = Varint(i)
	return e
}

func (this *Varint) ReadBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	n, l, e := endian.Varint(buf)
	*this = Varint(n)
	return l, e
}

func (this *Varint) Write(wt io.Writer, endian byteorder.ByteOrder) error {
	return byteorder.PutVarint(wt, endian, int64(*this))
}

func (this *Varint) WriteBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	return endian.PutVarint(buf, int64(*this)), nil
}
