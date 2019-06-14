package data_types

import (
	"io"

	"github.com/xhebox/bstruct/byteorder"
)

type ByteArray []byte

func (this *ByteArray) Read(rd io.Reader, endian byteorder.ByteOrder) error {
	u := UVarint(0)

	e := u.Read(rd, endian)
	if e != nil {
		return e
	}

	*this = make([]byte, u)

	if _, e := io.ReadFull(rd, *this); e != nil {
		return e
	}

	return nil
}

func (this *ByteArray) ReadBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	u := UVarint(0)

	l, e := u.ReadBuf(buf, endian)
	if e != nil {
		return 0, e
	}

	*this = make([]byte, u)

	copy(*this, buf[l:])

	return l + int(u), nil
}

func (this *ByteArray) Write(wt io.Writer, endian byteorder.ByteOrder) error {
	u := UVarint(uint(len(*this)))

	e := u.Write(wt, endian)
	if e != nil {
		return e
	}

	_, e = wt.Write(*this)
	if e != nil {
		return e
	}

	return nil
}

func (this *ByteArray) WriteBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	blen := len(*this)

	u := UVarint(blen)

	l, e := u.WriteBuf(buf, endian)
	if e != nil {
		return 0, e
	}

	copy(buf[l:], *this)

	return l + blen, nil
}
