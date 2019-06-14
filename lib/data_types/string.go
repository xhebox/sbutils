package data_types

import (
	"io"

	"github.com/xhebox/bstruct/byteorder"
)

type String string

func ReadString(rd io.Reader, endian byteorder.ByteOrder) (String, error) {
	buf := ByteArray{}

	e := buf.Read(rd, endian)
	if e != nil {
		return "", e
	}

	return String(buf), nil
}

func (this *String) Read(rd io.Reader, endian byteorder.ByteOrder) error {
	buf := ByteArray{}

	e := buf.Read(rd, endian)
	if e != nil {
		return e
	}

	*this = String(buf)
	return nil
}

func (this *String) ReadBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	arr := &ByteArray{}

	l, e := arr.ReadBuf(buf, endian)
	if e != nil {
		return l, e
	}

	*this = String(*arr)
	return l, nil
}

func (this *String) Write(wt io.Writer, endian byteorder.ByteOrder) error {
	h := ByteArray(*this)
	return h.Write(wt, endian)
}

func (this *String) WriteBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	h := ByteArray(*this)
	return h.WriteBuf(buf, endian)
}
