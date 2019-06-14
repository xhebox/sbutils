package data_types

import (
	"encoding/hex"
	"errors"
	"io"

	"github.com/xhebox/bstruct/byteorder"
)

type UUID [16]byte

func (this *UUID) Read(rd io.Reader, endian byteorder.ByteOrder) error {
	if _, e := io.ReadFull(rd, (*this)[:]); e != nil {
		return e
	}

	return nil
}

func (this *UUID) ReadBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	if len(buf) < len(*this) {
		return 0, errors.New("not enough bytes to read")
	}

	return copy((*this)[:], buf), nil
}

func (this *UUID) Write(wt io.Writer, endian byteorder.ByteOrder) error {
	_, e := wt.Write((*this)[:])
	if e != nil {
		return e
	}

	return nil
}

func (this *UUID) WriteBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	if len(buf) < len(*this) {
		return 0, errors.New("not enough bytes to write")
	}

	return copy(buf, (*this)[:]), nil
}

func (this *UUID) String() string {
	return hex.EncodeToString((*this)[:])
}
