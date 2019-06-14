package data_types

import (
	"errors"
	"fmt"
	"io"

	"github.com/xhebox/bstruct/byteorder"
)

type Maybe struct {
	Content interface {
		Reader
		BufReader
		Writer
		BufWriter
	}
}

func (this *Maybe) Read(rd io.Reader, endian byteorder.ByteOrder) error {
	var c [1]byte
	if _, e := io.ReadFull(rd, c[:]); e != nil {
		return e
	}

	if c[0] == 0 {
		return nil
	}

	if this.Content == nil {
		return errors.New("need to specific a type first")
	}

	return this.Content.Read(rd, endian)
}

func (this *Maybe) ReadBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	if buf[0] == 0 {
		return 1, nil
	}

	if this.Content == nil {
		return 1, errors.New("need to specific a type first")
	}

	l, e := this.Content.ReadBuf(buf[1:], endian)
	return l + 1, e
}

func (this *Maybe) Write(wt io.Writer, endian byteorder.ByteOrder) error {
	var c [1]byte

	b := this.Content == nil

	c[0] = byteorder.Bool2Byte(b)

	_, e := wt.Write(c[:])
	if e != nil {
		return e
	}

	if b {
		return nil
	}

	return this.Content.Write(wt, endian)
}

func (this *Maybe) WriteBuf(buf []byte, endian byteorder.ByteOrder) (int, error) {
	b := this.Content == nil

	buf[0] = byteorder.Bool2Byte(b)

	if b {
		return 1, nil
	}

	l, e := this.Content.WriteBuf(buf[1:], endian)
	return l + 1, e
}

func (this *Maybe) String() string {
	return fmt.Sprint(this.Content)
}
