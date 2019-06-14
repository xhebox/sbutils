package data_types

import (
	"io"

	"github.com/xhebox/bstruct/byteorder"
)

type Reader interface {
	Read(io.Reader, byteorder.ByteOrder) error
}

type BufReader interface {
	ReadBuf([]byte, byteorder.ByteOrder) (int, error)
}

type Writer interface {
	Write(io.Writer, byteorder.ByteOrder) error
}

type BufWriter interface {
	WriteBuf([]byte, byteorder.ByteOrder) (int, error)
}
