package main

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/xhebox/bstruct/byteorder"
	"github.com/xhebox/sbutils/lib/packet"
)

var (
	ErrUnknownType = errors.New("packet of unknown type")
)

type FilterCallback func(*Filter, packet.Packet)

type Filter struct {
	in  io.Reader
	out io.Writer
	// filter function
	cb  FilterCallback
	End byteorder.ByteOrder
}

func NewFilter(in io.Reader, out io.Writer, cb FilterCallback) *Filter {
	r := &Filter{}
	r.in = in
	r.out = out
	r.cb = cb
	r.End = byteorder.BigEndian
	return r
}

func (r *Filter) ReadPacket() (*packet.BasePacket, error) {
	pktType, e := byteorder.Uint8(r.in)
	if e != nil {
		return nil, errors.WithStack(e)
	}

	length, e := byteorder.Varint(r.in, r.End)
	if e != nil {
		return nil, errors.WithStack(e)
	}
	abslength := int64(length)
	if abslength < 0 {
		abslength = -abslength
	}

	buf := &bytes.Buffer{}

	in := io.LimitReader(r.in, abslength)

	if length < 0 {
		zin, e := zlib.NewReader(in)
		if e != nil {
			return nil, errors.WithStack(e)
		}

		defer zin.Close()
		in = zin
	}

	_, e = io.Copy(buf, in)
	if e != nil {
		return nil, errors.WithStack(e)
	}

	return &packet.BasePacket{Type: pktType, Buf: buf.Bytes()}, nil
}

func (r *Filter) WritePacket(pkt *packet.BasePacket) error {
	e := byteorder.PutUint8(r.out, pkt.Type)
	if e != nil {
		return errors.WithStack(e)
	}

	e = byteorder.PutVarint(r.out, r.End, int64(len(pkt.Buf)))
	if e != nil {
		return errors.WithStack(e)
	}

	_, e = r.out.Write(pkt.Buf)
	if e != nil {
		return errors.WithStack(e)
	}

	return nil
}

// loop function
func (r *Filter) Loop() error {
	for {
		pkt, e := r.ReadPacket()
		if e != nil {
			return errors.WithStack(e)
		}

		p, e := pkt.Parse(r.End)
		if e != nil {
			fmt.Println(e)
		}

		if p != nil {
			if r.cb != nil {
				r.cb(r, p)
			}

			pkt, e = p.Pack(r.End)
			if e != nil {
				return errors.WithStack(e)
			}
		} else if pkt.Type == 13 {
			ioutil.WriteFile("../out", pkt.Buf, 0644)
		}

		if e := r.WritePacket(pkt); e != nil {
			return errors.WithStack(e)
		}
	}
}
