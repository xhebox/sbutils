package packet

import (
	"bytes"
	"compress/zlib"
	"io"

	"github.com/pkg/errors"
	"github.com/xhebox/bstruct/byteorder"
)

func ReadPacket(rd io.Reader, end byteorder.ByteOrder) (*BasePacket, error) {
	pktType, e := byteorder.Uint8(rd)
	if e != nil {
		return nil, errors.WithStack(e)
	}

	length, e := byteorder.Varint(rd, end)
	if e != nil {
		return nil, errors.WithStack(e)
	}

	if length > 0 {
		buf := make([]byte, length)

		if _, e := io.ReadFull(rd, buf); e != nil {
			return nil, e
		}

		return &BasePacket{Type: pktType, Buf: buf}, nil
	}

	zin, e := zlib.NewReader(io.LimitReader(rd, -length))
	if e != nil {
		return nil, errors.WithStack(e)
	}
	defer zin.Close()

	buffer := &bytes.Buffer{}

	_, e = buffer.ReadFrom(zin)
	if e != nil {
		return nil, errors.WithStack(e)
	}

	return &BasePacket{Type: pktType, Buf: buffer.Bytes()}, nil
}

func WritePacket(wt io.Writer, end byteorder.ByteOrder, pkt *BasePacket) error {
	e := byteorder.PutUint8(wt, pkt.Type)
	if e != nil {
		return errors.WithStack(e)
	}

	length := len(pkt.Buf)

	if length < 0x40 {
		e = byteorder.PutVarint(wt, end, int64(length))
		if e != nil {
			return e
		}

		_, e = wt.Write(pkt.Buf)
		if e != nil {
			return errors.WithStack(e)
		}

		return nil
	}

	tmpbuf := &bytes.Buffer{}

	zout := zlib.NewWriter(tmpbuf)

	_, e = zout.Write(pkt.Buf)
	if e != nil {
		return errors.WithStack(e)
	}

	zout.Close()

	e = byteorder.PutVarint(wt, end, -int64(tmpbuf.Len()))
	if e != nil {
		return e
	}

	_, e = wt.Write(tmpbuf.Bytes())
	if e != nil {
		return errors.WithStack(e)
	}

	return nil
}

type Packet interface {
	Unpack(byteorder.ByteOrder, []byte) error
	Pack(byteorder.ByteOrder) (*BasePacket, error)
}

type BasePacket struct {
	Type byte
	Buf  []byte
}

func (this BasePacket) Parse(end byteorder.ByteOrder) (Packet, error) {
	var p Packet

	switch this.Type {
	case ProtocolRequest:
		p = &ProtocolRequestPacket{}
	case ProtocolResponse:
		p = &ProtocolResponsePacket{}
	case ConnectFailure:
		p = &ConnectFailurePacket{}
	default:
		return nil, errors.Errorf("unknown packet type: %v", this.Type)
	}

	if e := p.Unpack(end, this.Buf); e != nil {
		return nil, e
	}

	return p, nil
}

/*
type UniverseTimeUpdatePacket struct {
	Time uint32
}

func (r *UniverseTimeUpdatePacket) Read(e byteorder.ByteOrder, pkt []byte) error {
	r.Time = e.Uint32(pkt)
	return nil
}

func (r *UniverseTimeUpdatePacket) Write(e byteorder.ByteOrder) (byte, []byte, error) {
	buf := []byte{0, 0, 0, 0, 0}

	e.PutVarintB(buf, 4)
	e.PutUint32(buf, r.Time)

	return UniverseTimeUpdate, buf, nil
}


type block struct{
	u1 []byte
	Filled bool
	u3 []byte
}

type ClientConnectPacket struct {
	Digest         []byte
	DigestMismatch bool
	UUID           []byte
	Name           string
	Race           string

	//    Star::MapMixin<
	//     Star::FlatHashMap<
	//       Star::ByteArray,
	//       Star::Maybe<Star::ByteArray>,
	//       Star::hash<Star::ByteArray,void>,
	//       std::equal_to<Star::ByteArray>,
	//       std::allocator<std::pair<Star::ByteArray const,Star::Maybe<Star::ByteArray>>>
	//     >
	//   >,
	u1 uint64
	u2 []block
}

func (r *ClientConnectPacket) Read(p []byte) {
	var l uint64
	r.Digest, l = ByteArray(p)
	b := p[l:]

	r.DigestMismatch = Bool(b)
	b = b[1:]

	r.UUID = Bytes(b, 16)
	b = b[16:]

	r.Name, l = String(b)
	b = b[l:]

	r.Race, l = String(b)
	b = b[l:]

	r.u1, l = Uvarint(b)
	b = b[l:]

	r.u2 = make([]block, r.u1)

	for i := uint64(0); i < r.u1; i++ {
		fmt.Println(i, r.u1)
		r.u2[i].u1, l = ByteArray(b)
		b = b[l:]

		r.u2[i].Filled = Bool(b)
		b = b[1:]

		if r.u2[i].Filled {
			r.u2[i].u3, l = ByteArray(b)
			b = b[l:]
		}
	}

	c, d := Uvarint(b)
	fmt.Printf("unknow parts, %x, %x, %x\n", c, d, len(b))
	fmt.Printf("%s\n", string(b[:100]))
}
*/
