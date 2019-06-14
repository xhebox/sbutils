package packet

import "github.com/xhebox/bstruct/byteorder"

type ProtocolRequestPacket struct {
	Version uint32
}

func (r *ProtocolRequestPacket) Unpack(e byteorder.ByteOrder, buf []byte) error {
	r.Version = e.Uint32(buf)
	return nil
}

func (r *ProtocolRequestPacket) Pack(e byteorder.ByteOrder) (*BasePacket, error) {
	c := &BasePacket{
		Type: ProtocolRequest,
		Buf:  make([]byte, 4),
	}

	e.PutUint32(c.Buf, r.Version)

	return c, nil
}
