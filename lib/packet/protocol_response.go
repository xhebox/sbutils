package packet

import "github.com/xhebox/bstruct/byteorder"

type ProtocolResponsePacket struct {
	Available bool
}

func (r *ProtocolResponsePacket) Unpack(e byteorder.ByteOrder, buf []byte) error {
	r.Available = buf[0] != 0
	return nil
}

func (r *ProtocolResponsePacket) Pack(e byteorder.ByteOrder) (*BasePacket, error) {
	c := &BasePacket{
		Type: ProtocolResponse,
		Buf:  make([]byte, 1),
	}

	if r.Available {
		c.Buf[0] = 1
	} else {
		c.Buf[0] = 0
	}

	return c, nil
}
