package packet

import (
	"github.com/xhebox/bstruct/byteorder"
	. "github.com/xhebox/sbutils/lib/common"
)

type ConnectFailurePacket struct {
	Execuse String
}

func (r *ConnectFailurePacket) Unpack(e byteorder.ByteOrder, buf []byte) (err error) {
	_, err = r.Execuse.ReadBuf(buf, e)
	return
}

func (r *ConnectFailurePacket) Pack(e byteorder.ByteOrder) (*BasePacket, error) {
	c := &BasePacket{
		Type: ConnectFailure,
		Buf:  make([]byte, len(r.Execuse)+byteorder.VMAXLEN),
	}

	if _, e := r.Execuse.WriteBuf(c.Buf, e); e != nil {
		return nil, e
	}

	return c, nil
}
