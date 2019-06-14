package sbvj01

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"
	"github.com/xhebox/bstruct/byteorder"
	. "github.com/xhebox/sbutils/lib/data_types"
)

var (
	Magic = []byte{'S', 'B', 'V', 'J', '0', '1'}
)

const (
	NullT byte = iota + 1
	NumberT
	BoolT
	VarintT
	StringT
	ArrayT
	ObjectT
)

type VerJsonHdr struct {
	Id      String `json:"id"`
	Endian  byteorder.ByteOrder
	Version int32 `json:"version"`
}

func ReadHdr(rd io.Reader) (r VerJsonHdr, e error) {
	r.Id, e = ReadString(rd, byteorder.BigEndian)
	if e != nil {
		return
	}

	var end byte

	end, e = byteorder.Uint8(rd)
	if e != nil {
		return
	}

	if end == 0 {
		r.Endian = byteorder.BigEndian
	} else {
		r.Endian = byteorder.LittleEndian
	}

	r.Version, e = byteorder.Int32(rd, r.Endian)
	if e != nil {
		return
	}

	return
}

func WriteHdr(wt io.Writer, r VerJsonHdr) error {
	strlen := len(r.Id)

	if e := byteorder.PutUVarint(wt, r.Endian, uint64(strlen)); e != nil {
		return e
	}

	if e := byteorder.PutBool(wt, r.Endian == byteorder.LittleEndian); e != nil {
		return e
	}

	if e := byteorder.PutInt32(wt, r.Endian, int32(r.Version)); e != nil {
		return e
	}

	return nil
}

func Read(rd io.Reader, endian byteorder.ByteOrder) (interface{}, error) {
	typ, e := byteorder.Uint8(rd)
	if e != nil {
		return nil, e
	}

	switch typ {
	case NullT:
		return nil, nil
	case NumberT:
		return byteorder.Float64(rd, endian)
	case BoolT:
		return byteorder.Bool(rd)
	case VarintT:
		return byteorder.Varint(rd, endian)
	case StringT:
		return ReadString(rd, endian)
	case ArrayT:
		return ReadArray(rd, endian)
	case ObjectT:
		return ReadObject(rd, endian)
	default:
		return nil, errors.Errorf("unknown type %d", typ)
	}
}

func ReadArray(rd io.Reader, endian byteorder.ByteOrder) ([]interface{}, error) {
	cnt, e := byteorder.UVarint(rd, endian)
	if e != nil {
		return nil, e
	}

	r := []interface{}{}

	for i, c := 0, int(cnt); i < c; i++ {
		value, e := Read(rd, endian)
		if e != nil {
			return nil, e
		}

		r = append(r, value)
	}

	return r, nil
}

func ReadObject(rd io.Reader, endian byteorder.ByteOrder) (map[String]interface{}, error) {
	cnt, e := byteorder.UVarint(rd, endian)
	if e != nil {
		return nil, e
	}

	r := map[String]interface{}{}

	for i, c := 0, int(cnt); i < c; i++ {
		key, e := ReadString(rd, endian)
		if e != nil {
			return nil, e
		}

		value, e := Read(rd, endian)
		if e != nil {
			return nil, e
		}

		r[key] = value
	}

	return r, nil
}

func Write(wt io.Writer, endian byteorder.ByteOrder, anything interface{}) error {
	switch n := anything.(type) {
	case nil:
		if e := byteorder.PutUint8(wt, NullT); e != nil {
			return e
		}
	case json.Number:
		m, e := n.Int64()
		if e == nil {
			if e := byteorder.PutUint8(wt, VarintT); e != nil {
				return e
			}

			return byteorder.PutVarint(wt, endian, m)
		}

		if e := byteorder.PutUint8(wt, NumberT); e != nil {
			return e
		}

		p, e := n.Float64()
		if e != nil {
			return e
		}

		return byteorder.PutFloat64(wt, endian, p)
	case float64:
		if e := byteorder.PutUint8(wt, NumberT); e != nil {
			return e
		}

		return byteorder.PutFloat64(wt, endian, n)
	case Varint:
		if e := byteorder.PutUint8(wt, VarintT); e != nil {
			return e
		}

		return n.Write(wt, endian)
	case int64:
		if e := byteorder.PutUint8(wt, VarintT); e != nil {
			return e
		}

		return byteorder.PutVarint(wt, endian, n)
	case bool:
		if e := byteorder.PutUint8(wt, BoolT); e != nil {
			return e
		}

		return byteorder.PutBool(wt, n)
	case String:
		if e := byteorder.PutUint8(wt, StringT); e != nil {
			return e
		}

		return n.Write(wt, endian)
	case string:
		if e := byteorder.PutUint8(wt, StringT); e != nil {
			return e
		}

		r := String(n)
		return r.Write(wt, endian)
	case []interface{}:
		if e := byteorder.PutUint8(wt, ArrayT); e != nil {
			return e
		}

		return WriteArray(wt, endian, n)
	case map[String]interface{}:
		if e := byteorder.PutUint8(wt, ObjectT); e != nil {
			return e
		}

		return WriteObject(wt, endian, n)
	case map[string]interface{}:
		if e := byteorder.PutUint8(wt, ObjectT); e != nil {
			return e
		}

		return writeobj(wt, endian, n)
	default:
		return errors.Errorf("unknown type %+v", anything)
	}

	return nil
}

func WriteArray(wt io.Writer, endian byteorder.ByteOrder, array []interface{}) error {
	arrlen := len(array)

	e := byteorder.PutUVarint(wt, endian, uint64(arrlen))
	if e != nil {
		return e
	}

	for i := 0; i < arrlen; i++ {
		if e := Write(wt, endian, array[i]); e != nil {
			return e
		}
	}

	return nil
}

func WriteObject(wt io.Writer, endian byteorder.ByteOrder, object map[String]interface{}) error {
	e := byteorder.PutUVarint(wt, endian, uint64(len(object)))
	if e != nil {
		return e
	}

	for k, v := range object {
		if e := k.Write(wt, endian); e != nil {
			return e
		}

		if e := Write(wt, endian, v); e != nil {
			return e
		}
	}

	return nil
}

func writeobj(wt io.Writer, endian byteorder.ByteOrder, object map[string]interface{}) error {
	e := byteorder.PutUVarint(wt, endian, uint64(len(object)))
	if e != nil {
		return e
	}

	for k, v := range object {
		r := String(k)

		if e := r.Write(wt, endian); e != nil {
			return e
		}

		if e := Write(wt, endian, v); e != nil {
			return e
		}
	}

	return nil
}
