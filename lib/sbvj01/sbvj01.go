package sbvj01

import (
	"encoding/json"
	"io"
	"math"

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
	Id        String `json:"id"`
	Versioned bool   `json:"versioned"`
	Version   int32  `json:"version"`
}

func ReadHdr(rd io.Reader) (r VerJsonHdr, e error) {
	r.Id, e = ReadString(rd, byteorder.BigEndian)
	if e != nil {
		return
	}

	r.Versioned, e = byteorder.Bool(rd)
	if e != nil {
		return
	}

	if r.Versioned {
		r.Version, e = byteorder.Int32(rd, byteorder.BigEndian)
		if e != nil {
			return
		}
	}

	return
}

func WriteHdr(wt io.Writer, r VerJsonHdr) error {
	strlen := len(r.Id)

	if e := byteorder.PutUVarint(wt, byteorder.BigEndian, uint64(strlen)); e != nil {
		return e
	}

	if e := byteorder.PutBool(wt, r.Versioned); e != nil {
		return e
	}

	if r.Versioned {
		if e := byteorder.PutInt32(wt, byteorder.BigEndian, int32(r.Version)); e != nil {
			return e
		}
	}

	return nil
}

func Read(rd io.Reader) (interface{}, error) {
	typ, e := byteorder.Uint8(rd)
	if e != nil {
		return nil, e
	}

	switch typ {
	case NullT:
		return nil, nil
	case NumberT:
		r, e := byteorder.Float64(rd, byteorder.BigEndian)
		if math.IsNaN(r) {
			return "____NaN____", e
		} else if math.IsInf(r, 1) {
			return "____+Inf____", e
		} else if math.IsInf(r, -1) {
			return "____-Inf____", e
		}
		return r, e
	case BoolT:
		return byteorder.Bool(rd)
	case VarintT:
		return byteorder.Varint(rd, byteorder.BigEndian)
	case StringT:
		return ReadString(rd, byteorder.BigEndian)
	case ArrayT:
		return ReadArray(rd)
	case ObjectT:
		return ReadObject(rd)
	default:
		return nil, errors.Errorf("unknown type %d", typ)
	}
}

func ReadArray(rd io.Reader) ([]interface{}, error) {
	cnt, e := byteorder.UVarint(rd, byteorder.BigEndian)
	if e != nil {
		return nil, e
	}

	r := []interface{}{}

	for i, c := 0, int(cnt); i < c; i++ {
		value, e := Read(rd)
		if e != nil {
			return nil, e
		}

		r = append(r, value)
	}

	return r, nil
}

func ReadObject(rd io.Reader) (map[String]interface{}, error) {
	cnt, e := byteorder.UVarint(rd, byteorder.BigEndian)
	if e != nil {
		return nil, e
	}

	r := map[String]interface{}{}

	for i, c := 0, int(cnt); i < c; i++ {
		key, e := ReadString(rd, byteorder.BigEndian)
		if e != nil {
			return nil, e
		}

		value, e := Read(rd)
		if e != nil {
			return nil, e
		}

		r[key] = value
	}

	return r, nil
}

func Write(wt io.Writer, anything interface{}) error {
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

			return byteorder.PutVarint(wt, byteorder.BigEndian, m)
		}

		if e := byteorder.PutUint8(wt, NumberT); e != nil {
			return e
		}

		p, e := n.Float64()
		if e != nil {
			return e
		}

		return byteorder.PutFloat64(wt, byteorder.BigEndian, p)
	case float64:
		if e := byteorder.PutUint8(wt, NumberT); e != nil {
			return e
		}

		return byteorder.PutFloat64(wt, byteorder.BigEndian, n)
	case Varint:
		if e := byteorder.PutUint8(wt, VarintT); e != nil {
			return e
		}

		return n.Write(wt, byteorder.BigEndian)
	case int64:
		if e := byteorder.PutUint8(wt, VarintT); e != nil {
			return e
		}

		return byteorder.PutVarint(wt, byteorder.BigEndian, n)
	case bool:
		if e := byteorder.PutUint8(wt, BoolT); e != nil {
			return e
		}

		return byteorder.PutBool(wt, n)
	case String:
		switch n {
		case "____NaN____":
			if e := byteorder.PutUint8(wt, NumberT); e != nil {
				return e
			}

			return byteorder.PutFloat64(wt, byteorder.BigEndian, math.NaN())
		case "____+Inf____":
			if e := byteorder.PutUint8(wt, NumberT); e != nil {
				return e
			}

			return byteorder.PutFloat64(wt, byteorder.BigEndian, math.Inf(1))
		case "____-Inf____":
			if e := byteorder.PutUint8(wt, NumberT); e != nil {
				return e
			}

			return byteorder.PutFloat64(wt, byteorder.BigEndian, math.Inf(-1))
		default:
			if e := byteorder.PutUint8(wt, StringT); e != nil {
				return e
			}

			return n.Write(wt, byteorder.BigEndian)
		}
	case string:
		switch n {
		case "____NaN____":
			if e := byteorder.PutUint8(wt, NumberT); e != nil {
				return e
			}

			return byteorder.PutFloat64(wt, byteorder.BigEndian, math.NaN())
		case "____+Inf____":
			if e := byteorder.PutUint8(wt, NumberT); e != nil {
				return e
			}

			return byteorder.PutFloat64(wt, byteorder.BigEndian, math.Inf(1))
		case "____-Inf____":
			if e := byteorder.PutUint8(wt, NumberT); e != nil {
				return e
			}

			return byteorder.PutFloat64(wt, byteorder.BigEndian, math.Inf(-1))
		default:
			if e := byteorder.PutUint8(wt, StringT); e != nil {
				return e
			}

			r := String(n)
			return r.Write(wt, byteorder.BigEndian)
		}
	case []interface{}:
		if e := byteorder.PutUint8(wt, ArrayT); e != nil {
			return e
		}

		return WriteArray(wt, n)
	case map[String]interface{}:
		if e := byteorder.PutUint8(wt, ObjectT); e != nil {
			return e
		}

		return WriteObject(wt, n)
	case map[string]interface{}:
		if e := byteorder.PutUint8(wt, ObjectT); e != nil {
			return e
		}

		return writeobj(wt, n)
	default:
		return errors.Errorf("unknown type %+v", anything)
	}

	return nil
}

func WriteArray(wt io.Writer, array []interface{}) error {
	arrlen := len(array)

	e := byteorder.PutUVarint(wt, byteorder.BigEndian, uint64(arrlen))
	if e != nil {
		return e
	}

	for i := 0; i < arrlen; i++ {
		if e := Write(wt, array[i]); e != nil {
			return e
		}
	}

	return nil
}

func WriteObject(wt io.Writer, object map[String]interface{}) error {
	e := byteorder.PutUVarint(wt, byteorder.BigEndian, uint64(len(object)))
	if e != nil {
		return e
	}

	for k, v := range object {
		if e := k.Write(wt, byteorder.BigEndian); e != nil {
			return e
		}

		if e := Write(wt, v); e != nil {
			return e
		}
	}

	return nil
}

func writeobj(wt io.Writer, object map[string]interface{}) error {
	e := byteorder.PutUVarint(wt, byteorder.BigEndian, uint64(len(object)))
	if e != nil {
		return e
	}

	for k, v := range object {
		r := String(k)

		if e := r.Write(wt, byteorder.BigEndian); e != nil {
			return e
		}

		if e := Write(wt, v); e != nil {
			return e
		}
	}

	return nil
}
