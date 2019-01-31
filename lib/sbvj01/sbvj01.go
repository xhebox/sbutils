package sbvj01

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/pkg/errors"
	"github.com/xhebox/bstruct/byteorder"
)

var (
	Magic = []byte{'S', 'B', 'V', 'J', '0', '1'}
)

const (
	NullT = iota + 1
	NumberT
	BoolT
	VarintT
	StringT
	ArrayT
	ObjectT
)

type VersionedJson struct {
	Id      string `json:"id"`
	endian  byteorder.ByteOrder
	Version int         `json:"version"`
	Content interface{} `json:"content"`
}

func ParseMagic(data []byte) (VersionedJson, error) {
	off := len(Magic)

	if bytes.Compare(data[:off], Magic) != 0 {
		return VersionedJson{}, errors.New("magic is not correct")
	}

	return Parse(data[off:])
}

func Parse(data []byte) (VersionedJson, error) {
	r := VersionedJson{}

	off := 0

	id, l, e := parseString(data[off:], byteorder.BigEndian)
	if e != nil {
		return r, e
	}
	off += l
	r.Id = id

	r.endian = byteorder.ByteOrder(data[off] == 0)

	r.Version = int(r.endian.Int32(data[off+1:]))

	content, _, e := ParseRaw(data[off+5:], r.endian)
	if e != nil {
		return r, e
	}

	r.Content = content
	return r, nil
}

func ParseRaw(data []byte, endian byteorder.ByteOrder) (interface{}, int, error) {
	switch data[0] {
	case NullT:
		return nil, 1, nil
	case NumberT:
		r, l, e := parseNumber(data[1:], endian)
		return r, l + 1, e
	case BoolT:
		r, l, e := parseBool(data[1:], endian)
		return r, l + 1, e
	case VarintT:
		r, l, e := parseVarint(data[1:], endian)
		return r, l + 1, e
	case StringT:
		r, l, e := parseString(data[1:], endian)
		return r, l + 1, e
	case ArrayT:
		r, l, e := parseArray(data[1:], endian)
		return r, l + 1, e
	case ObjectT:
		r, l, e := parseObject(data[1:], endian)
		return r, l + 1, e
	default:
		return nil, 0, errors.Errorf("unknown type %d", data[0])
	}
}

func parseNumber(data []byte, endian byteorder.ByteOrder) (float64, int, error) {
	return endian.Float64(data), 8, nil
}

func parseBool(data []byte, endian byteorder.ByteOrder) (bool, int, error) {
	return endian.Bool(data), 1, nil
}

func parseVarint(data []byte, endian byteorder.ByteOrder) (int64, int, error) {
	r, l, e := endian.VarintB(data)
	if e != nil {
		return -1, 0, e
	}

	return r, l, nil
}

func parseString(data []byte, endian byteorder.ByteOrder) (string, int, error) {
	keylen, l, e := endian.UVarintB(data)
	if e != nil {
		return "", 0, e
	}

	o := int(keylen)
	return string(data[l : l+o]), l + o, nil
}

func parseArray(data []byte, endian byteorder.ByteOrder) ([]interface{}, int, error) {
	cnt, l, e := endian.UVarintB(data)
	if e != nil {
		return nil, 0, e
	}

	r := []interface{}{}

	off := l

	for i, c := 0, int(cnt); i < c; i++ {
		value, l, e := ParseRaw(data[off:], endian)
		if e != nil {
			return nil, 0, e
		}
		off += l

		r = append(r, value)
	}

	return r, off, nil
}

func parseObject(data []byte, endian byteorder.ByteOrder) (map[string]interface{}, int, error) {
	cnt, l, e := endian.UVarintB(data)
	if e != nil {
		return nil, 0, e
	}

	r := map[string]interface{}{}

	off := l

	for i, c := 0, int(cnt); i < c; i++ {
		key, l, e := parseString(data[off:], endian)
		if e != nil {
			return nil, 0, e
		}
		off += l

		value, l, e := ParseRaw(data[off:], endian)
		if e != nil {
			return nil, 0, e
		}
		off += l

		r[key] = value
	}

	return r, off, nil
}

func WriteMagic(wt io.Writer, r VersionedJson) error {
	if _, e := wt.Write(Magic); e != nil {
		return e
	}

	if e := Write(wt, r); e != nil {
		return e
	}

	return nil
}

func Write(wt io.Writer, r VersionedJson) error {
	if e := writeString(wt, r.Id, r.endian); e != nil {
		return e
	}

	buf := make([]byte, 5)
	r.endian.PutBool(buf, !bool(r.endian))
	r.endian.PutInt32(buf[1:], int32(r.Version))

	if _, e := wt.Write(buf); e != nil {
		return e
	}

	return WriteRaw(wt, r.Content, r.endian)
}

func WriteRaw(wt io.Writer, anything interface{}, endian byteorder.ByteOrder) error {
	buf := make([]byte, 1)
	switch n := anything.(type) {
	case nil:
		buf[0] = NullT
		if _, e := wt.Write(buf); e != nil {
			return e
		}

		return nil
	case json.Number:
		m, e := n.Int64()
		if e == nil {
			buf[0] = VarintT
			if _, e := wt.Write(buf); e != nil {
				return e
			}

			return writeVarint(wt, m, endian)
		}

		buf[0] = NumberT
		if _, e := wt.Write(buf); e != nil {
			return e
		}

		p, e := n.Float64()
		if e != nil {
			return e
		}

		return writeNumber(wt, p, endian)
	case float64:
		buf[0] = VarintT
		if _, e := wt.Write(buf); e != nil {
			return e
		}

		return writeNumber(wt, n, endian)
	case int64:
		buf[0] = NumberT
		if _, e := wt.Write(buf); e != nil {
			return e
		}

		return writeVarint(wt, n, endian)
	case bool:
		buf[0] = BoolT
		if _, e := wt.Write(buf); e != nil {
			return e
		}

		return writeBool(wt, n, endian)
	case string:
		buf[0] = StringT
		if _, e := wt.Write(buf); e != nil {
			return e
		}

		return writeString(wt, n, endian)
	case []interface{}:
		buf[0] = ArrayT
		if _, e := wt.Write(buf); e != nil {
			return e
		}

		return writeArray(wt, n, endian)
	case map[string]interface{}:
		buf[0] = ObjectT
		if _, e := wt.Write(buf); e != nil {
			return e
		}

		return writeObject(wt, n, endian)
	default:
		return errors.Errorf("unknown type %+v", anything)
	}
}

func writeNumber(wt io.Writer, num float64, endian byteorder.ByteOrder) error {
	buf := make([]byte, 8)
	endian.PutFloat64(buf, num)
	if _, e := wt.Write(buf); e != nil {
		return e
	}
	return nil
}

func writeBool(wt io.Writer, boolean bool, endian byteorder.ByteOrder) error {
	buf := make([]byte, 1)
	if boolean {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	if _, e := wt.Write(buf); e != nil {
		return e
	}
	return nil
}

func writeVarint(wt io.Writer, num int64, endian byteorder.ByteOrder) error {
	buf := make([]byte, 10)
	l := endian.PutVarint(buf, num)

	if _, e := wt.Write(buf[:l]); e != nil {
		return e
	}
	return nil
}

func writeString(wt io.Writer, str string, endian byteorder.ByteOrder) error {
	length := len(str)

	buf := make([]byte, 10+length)

	l := endian.PutUVarint(buf, uint64(length))

	copy(buf[l:], str[:])

	if _, e := wt.Write(buf[:l+length]); e != nil {
		return e
	}
	return nil
}

func writeArray(wt io.Writer, array []interface{}, endian byteorder.ByteOrder) error {
	length := len(array)

	buf := make([]byte, 10)
	l := endian.PutUVarint(buf, uint64(length))
	if _, e := wt.Write(buf[:l]); e != nil {
		return e
	}

	for i := 0; i < length; i++ {
		if e := WriteRaw(wt, array[i], endian); e != nil {
			return e
		}
	}

	return nil
}

func writeObject(wt io.Writer, object map[string]interface{}, endian byteorder.ByteOrder) error {
	length := len(object)

	buf := make([]byte, 10)
	l := endian.PutUVarint(buf, uint64(length))
	if _, e := wt.Write(buf[:l]); e != nil {
		return e
	}

	for k, v := range object {
		if e := writeString(wt, k, endian); e != nil {
			return e
		}

		if e := WriteRaw(wt, v, endian); e != nil {
			return e
		}
	}

	return nil
}
