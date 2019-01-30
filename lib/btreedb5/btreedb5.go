package btreedb5

import (
	"bytes"
	"os"
	"sort"
	"strings"
	"sync"

	mmap "github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"github.com/xhebox/bstruct/byteorder"
)

const (
	DefaultBlocks = 255
	InterBlock    = 'I'
	LeafBlock     = 'L'
	FreeBlock     = 'F'
)

var (
	Magic = []byte{'B', 'T', 'r', 'e', 'e', 'D', 'B', '5'}
	big   = byteorder.BigEndian
)

type Key []byte

func (t Key) Less(h Key) bool {
	if len(t) != len(h) {
		panic("key length is not consistent")
	}

	for k := range t {
		if t[k] > h[k] {
			return false
		} else if t[k] < h[k] {
			return true
		}
	}

	return false
}

func (t Key) NonZero() bool {
	for k := range t {
		if t[k] != 0 {
			return true
		}
	}
	return false
}

type Record struct {
	Key  Key
	Data []byte
	upd  bool
}

type ii struct {
	height  int8
	ptr     int
	keys    []Key
	ptrs    []int
	records []Record
}

type BTree struct {
	Un1       bool
	Cnt       int
	Size      int64
	RootBlock int
}

type BTreeDB5 struct {
	BlockSize  int
	Identifier string
	KeySize    int
	Tree1      BTree
	Tree2      BTree

	tree     int
	order    int
	freelist []int
	freemu   sync.Mutex
	rwmu     sync.RWMutex
	file     *os.File
	fmap     mmap.MMap
}

func (h *BTreeDB5) Close() error {
	h.rwmu.Lock()
	defer h.rwmu.Unlock()

	h.marshalHeader()

	if e := h.fmap.Flush(); e != nil {
		return e
	}

	return h.file.Close()
}

func (h *BTreeDB5) Flush() error {
	h.rwmu.Lock()
	defer h.rwmu.Unlock()

	return h.fmap.Flush()
}

func (h *BTreeDB5) marshalHeader() {
	copy(h.fmap, Magic)

	big.PutInt32(h.fmap[8:], int32(h.BlockSize))

	for k := 0; k < 16; k++ {
		h.fmap[12+k] = 0
	}
	copy(h.fmap[12:], h.Identifier[:])

	big.PutInt32(h.fmap[28:], int32(h.KeySize))

	h.Tree1.Size = int64(512 + (h.Tree1.Cnt+1)*h.BlockSize)
	big.PutBool(h.fmap[32:], h.Tree1.Un1)
	big.PutInt32(h.fmap[33:], int32(h.Tree1.Cnt))
	big.PutInt64(h.fmap[37:], h.Tree1.Size)
	big.PutInt32(h.fmap[45:], int32(h.Tree1.RootBlock))

	h.Tree2.Size = int64(512 + (h.Tree2.Cnt+1)*h.BlockSize)
	big.PutBool(h.fmap[49:], h.Tree2.Un1)
	big.PutInt32(h.fmap[50:], int32(h.Tree2.Cnt))
	big.PutInt64(h.fmap[54:], h.Tree2.Size)
	big.PutInt32(h.fmap[62:], int32(h.Tree2.RootBlock))

	h.order = (h.BlockSize - 11) / (h.KeySize + 4)
}

func (h *BTreeDB5) unmarshalHeader() {
	h.BlockSize = int(big.Int32(h.fmap[8:]))

	h.Identifier = string(h.fmap[12:28])

	h.KeySize = int(big.Int32(h.fmap[28:]))

	h.Tree1.Un1 = big.Bool(h.fmap[32:])
	h.Tree1.Cnt = int(big.Int32(h.fmap[33:]))
	h.Tree1.Size = big.Int64(h.fmap[37:])
	h.Tree1.RootBlock = int(big.Int32(h.fmap[45:]))

	h.Tree2.Un1 = big.Bool(h.fmap[49:])
	h.Tree2.Cnt = int(big.Int32(h.fmap[50:]))
	h.Tree2.Size = big.Int64(h.fmap[54:])
	h.Tree2.RootBlock = int(big.Int32(h.fmap[62:]))

	h.order = (h.BlockSize - 11) / (h.KeySize + 4)
}

func (h *BTreeDB5) freelist_new(s, t int) {
	h.freemu.Lock()
	defer h.freemu.Unlock()

	h.freelist = make([]int, t-s)
	for k := s; k < t; k++ {
		h.freelist[k-s] = k

		typ, _, nptr := h.sector(k)
		typ[0] = FreeBlock
		typ[1] = FreeBlock
		big.PutInt32(nptr, -1)
	}
}

func (h *BTreeDB5) freelist_push(ptr int) {
	h.freemu.Lock()
	defer h.freemu.Unlock()

	for {
		if ptr == -1 {
			break
		}

		typ, _, nptr := h.sector(ptr)

		j := len(h.freelist)

		n := sort.Search(j, func(i int) bool {
			return h.freelist[i] > ptr
		})

		if n == 0 || h.freelist[n-1] != ptr {
			h.freelist = append(h.freelist[:n], append([]int{ptr}, h.freelist[n:]...)...)
		}

		ptr = int(big.Int32(nptr))
		big.PutInt32(nptr, -1)

		if typ[0] == FreeBlock {
			break
		}

		if typ[0] == InterBlock && ptr == 0 {
			typ[0] = FreeBlock
			typ[1] = FreeBlock
			break
		}

		typ[0] = FreeBlock
		typ[1] = FreeBlock
	}
}

func (h *BTreeDB5) freelist_pop() (ptr int, e error) {
	h.freemu.Lock()
	defer h.freemu.Unlock()

	if len(h.freelist) == 0 {
		e = h.fmap.Unmap()
		if e != nil {
			return
		}

		if h.tree == 1 {
			h.Tree1.Cnt *= 2
			err := h.file.Truncate(int64(512 + h.BlockSize*h.Tree1.Cnt))
			if err != nil {
				e = errors.Wrapf(err, "can not resize file")
				return
			}

			h.fmap, err = mmap.Map(h.file, mmap.RDWR, 0)
			if err != nil {
				e = errors.Wrapf(err, "failed to mmap")
				return
			}

			s := h.Tree1.Cnt / 2
			for k, t := s, h.Tree1.Cnt; k < t; k++ {
				h.freelist = append(h.freelist, k)

				typ, _, nptr := h.sector(k)
				typ[0] = FreeBlock
				typ[1] = FreeBlock
				big.PutInt32(nptr, -1)
			}

			e = errors.New("remap")
		} else {
			h.Tree2.Cnt *= 2
			err := h.file.Truncate(int64(512 + h.BlockSize*h.Tree2.Cnt))
			if err != nil {
				e = errors.Wrapf(err, "can not resize file")
				return
			}

			h.fmap, err = mmap.Map(h.file, mmap.RDWR, 0)
			if err != nil {
				e = errors.Wrapf(err, "failed to mmap")
				return
			}

			s := h.Tree1.Cnt / 2
			for k, t := s, h.Tree1.Cnt; k < t; k++ {
				h.freelist = append(h.freelist, k)

				typ, _, nptr := h.sector(k)
				typ[0] = FreeBlock
				typ[1] = FreeBlock
				big.PutInt32(nptr, -1)
			}

			e = errors.New("remap")
		}
	}

	ptr, h.freelist = h.freelist[0], h.freelist[1:]

	if h.tree == 1 {
		if ptr > h.Tree1.Cnt {
			h.Tree1.Cnt = ptr
		}
	} else {
		if ptr > h.Tree2.Cnt {
			h.Tree2.Cnt = ptr
		}
	}

	return
}

func (h *BTreeDB5) SetTree(p int) {
	h.rwmu.Lock()
	defer h.rwmu.Unlock()

	if p != 1 && p != 2 {
		return
	}

	h.tree = p
}

func (h *BTreeDB5) setRoot(p int) {
	if h.tree == 1 {
		h.Tree1.RootBlock = p
	} else {
		h.Tree2.RootBlock = p
	}
}

func (h *BTreeDB5) getRoot() int {
	if h.tree == 1 {
		return h.Tree1.RootBlock
	} else {
		return h.Tree2.RootBlock
	}
}

func New(file string, ident string, blocksz, keysz, blocks int) (*BTreeDB5, error) {
	var e error
	h := &BTreeDB5{}

	h.file, e = os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		return nil, errors.Wrapf(e, "failed to read")
	}

	if e = h.file.Truncate(int64(512 + blocksz*blocks)); e != nil {
		return nil, errors.Wrapf(e, "can not resize file")
	}

	h.fmap, e = mmap.Map(h.file, mmap.RDWR, 0)
	if e != nil {
		return nil, errors.Wrapf(e, "failed to mmap")
	}

	h.BlockSize = blocksz
	h.Identifier = ident
	h.KeySize = keysz

	h.Tree1.Cnt = blocks
	h.Tree1.Size = int64(512 + h.BlockSize*blocks)
	h.Tree1.RootBlock = 0

	h.Tree2.Cnt = blocks
	h.Tree2.Size = int64(512 + h.BlockSize*blocks)
	h.Tree2.RootBlock = 1

	h.marshalHeader()

	h.SetTree(2)
	if e := h.wll(1, []Record{}); e != nil {
		return nil, e
	}

	h.SetTree(1)
	if e := h.wll(0, []Record{}); e != nil {
		return nil, e
	}

	h.freelist_new(2, blocks)

	return h, nil
}

func Load(file string) (*BTreeDB5, error) {
	var e error
	h := &BTreeDB5{}

	h.file, e = os.OpenFile(file, os.O_RDWR, 0644)
	if e != nil {
		return nil, errors.Wrapf(e, "failed to read")
	}

	h.fmap, e = mmap.Map(h.file, mmap.RDWR, 0)
	if e != nil {
		return nil, errors.Wrapf(e, "failed to mmap")
	}

	if bytes.Compare(h.fmap[:len(Magic)], Magic) != 0 {
		return nil, errors.Errorf("magic is not correct")
	}

	h.unmarshalHeader()

	cnt := h.Tree1.Cnt
	h.SetTree(1)
	if h.Tree2.Cnt > cnt {
		cnt = h.Tree2.Cnt
		h.SetTree(2)
	}

	for k := 0; k < cnt; k++ {
		typ, _, _ := h.sector(k)

		if typ[0] != typ[1] {
			return nil, errors.Errorf("type inconsistent, maybe data corrupted on block[%d]", k)
		}

		if typ[0] == FreeBlock {
			h.freelist_push(k)
		}
	}

	return h, nil
}

func (h *BTreeDB5) sector(ptr int) ([]byte, []byte, []byte) {
	sector := h.fmap[512+ptr*h.BlockSize : 512+(ptr+1)*h.BlockSize]

	return sector[:2], sector[2 : h.BlockSize-4], sector[h.BlockSize-4:]
}

func (h *BTreeDB5) blk(ptr int) (r ii, e error) {
	r.ptr = ptr

	typ, blk, nptr := h.sector(ptr)

	switch typ[0] {
	case InterBlock:
		r.height = int8(blk[0])

		N := int(big.Int32(blk[1:]))

		r.keys = make([]Key, N-1)
		r.ptrs = make([]int, N)

		m := h.KeySize + 4
		for k := 0; k < N; k++ {
			j := 5 + k*m
			r.ptrs[k] = int(big.Int32(blk[j:]))
			if k < N-1 {
				r.keys[k] = append([]byte{}, blk[4+j:4+j+h.KeySize]...)
			}
		}
	case LeafBlock:
		r.height = -1

		blks := [][]byte{blk}

		for {
			ptr = int(big.Int32(nptr))

			if ptr == -1 {
				break
			}

			typ, blk, nptr = h.sector(ptr)

			if typ[0] != LeafBlock {
				e = errors.New("except a leaf block")
				return
			}

			blks = append(blks, blk)
		}

		buf := make([]byte, h.BlockSize*len(blks))
		off := 0
		for k, e := 0, len(blks); k < e; k++ {
			off += copy(buf[off:], blks[k])
		}

		N := int(big.Int32(buf))
		off = 4

		r.records = make([]Record, N)

		for n := 0; n < N; n++ {
			r.records[n].Key = buf[off : off+h.KeySize]
			off += h.KeySize

			size, l, err := big.UVarintB(buf[off:])
			if err != nil {
				e = errors.Wrapf(err, "unexpected varint read error")
				return
			}
			off += l

			sz := int(size)
			r.records[n].Data = buf[off : off+sz]
			off += sz
		}
	default:
		h.freelist_push(ptr)
	}

	return
}

func (h *BTreeDB5) wii(ptr int, inter ii) error {
	j := len(inter.ptrs)

	if j > h.order {
		return errors.New("need a smaller index list")
	}

	typ, data, nptr := h.sector(ptr)

	typ[0] = InterBlock
	typ[1] = InterBlock

	data[0] = byte(inter.height)
	big.PutInt32(data[1:], int32(j))

	off := 5

	for k := range inter.keys {
		big.PutInt32(data[off:], int32(inter.ptrs[k]))

		off += copy(data[off+4:], inter.keys[k]) + 4
	}

	big.PutInt32(data[off:], int32(inter.ptrs[len(inter.ptrs)-1]))

	big.PutInt32(nptr, 0)
	return nil
}

func (h *BTreeDB5) wll(ptr int, records []Record) error {
	if len(records) > h.order {
		return errors.New("too many records")
	}

	bufsz := 4
	for _, v := range records {
		bufsz += h.KeySize + 10 + len(v.Data)
	}

	buf := make([]byte, bufsz)

	big.PutInt32(buf, int32(len(records)))
	off := 4

	for _, v := range records {
		off += copy(buf[off:], v.Key)

		off += big.PutUVarint(buf[off:], uint64(len(v.Data)))

		off += copy(buf[off:], v.Data)
	}

	roff := 0
	for {
		typ, data, nptr := h.sector(ptr)

		typ[0] = LeafBlock
		typ[1] = LeafBlock

		roff += copy(data, buf[roff:])

		ptr = int(big.Int32(nptr))

		if roff >= off {
			big.PutInt32(nptr, -1)
			h.freelist_push(ptr)
			break
		}

		if ptr == -1 {
			var e error
			ptr, e = h.freelist_pop()
			if e != nil {
				if !strings.HasSuffix(e.Error(), "remap") {
					return e
				}

				_, _, nptr = h.sector(ptr)
			}

			big.PutInt32(nptr, int32(ptr))
		}
	}

	return nil
}

func (h *BTreeDB5) addii(inters []ii, ptrs []int, height int8, tokey Key, left_ptr, right_ptr int) error {
	ptrs_len := len(ptrs)
	origin_ptr := ptrs_len - 1

	// only happens when root is a leaf
	if ptrs_len == 0 || inters[origin_ptr].height == -1 {
		newroot, e := h.freelist_pop()
		if e != nil && !strings.HasSuffix(e.Error(), "remap") {
			return e
		}

		if e := h.wii(newroot, ii{
			height: height,
			keys: []Key{
				tokey,
			},
			ptrs: []int{
				left_ptr,
				right_ptr,
			},
		}); e != nil {
			return e
		}

		h.setRoot(newroot)
		return nil
	}

	orilen := len(inters[origin_ptr].keys)

	n := sort.Search(orilen, func(i int) bool {
		return tokey.Less(inters[origin_ptr].keys[i])
	})

	inters[origin_ptr].keys = append(inters[origin_ptr].keys[:n], append([]Key{tokey}, inters[origin_ptr].keys[n:]...)...)
	inters[origin_ptr].ptrs = append(inters[origin_ptr].ptrs[:n], append([]int{left_ptr}, inters[origin_ptr].ptrs[n:]...)...)
	inters[origin_ptr].ptrs[n+1] = right_ptr

	// no need to split keys
	if orilen+2 <= h.order {
		inters[origin_ptr].height = height
		return h.wii(inters[origin_ptr].ptr, inters[origin_ptr])
	}

	middle := (orilen + 1) >> 1

	low := inters[origin_ptr]
	low.keys = low.keys[:middle]
	low.ptrs = low.ptrs[:middle+1]
	low.height = height
	e := h.wii(low.ptr, low)
	if e != nil {
		return e
	}

	high := inters[origin_ptr]
	high.keys = high.keys[middle+1:]
	high.ptrs = high.ptrs[middle+1:]
	high.height = height
	high.ptr, e = h.freelist_pop()
	if e != nil && !strings.HasSuffix(e.Error(), "remap") {
		return e
	}

	if e := h.wii(high.ptr, high); e != nil {
		return e
	}

	if origin_ptr > 0 {
		return h.addii(inters[:origin_ptr], ptrs[:origin_ptr], height+1, high.keys[0], low.ptr, high.ptr)
	} else {
		return h.addii(nil, nil, height+1, high.keys[0], low.ptr, high.ptr)
	}
}

func (h *BTreeDB5) rmii(inters []ii, ptrs []int, mayberoot int, height int, left bool) error {
	var e error

	ptrs_len := len(ptrs)
	origin_ptr := ptrs_len - 1
	parent_ptr := ptrs_len - 2

	toptr := ptrs[ptrs_len-1]

	oriklen := len(inters[origin_ptr].keys)
	oriplen := len(inters[origin_ptr].ptrs)

	if left {
		if toptr == 0 {
			return errors.New("remove the left side of firstone?")
		}

		if toptr < oriklen {
			copy(inters[origin_ptr].keys[toptr-1:], inters[origin_ptr].keys[toptr:])
		}
		inters[origin_ptr].keys = inters[origin_ptr].keys[:oriklen-1]

		if toptr < oriplen {
			copy(inters[origin_ptr].ptrs[toptr-1:], inters[origin_ptr].ptrs[toptr:])
		}
		inters[origin_ptr].ptrs = inters[origin_ptr].ptrs[:oriplen-1]
	} else {
		if toptr == oriklen+1 {
			return errors.New("remove the right side of lastone?")
		}

		if toptr+1 < oriklen {
			copy(inters[origin_ptr].keys[toptr:], inters[origin_ptr].keys[toptr+1:])
		}
		inters[origin_ptr].keys = inters[origin_ptr].keys[:oriklen-1]

		if toptr+2 < oriplen {
			copy(inters[origin_ptr].ptrs[toptr+1:], inters[origin_ptr].ptrs[toptr+2:])
		}
		inters[origin_ptr].ptrs = inters[origin_ptr].ptrs[:oriplen-1]
	}

	// oriklen = oriplen - 1
	if oriklen > h.order/2 {
		if e := h.wii(inters[origin_ptr].ptr, inters[origin_ptr]); e != nil {
			return e
		}

		return nil
	}

	if ptrs_len == 1 {
		// this should be the root node
		if oriklen < 2 {
			// and it's too smal
			// so it is to be destroyed
			h.freelist_push(inters[origin_ptr].ptr)
			h.setRoot(inters[origin_ptr].ptrs[toptr])
			return nil
		} else {
			if e := h.wii(inters[origin_ptr].ptr, inters[origin_ptr]); e != nil {
				return e
			}

			return nil
		}
	}

	sib_side := 0
	sib := ii{}

	if ptrs[ptrs_len-2]+1 < len(inters[parent_ptr].ptrs) {
		sib_side = 2

		sib, e = h.blk(inters[parent_ptr].ptrs[ptrs[ptrs_len-2]+1])
		if e != nil {
			return e
		}

		sib_oriplen := len(sib.ptrs)
		if sib_oriplen > h.order/2 {
			inters[origin_ptr].keys = inters[origin_ptr].keys[:oriklen]
			inters[origin_ptr].keys[oriklen-1], sib.keys = sib.keys[0], sib.keys[1:]

			inters[origin_ptr].ptrs = inters[origin_ptr].ptrs[:oriplen]
			inters[origin_ptr].ptrs[oriplen-1], sib.ptrs = sib.ptrs[0], sib.ptrs[1:]

			if e := h.wii(sib.ptr, sib); e != nil {
				return e
			}

			if e := h.wii(inters[origin_ptr].ptr, inters[origin_ptr]); e != nil {
				return e
			}

			inters[parent_ptr].keys[ptrs[ptrs_len-2]] = sib.keys[0]

			return h.wii(inters[parent_ptr].ptr, inters[parent_ptr])
		}
	}

	if sib_side == 0 && ptrs[ptrs_len-2] > 0 {
		sib_side = 1

		sib, e = h.blk(inters[parent_ptr].ptrs[ptrs[ptrs_len-2]-1])
		if e != nil {
			return e
		}

		sib_oriklen := len(sib.keys)
		sib_oriplen := len(sib.ptrs)
		if sib_oriplen > h.order/2 {
			inters[origin_ptr].keys = inters[origin_ptr].keys[:oriklen]
			inters[origin_ptr].keys = append([]Key{Key{}}, inters[origin_ptr].keys...)
			inters[origin_ptr].keys[0], sib.keys = sib.keys[sib_oriklen-1], sib.keys[:sib_oriklen-1]

			inters[origin_ptr].ptrs = inters[origin_ptr].ptrs[:oriplen]
			inters[origin_ptr].ptrs = append([]int{0}, inters[origin_ptr].ptrs...)
			inters[origin_ptr].ptrs[0], sib.ptrs = sib.ptrs[sib_oriplen-1], sib.ptrs[:sib_oriplen-1]

			if e := h.wii(sib.ptr, sib); e != nil {
				return e
			}

			if e := h.wii(inters[origin_ptr].ptr, inters[origin_ptr]); e != nil {
				return e
			}

			inters[parent_ptr].keys[ptrs[ptrs_len-2]-1] = inters[origin_ptr].keys[0]

			return h.wii(inters[parent_ptr].ptr, inters[parent_ptr])
		}
	}

	if sib_side == 1 {
		sib.keys = append(sib.keys, inters[parent_ptr].keys[ptrs[ptrs_len-2]-1])
		sib.keys = append(sib.keys, inters[origin_ptr].keys...)
		sib.ptrs = append(sib.ptrs, inters[origin_ptr].ptrs...)

		if e := h.wii(sib.ptr, sib); e != nil {
			return e
		}

		h.freelist_push(inters[origin_ptr].ptr)

		return h.rmii(inters[:origin_ptr], ptrs[:origin_ptr], sib.ptr, height+1, true)
	} else if sib_side == 2 {
		inters[origin_ptr].keys = append(inters[origin_ptr].keys, inters[parent_ptr].keys[ptrs[ptrs_len-2]])
		inters[origin_ptr].keys = append(inters[origin_ptr].keys, sib.keys...)
		inters[origin_ptr].ptrs = append(inters[origin_ptr].ptrs, sib.ptrs...)

		if e := h.wii(inters[origin_ptr].ptr, inters[origin_ptr]); e != nil {
			return e
		}

		h.freelist_push(sib.ptr)

		return h.rmii(inters[:origin_ptr], ptrs[:origin_ptr], inters[origin_ptr].ptr, height+1, false)
	} else {
		return errors.New("not enough nodes to merge, tree corrupted")
	}
}

func (h *BTreeDB5) traverse(ptr int, f func(Record) error) error {
	inter, e := h.blk(ptr)
	if e != nil {
		return e
	}

	if inter.height != -1 {
		for _, v := range inter.ptrs {
			if e := h.traverse(v, f); e != nil {
				return e
			}
		}
	} else {
		records := inter.records
		for k := range records {
			if e := f(records[k]); e != nil {
				return e
			}
		}
	}

	return nil
}

func (h *BTreeDB5) Find(key Key, f func(Record) error) error {
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()

	root := h.getRoot()

	inter, e := h.blk(root)
	if e != nil {
		return e
	}

	for inter.height != -1 {
		inter, e = h.blk(inter.ptrs[sort.Search(len(inter.keys), func(i int) bool {
			return key.Less(inter.keys[i])
		})])

		if e != nil {
			return e
		}
	}

	records := inter.records

	n := sort.Search(len(records), func(i int) bool {
		return key.Less(records[i].Key)
	})

	if n > 0 && !records[n-1].Key.Less(key) {
		if e := f(records[n-1]); e != nil {
			return e
		}
	} else {
		return errors.New("not found")
	}

	return nil
}

func (h *BTreeDB5) First(f func(Record) error) error {
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()

	root := h.getRoot()

	inter, e := h.blk(root)
	if e != nil {
		return e
	}

	for inter.height != -1 {
		inter, e = h.blk(inter.ptrs[0])
		if e != nil {
			return e
		}
	}

	if e := f(inter.records[0]); e != nil {
		return e
	}

	return nil
}

func (h *BTreeDB5) Last(f func(Record) error) error {
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()

	root := h.getRoot()

	inter, e := h.blk(root)
	if e != nil {
		return e
	}

	for inter.height != -1 {
		inter, e = h.blk(inter.ptrs[len(inter.ptrs)-1])
		if e != nil {
			return e
		}
	}

	if e := f(inter.records[len(inter.records)-1]); e != nil {
		return e
	}

	return nil
}

func (h *BTreeDB5) Traverse(f func(Record) error) error {
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()

	return h.traverse(h.getRoot(), f)
}

func (h *BTreeDB5) Insert(key Key, data []byte) error {
	h.rwmu.Lock()
	defer h.rwmu.Unlock()

	root := h.getRoot()

	inter, e := h.blk(root)
	if e != nil {
		return e
	}

	inters := []ii{}
	ptrs := []int{}
	for inter.height != -1 {
		inters = append(inters, inter)

		j := sort.Search(len(inter.keys), func(i int) bool {
			return key.Less(inter.keys[i])
		})

		ptrs = append(ptrs, j)

		inter, e = h.blk(inter.ptrs[j])
		if e != nil {
			return e
		}
	}

	records := inter.records
	orilen := len(records)

	n := sort.Search(orilen, func(i int) bool {
		return key.Less(records[i].Key)
	})

	if n > 0 && !records[n-1].Key.Less(key) {
		records[n-1].Data = data
		if e := h.wll(inter.ptr, records); e != nil {
			return e
		}

		return nil
	}

	records = append(records[:n], append([]Record{Record{Key: key, Data: data}}, records[n:]...)...)

	if orilen+1 <= h.order {
		if e := h.wll(inter.ptr, records); e != nil {
			return e
		}

		return nil
	}

	h.freelist_push(inter.ptr)

	middle := (orilen + 1) >> 1

	left_ptr, e := h.freelist_pop()
	if e != nil && !strings.HasSuffix(e.Error(), "remap") {
		return e
	}

	if e := h.wll(left_ptr, records[:middle]); e != nil {
		return e
	}

	right_ptr, e := h.freelist_pop()
	if e != nil && !strings.HasSuffix(e.Error(), "remap") {
		return e
	}

	if e := h.wll(right_ptr, records[middle:]); e != nil {
		return e
	}

	if e := h.addii(inters, ptrs, 0, records[middle].Key, left_ptr, right_ptr); e != nil {
		return e
	}

	return nil
}

func (h *BTreeDB5) Delete(key Key) error {
	h.rwmu.Lock()
	defer h.rwmu.Unlock()

	root := h.getRoot()

	inter, e := h.blk(root)
	if e != nil {
		return e
	}

	inters := []ii{}
	ptrs := []int{}
	for inter.height != -1 {
		inters = append(inters, inter)

		j := sort.Search(len(inter.keys), func(i int) bool {
			return key.Less(inter.keys[i])
		})

		ptrs = append(ptrs, j)

		inter, e = h.blk(inter.ptrs[j])
		if e != nil {
			return e
		}
	}
	ptrs_len := len(ptrs)
	parent_ptr := ptrs_len - 1

	records := inter.records
	orilen := len(records)

	n := sort.Search(orilen, func(i int) bool {
		return key.Less(records[i].Key)
	})

	if n > 0 && !records[n-1].Key.Less(key) {
		copy(records[n-1:], records[n:])
		records = records[:orilen-1]
	} else {
		return errors.New("not found")
	}

	// orilen-1 >= h.order/2
	if orilen > h.order/2 {
		if e := h.wll(inter.ptr, records); e != nil {
			return e
		}

		return nil
	}

	// only happens when root is a leaf
	if ptrs_len == 0 {
		if orilen == 1 {
			return errors.New("can not remove the lastkey")
		}

		if e := h.wll(inter.ptr, records); e != nil {
			return e
		}

		return nil
	}

	sib_side := 0
	sib := ii{}
	sib_records := ([]Record)(nil)

	if ptrs[parent_ptr]+1 < len(inters[parent_ptr].ptrs) {
		sib_side = 2

		sib, e = h.blk(inters[parent_ptr].ptrs[ptrs[parent_ptr]+1])
		if e != nil {
			return e
		}

		sib_records = sib.records
		sib_orilen := len(sib_records)

		if sib_orilen > h.order/2 {
			records = records[:orilen]
			records[orilen-1], sib_records = sib_records[0], sib_records[1:]

			if e := h.wll(inter.ptr, records); e != nil {
				return e
			}

			if e := h.wll(sib.ptr, sib_records); e != nil {
				return e
			}

			inters[parent_ptr].keys[ptrs[parent_ptr]] = sib_records[0].Key

			return h.wii(inters[parent_ptr].ptr, inters[parent_ptr])
		}
	}

	if sib_side == 0 && ptrs[parent_ptr] > 0 {
		sib_side = 1

		sib, e = h.blk(inters[parent_ptr].ptrs[ptrs[parent_ptr]-1])
		if e != nil {
			return e
		}

		sib_records = sib.records
		sib_orilen := len(sib_records)

		if sib_orilen > h.order/2 {
			records = records[:orilen]
			records = append([]Record{Record{}}, records...)
			records[0], sib_records = sib_records[sib_orilen-1], sib_records[:sib_orilen-1]

			if e := h.wll(sib.ptr, sib_records); e != nil {
				return e
			}

			if e := h.wll(inter.ptr, records); e != nil {
				return e
			}

			inters[parent_ptr].keys[ptrs[parent_ptr]-1] = records[0].Key

			return h.wii(inters[parent_ptr].ptr, inters[parent_ptr])
		}
	}

	if sib_side == 1 {
		sib_records = append(sib_records, records...)

		if e := h.wll(sib.ptr, sib_records); e != nil {
			return e
		}

		h.freelist_push(inter.ptr)

		return h.rmii(inters, ptrs, inter.ptr, 0, true)
	} else if sib_side == 2 {
		records = append(records, sib_records...)

		if e := h.wll(inter.ptr, records); e != nil {
			return e
		}

		h.freelist_push(sib.ptr)

		return h.rmii(inters, ptrs, inter.ptr, 0, false)
	} else {
		return errors.New("not enough nodes to merge, tree corrupted")
	}
}
