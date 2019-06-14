package btreedb5

import (
	"bytes"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/xhebox/bstruct/byteorder"
	"github.com/xhebox/sbutils/lib/blockfile"
	"github.com/xhebox/sbutils/lib/data_types"
)

type direction int

const (
	IndexNode = 'I'
	LeafNode  = 'L'
	FreeNode  = 'F'

	descend = direction(-1)
	ascend  = direction(+1)
	maxptr  = uint(^uint32(0))
)

var (
	Magic = []byte{'B', 'T', 'r', 'e', 'e', 'D', 'B', '5'}
	zero  = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
)

type BTree struct {
	FreeIndex  uint  // HeadFreeIndex
	Size       int64 // DeviceSize
	RootBlock  uint
	RootIsLeaf bool
}

type BTreeDB5 struct {
	Identifier string
	BlockSize  int
	KeySize    int
	UseAltRoot bool
	Tree       BTree

	used_uncommitted map[uint]bool
	free_committed   map[uint]bool
	free_uncommitted map[uint]bool
	intermax         int
	freemax          int
	leafmax          int
	freemu           sync.Mutex
	file             *blockfile.BlockFile
}

func intermax(blksz, keysz int) int {
	return (blksz-2-1-4-4)/(keysz+4) + 1
}

func freemax(blksz int) int {
	return (blksz - 2 - 4 - 4) / 4
}

func New(file string, ident string, blksz, keysz int) (h *BTreeDB5, e error) {
	h = &BTreeDB5{
		Identifier: ident,
		UseAltRoot: false,
		Tree: BTree{
			FreeIndex:  maxptr,
			RootBlock:  maxptr,
			RootIsLeaf: true,
		},
		BlockSize:        blksz,
		KeySize:          keysz,
		used_uncommitted: make(map[uint]bool),
		free_committed:   make(map[uint]bool),
		free_uncommitted: make(map[uint]bool),
		freemax:          freemax(blksz),
		intermax:         intermax(blksz, keysz),
		leafmax:          2,
	}

	os.Remove(file)

	h.file, e = blockfile.NewBlockFile(file, 512)
	if e != nil {
		return nil, errors.Wrapf(e, "failed to open a block file")
	}

	h.file.SetBlksz(blksz)

	if e := h.file.Resize(0); e != nil {
		return nil, errors.Wrapf(e, "failed to resize the block file")
	}

	h.marshalHeader()

	h.Tree.RootBlock = h.writeLeafNode(&leafNode{self: maxptr})

	return h, nil
}

func Load(file string) (h *BTreeDB5, e error) {
	h = &BTreeDB5{}

	h.file, e = blockfile.NewBlockFile(file, 512)
	if e != nil {
		return nil, errors.Wrapf(e, "failed to open a block file")
	}

	h.unmarshalHeader()

	h.file.SetBlksz(h.BlockSize)

	h.readRoot()

	return h, nil
}

func (h *BTreeDB5) Close() error {
	e := h.Commit()
	if e != nil {
		return e
	}
	return h.file.Close()
}

func (h *BTreeDB5) marshalHeader() {
	hdr := h.file.Header()

	copy(hdr, Magic)

	byteorder.BigEndian.PutInt32(hdr[8:], int32(h.BlockSize))

	copy(hdr[12+copy(hdr[12:28], h.Identifier[:]):28], zero)

	byteorder.BigEndian.PutUint32(hdr[28:], uint32(uint(h.KeySize)))
}

func (h *BTreeDB5) writeRoot() {
	hdr := h.file.Header()

	hdr[32] = byteorder.Bool2Byte(h.UseAltRoot)

	if !h.UseAltRoot {
		byteorder.BigEndian.PutUint32(hdr[33:], uint32(h.Tree.FreeIndex))
		byteorder.BigEndian.PutInt64(hdr[37:], h.file.Size())
		byteorder.BigEndian.PutUint32(hdr[45:], uint32(h.Tree.RootBlock))
		hdr[49] = byteorder.Bool2Byte(h.Tree.RootIsLeaf)
	} else {
		byteorder.BigEndian.PutUint32(hdr[50:], uint32(h.Tree.FreeIndex))
		byteorder.BigEndian.PutInt64(hdr[54:], h.file.Size())
		byteorder.BigEndian.PutUint32(hdr[62:], uint32(h.Tree.RootBlock))
		hdr[66] = byteorder.Bool2Byte(h.Tree.RootIsLeaf)
	}
}

func (h *BTreeDB5) unmarshalHeader() {
	hdr := h.file.Header()

	h.BlockSize = int(byteorder.BigEndian.Int32(hdr[8:]))

	h.Identifier = string(hdr[12:28])

	h.KeySize = int(byteorder.BigEndian.Int32(hdr[28:]))
}

func (h *BTreeDB5) readRoot() {
	hdr := h.file.Header()

	h.UseAltRoot = byteorder.Byte2Bool(hdr[32])

	if !h.UseAltRoot {
		h.Tree.FreeIndex = uint(byteorder.BigEndian.Uint32(hdr[33:]))
		//h.Tree.Size = byteorder.BigEndian.Int64(hdr[37:])
		h.Tree.RootBlock = uint(byteorder.BigEndian.Uint32(hdr[45:]))
		h.Tree.RootIsLeaf = byteorder.Byte2Bool(hdr[49])
	} else {
		h.Tree.FreeIndex = uint(byteorder.BigEndian.Uint32(hdr[50:]))
		//h.Tree.Size = byteorder.BigEndian.Int64(hdr[54:])
		h.Tree.RootBlock = uint(byteorder.BigEndian.Uint32(hdr[62:]))
		h.Tree.RootIsLeaf = byteorder.Byte2Bool(hdr[66])
	}

	h.intermax = intermax(h.BlockSize, h.KeySize)
}

func (h *BTreeDB5) freeNode(ptr uint) *freeNode {
	r := &freeNode{}

	block := h.file.Block(ptr)

	if block[0] != FreeNode || block[1] != FreeNode {
		panic("except correct signature")
	}

	r.next = uint(byteorder.BigEndian.Uint32(block[2:]))

	N := byteorder.BigEndian.Uint32(block[6:])

	r.ptrs = make([]uint, N)

	off := 10
	for k := range r.ptrs {
		r.ptrs[k] = uint(byteorder.BigEndian.Uint32(block[off:]))
		off += 4
	}

	return r
}

func (h *BTreeDB5) indexNode(ptr uint) *indexNode {
	r := &indexNode{}
	r.self = ptr

	block := h.file.Block(ptr)

	if block[0] != IndexNode || block[1] != IndexNode {
		panic("except correct signature")
	}

	r.height = block[2]

	N := byteorder.BigEndian.Uint32(block[3:])

	r.keys = make([]Key, N)
	r.ptrs = make([]uint, N+1)

	r.ptrs[0] = uint(byteorder.BigEndian.Uint32(block[7:]))

	off := 11
	for k := range r.keys {
		r.keys[k] = make(Key, h.KeySize)
		off += copy(r.keys[k], block[off:])

		r.ptrs[k+1] = uint(byteorder.BigEndian.Uint32(block[off:]))
		off += 4
	}

	return r
}

func (h *BTreeDB5) leafNode(ptr uint) *leafNode {
	r := &leafNode{}
	r.self = ptr

	readers := []io.Reader{}

	for ptr != maxptr {
		block := h.file.Block(ptr)

		if block[0] != LeafNode || block[1] != LeafNode {
			panic("except correct signature")
		}

		readers = append(readers, bytes.NewReader(block[2:h.BlockSize-4]))

		ptr = uint(byteorder.BigEndian.Uint32(block[h.BlockSize-4:]))
	}

	rd := io.MultiReader(readers...)

	N, e := byteorder.Uint32(rd, byteorder.BigEndian)
	if e != nil {
		panic(e)
	}

	r.keys = make([]Key, N)
	r.data = make([]ByteArray, N)

	for k := range r.keys {
		r.keys[k] = make(Key, h.KeySize)

		if _, e := io.ReadFull(rd, r.keys[k]); e != nil {
			panic(e)
		}

		e = r.data[k].Read(rd, byteorder.BigEndian)
		if e != nil {
			panic(e)
		}
	}

	return r
}

func (h *BTreeDB5) freelist_push(ptr uint) {
	h.freemu.Lock()

	if ptr != maxptr {
		if h.used_uncommitted[ptr] {
			h.used_uncommitted[ptr] = false
			h.free_committed[ptr] = true
		} else {
			h.free_uncommitted[ptr] = true
		}
	}

	h.freemu.Unlock()
}

func (h *BTreeDB5) freelist_gpop() (uint, bool) {
	r := h.file.Cap()

	e := h.file.Grow(1)
	if e != nil {
		panic(e)
	}

	h.used_uncommitted[r] = true
	return r, true
}

func (h *BTreeDB5) freelist_pop() (uint, bool) {
	h.freemu.Lock()

	var r uint

	if len(h.free_uncommitted) == 0 {
		if h.Tree.FreeIndex == maxptr {
			h.freemu.Unlock()
			return h.freelist_gpop()
		}

		res := h.freeNode(h.Tree.FreeIndex)

		if len(res.ptrs) == 0 {
			h.freemu.Unlock()
			return h.freelist_gpop()
		}

		ptrs := res.ptrs
		for k := range ptrs {
			h.free_uncommitted[ptrs[k]] = true
		}
		h.free_committed[h.Tree.FreeIndex] = true
		h.Tree.FreeIndex = res.next
	}

	h.used_uncommitted[r] = true
	m := h.free_uncommitted
	for k := range m {
		delete(m, k)
		h.freemu.Unlock()
		return k, false
	}

	h.freemu.Unlock()
	panic("should not go here")
}

func (h *BTreeDB5) freelist_clear() {
	h.freemu.Lock()
	m := h.free_uncommitted
	for k := range m {
		delete(m, k)
	}
	m = h.free_committed
	for k := range m {
		delete(m, k)
	}
	m = h.used_uncommitted
	for k := range m {
		delete(m, k)
	}
	h.freemu.Unlock()
}

func (h *BTreeDB5) Rollback() (e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	h.readRoot()
	h.freelist_clear()
	e = h.file.Resize(uint(int((h.file.Size() - 512) / int64(h.BlockSize))))
	return
}

func (h *BTreeDB5) commit(total map[uint]bool) {
	var ptr uint

	k := make([]uint, len(total))

	var i int
	for key := range total {
		k[i] = key
		i++
	}

	for len(k) != 0 {
		var node *freeNode = nil

		h.freemu.Lock()

		if h.Tree.FreeIndex != maxptr {
			node = h.freeNode(h.Tree.FreeIndex)
			if len(node.ptrs) < h.freemax {
				ptr = h.Tree.FreeIndex
			} else {
				node = nil
			}
		}

		if node == nil {
			ptr, k = k[0], k[1:]
			node = &freeNode{next: h.Tree.FreeIndex}
			h.Tree.FreeIndex = ptr
		}

		length := h.freemax - len(node.ptrs)
		if length > len(k) {
			length = len(k)
		}
		node.ptrs = append(node.ptrs, k[:length]...)
		k = k[length:]

		h.freemu.Unlock()

		h.writeFreeNode(node, ptr)
	}
}

func (h *BTreeDB5) Commit() (e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	h.writeRoot()
	h.UseAltRoot = !h.UseAltRoot
	h.commit(h.free_uncommitted)
	h.commit(h.free_committed)
	h.freelist_clear()
	return
}

func (h *BTreeDB5) writeFreeNode(node *freeNode, ptr uint) {
	block := h.file.Block(ptr)

	block[0] = FreeNode
	block[1] = FreeNode
	byteorder.BigEndian.PutUint32(block[2:], uint32(node.next))
	byteorder.BigEndian.PutUint32(block[6:], uint32(len(node.ptrs)))

	off := 10
	for k := range node.ptrs {
		byteorder.BigEndian.PutUint32(block[off:], uint32(node.ptrs[k]))
		off += 4
	}
}

func (h *BTreeDB5) writeIndexNode(node *indexNode) uint {
	h.freelist_push(node.self)

	node.self, _ = h.freelist_pop()

	block := h.file.Block(node.self)

	block[0] = IndexNode
	block[1] = IndexNode
	block[2] = node.height
	byteorder.BigEndian.PutUint32(block[3:], uint32(uint(len(node.ptrs)-1)))
	byteorder.BigEndian.PutUint32(block[7:], uint32(node.ptrs[0]))

	off := 11
	for k := range node.keys {
		off += copy(block[off:], node.keys[k])
		byteorder.BigEndian.PutUint32(block[off:], uint32(node.ptrs[k+1]))
		off += 4
	}

	return node.self
}

func (h *BTreeDB5) writeLeafNode(node *leafNode) uint {
	ptr := node.self

	for ptr != maxptr {
		block := h.file.Block(ptr)

		if block[0] != LeafNode || block[1] != LeafNode {
			panic("except correct signature")
		}

		h.freelist_push(ptr)

		ptr = uint(byteorder.BigEndian.Uint32(block[h.BlockSize-4:]))
	}

	buf := &bytes.Buffer{}

	e := byteorder.PutUint32(buf, byteorder.BigEndian, uint32(uint(len(node.data))))
	if e != nil {
		panic(e)
	}

	for k := range node.data {
		_, e = buf.Write(node.keys[k])
		if e != nil {
			panic(e)
		}

		e = node.data[k].Write(buf, byteorder.BigEndian)
		if e != nil {
			panic(e)
		}
	}

	src := buf.Bytes()
	end := len(src)
	off := 0
	nptr := uint(0)
	var block []byte

	for off < end {
		ptr, change := h.freelist_pop()

		if off == 0 {
			node.self = ptr
		}

		if len(block) != 0 {
			if change {
				block = h.file.Block(nptr)
			}

			byteorder.BigEndian.PutUint32(block[h.BlockSize-4:], uint32(ptr))
		}

		block = h.file.Block(ptr)

		block[0] = LeafNode
		block[1] = LeafNode
		byteorder.BigEndian.PutUint32(block[h.BlockSize-4:], uint32(maxptr))

		off += copy(block[2:h.BlockSize-4], src[off:])

		nptr = ptr
	}

	return node.self
}

type Key []byte

type ByteArray = data_types.ByteArray

type indexNode struct {
	self   uint
	height uint8
	keys   []Key
	ptrs   []uint
}

func (node *indexNode) find(key Key) (int, bool) {
	keys := node.keys

	index := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], key) >= 0 })

	if index < len(keys) && bytes.Equal(keys[index], key) {
		return index, true
	}

	return index, false
}

func (node *indexNode) insertAtKey(index int, key Key) {
	node.keys = append(node.keys, nil)
	copy(node.keys[index+1:], node.keys[index:])
	node.keys[index] = key
}

func (node *indexNode) insertAtPtr(index int, ptr uint) {
	node.ptrs = append(node.ptrs, 0)
	copy(node.ptrs[index+1:], node.ptrs[index:])
	node.ptrs[index] = ptr
}

func (node *indexNode) replaceAtKey(index int, key Key) {
	node.keys[index] = key
}

func (node *indexNode) replaceAtPtr(index int, ptr uint) {
	node.ptrs[index] = ptr
}

func (node *indexNode) removeAtKey(index int) Key {
	r1 := node.keys[index]
	copy(node.keys[index:], node.keys[index+1:])
	node.keys = node.keys[:len(node.keys)-1]
	return r1
}

func (node *indexNode) removeAtPtr(index int) uint {
	r2 := node.ptrs[index]
	copy(node.ptrs[index:], node.ptrs[index+1:])
	node.ptrs = node.ptrs[:len(node.ptrs)-1]
	return r2
}

func (node *indexNode) split() (*indexNode, Key) {
	i := (len(node.keys) + 1) / 2

	r := &indexNode{self: maxptr, height: node.height}

	r.keys = append(r.keys, node.keys[i+1:]...)
	rkey := node.keys[i]
	for t, k := i, len(node.keys); t < k; t++ {
		node.keys[t] = nil
	}
	node.keys = node.keys[:i]

	r.ptrs = append(r.ptrs, node.ptrs[i+1:]...)
	node.ptrs = node.ptrs[:i+1]

	return r, rkey
}

type leafNode struct {
	self uint
	keys []Key
	data []ByteArray
}

func (node *leafNode) find(key Key) (int, bool) {
	keys := node.keys

	index := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], key) >= 0 })

	if index < len(keys) && bytes.Equal(keys[index], key) {
		return index, true
	}

	return index, false
}

func (node *leafNode) replaceAt(index int, data ByteArray) {
	node.data[index] = data
}

func (node *leafNode) insertAt(index int, key Key, data ByteArray) {
	node.keys = append(node.keys, nil)
	copy(node.keys[index+1:], node.keys[index:])
	node.keys[index] = key

	node.data = append(node.data, nil)
	copy(node.data[index+1:], node.data[index:])
	node.data[index] = data
}

func (node *leafNode) size() int {
	size := 0
	for k := range node.keys {
		size += len(node.keys[k]) + byteorder.VMAXLEN + len(node.data[k])
	}
	return size
}

func (node *leafNode) removeAt(index int) (Key, ByteArray) {
	r1 := node.keys[index]
	copy(node.keys[index:], node.keys[index+1:])
	node.keys = node.keys[:len(node.keys)-1]

	r2 := node.data[index]
	copy(node.data[index:], node.data[index+1:])
	node.data = node.data[:len(node.data)-1]

	return r1, r2
}

func (node *leafNode) split() *leafNode {
	i := (len(node.keys) + 1) / 2

	r := &leafNode{self: maxptr}

	r.keys = append(r.keys, node.keys[i:]...)
	for t, k := i, len(node.keys); t < k; t++ {
		node.keys[t] = nil
	}
	node.keys = node.keys[:i]

	r.data = append(r.data, node.data[i:]...)
	for t, k := i, len(node.data); t < k; t++ {
		node.data[t] = nil
	}
	node.data = node.data[:i]

	return r
}

type freeNode struct {
	next uint
	ptrs []uint
}

func (h *BTreeDB5) getLeaf(ptr uint, key Key) ByteArray {
	node := h.leafNode(ptr)
	index, ok := node.find(key)
	if ok {
		return node.data[index]
	} else {
		return nil
	}
}

func (h *BTreeDB5) getIndex(ptr uint, key Key) ByteArray {
	node := h.indexNode(ptr)

	index, ok := node.find(key)
	if ok {
		index = index + 1
	}

	if node.height == 0 {
		return h.getLeaf(node.ptrs[index], key)
	} else {
		return h.getIndex(node.ptrs[index], key)
	}
}

func (h *BTreeDB5) Get(key Key) (r ByteArray, e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	if h.Tree.RootIsLeaf {
		r = h.getLeaf(h.Tree.RootBlock, key)
	} else {
		r = h.getIndex(h.Tree.RootBlock, key)
	}

	if r == nil {
		e = errors.New("not found")
	}

	return
}

func (h *BTreeDB5) Has(key Key) (r bool, e error) {
	var s ByteArray
	s, e = h.Get(key)
	return s != nil, e
}

func (h *BTreeDB5) hetaLeaf(ptr uint, head bool) (Key, ByteArray) {
	node := h.leafNode(ptr)
	var index int
	if head {
		index = 0
	} else {
		index = len(node.keys) - 1
	}
	return node.keys[index], node.data[index]
}

func (h *BTreeDB5) hetaIndex(ptr uint, head bool) (Key, ByteArray) {
	node := h.indexNode(ptr)
	var index int
	if head {
		index = 0
	} else {
		index = len(node.ptrs) - 1
	}
	if node.height == 0 {
		return h.hetaLeaf(node.ptrs[index], head)
	} else {
		return h.hetaIndex(node.ptrs[index], head)
	}
}

func (h *BTreeDB5) first() (Key, ByteArray) {
	if h.Tree.RootIsLeaf {
		return h.hetaLeaf(h.Tree.RootBlock, true)
	} else {
		return h.hetaIndex(h.Tree.RootBlock, true)
	}
}

func (h *BTreeDB5) First() (k Key, r ByteArray, e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	k, r = h.first()
	if r == nil {
		e = errors.New("not found")
	}

	return
}

func (h *BTreeDB5) last() (Key, ByteArray) {
	if h.Tree.RootIsLeaf {
		return h.hetaLeaf(h.Tree.RootBlock, false)
	} else {
		return h.hetaIndex(h.Tree.RootBlock, false)
	}
}

func (h *BTreeDB5) Last() (k Key, r ByteArray, e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	k, r = h.last()
	if r == nil {
		e = errors.New("not found")
	}

	return
}

type Iterator func(Key, []byte)

func (h *BTreeDB5) iterateLeaf(ptr uint, start, stop Key, dir direction, iter Iterator) bool {
	var i int
	var ok bool

	node := h.leafNode(ptr)

	switch dir {
	case ascend:
		if start != nil {
			i, _ = node.find(start)
		}
		//else i = 0

		for j := len(node.keys); i < j; i++ {
			if stop != nil && bytes.Compare(node.keys[i], stop) >= 0 {
				return true
			}

			iter(node.keys[i], node.data[i])
		}
	case descend:
		if start != nil {
			i, ok = node.find(start)
			if !ok {
				i = i - 1
			}
		} else {
			i = len(node.keys) - 1
		}

		for ; i > -1; i-- {
			if stop != nil && bytes.Compare(node.keys[i], stop) < 0 {
				return true
			}

			iter(node.keys[i], node.data[i])
		}
	}

	return false
}

func (h *BTreeDB5) iterateIndex(ptr uint, start, stop Key, dir direction, iter Iterator) bool {
	var i int
	var ok bool

	node := h.indexNode(ptr)

	switch dir {
	case ascend:
		if start != nil {
			i, ok = node.find(start)
			if ok {
				i = i + 1
			}
		}
		//else i = 0

		for j := len(node.ptrs); i < j; i++ {
			if node.height == 0 {
				if h.iterateLeaf(node.ptrs[i], start, stop, dir, iter) {
					return true
				}
			} else {
				if h.iterateIndex(node.ptrs[i], start, stop, dir, iter) {
					return true
				}
			}
		}
	case descend:
		if start != nil {
			i, ok = node.find(start)
			if !ok {
				i = i - 1
			}
		} else {
			i = len(node.ptrs) - 1
		}

		for ; i > -1; i-- {
			if node.height == 0 {
				if h.iterateLeaf(node.ptrs[i], start, stop, dir, iter) {
					return true
				}
			} else {
				if h.iterateIndex(node.ptrs[i], start, stop, dir, iter) {
					return true
				}
			}
		}
	}

	return false
}

func (h *BTreeDB5) Ascend(iter Iterator) (e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	if h.Tree.RootIsLeaf {
		h.iterateLeaf(h.Tree.RootBlock, nil, nil, ascend, iter)
	} else {
		h.iterateIndex(h.Tree.RootBlock, nil, nil, ascend, iter)
	}
	return
}

func (h *BTreeDB5) AscendRange(start, stop Key, iter Iterator) (e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	if h.Tree.RootIsLeaf {
		h.iterateLeaf(h.Tree.RootBlock, start, stop, ascend, iter)
	} else {
		h.iterateIndex(h.Tree.RootBlock, start, stop, ascend, iter)
	}
	return
}

func (h *BTreeDB5) Descend(iter Iterator) (e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	if h.Tree.RootIsLeaf {
		h.iterateLeaf(h.Tree.RootBlock, nil, nil, descend, iter)
	} else {
		h.iterateIndex(h.Tree.RootBlock, nil, nil, descend, iter)
	}
	return
}

func (h *BTreeDB5) DescendRange(start, stop Key, iter Iterator) (e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	if h.Tree.RootIsLeaf {
		h.iterateLeaf(h.Tree.RootBlock, start, stop, descend, iter)
	} else {
		h.iterateIndex(h.Tree.RootBlock, start, stop, descend, iter)
	}
	return
}

func (h *BTreeDB5) insertLeaf(ptr uint, key Key, data ByteArray) (uint, uint, Key) {
	node := h.leafNode(ptr)

	index, ok := node.find(key)

	if ok {
		node.replaceAt(index, data)
	} else {
		node.insertAt(index, key, data)
	}

	size := 0
	for k := range node.keys {
		size += h.KeySize + byteorder.VMAXLEN + len(node.data[k])
	}

	if 2*(h.BlockSize-6) > size || len(node.keys) == 1 {
		return h.writeLeafNode(node), maxptr, nil
	}

	newnode := node.split()
	return h.writeLeafNode(node), h.writeLeafNode(newnode), newnode.keys[0]
}

func (h *BTreeDB5) insertIndex(ptr uint, key Key, data ByteArray) (uint, uint, Key, uint8) {
	node := h.indexNode(ptr)

	index, ok := node.find(key)
	if ok {
		index = index + 1
	}

	var l, r uint
	var rkey Key

	if node.height == 0 {
		l, r, rkey = h.insertLeaf(node.ptrs[index], key, data)
	} else {
		l, r, rkey, _ = h.insertIndex(node.ptrs[index], key, data)
	}

	node.replaceAtPtr(index, l)

	if rkey == nil {
		return h.writeIndexNode(node), maxptr, nil, node.height
	}

	node.insertAtKey(index, rkey)
	node.insertAtPtr(index+1, r)

	if len(node.ptrs) <= h.intermax {
		return h.writeIndexNode(node), maxptr, nil, node.height
	}

	newnode, rkey := node.split()
	return h.writeIndexNode(node), h.writeIndexNode(newnode), rkey, node.height
}

func (h *BTreeDB5) Insert(key Key, data ByteArray) (e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	var l, r uint
	var o uint8 = 255
	var rkey Key

	if h.Tree.RootIsLeaf {
		l, r, rkey = h.insertLeaf(h.Tree.RootBlock, key, data)
	} else {
		l, r, rkey, o = h.insertIndex(h.Tree.RootBlock, key, data)
	}

	h.Tree.RootBlock = l

	if rkey != nil {
		node := &indexNode{self: maxptr, height: o + 1}
		node.keys = append(node.keys, rkey)
		node.ptrs = append(node.ptrs, l, r)

		h.Tree.RootBlock = h.writeIndexNode(node)
		h.Tree.RootIsLeaf = false
	}

	return nil
}

func (h *BTreeDB5) removeLeaf(ptr uint, key Key) *leafNode {
	node := h.leafNode(ptr)

	index, ok := node.find(key)

	if ok {
		node.removeAt(index)
	}

	return node
}

func (h *BTreeDB5) removeIndex(ptr uint, key Key) *indexNode {
	node := h.indexNode(ptr)

	index, ok := node.find(key)

	if ok {
		index = index + 1
	}

	if node.height == 0 {
		mnode := h.removeLeaf(node.ptrs[index], key)

		if (h.BlockSize - 6) > mnode.size() {
			if index > 0 {
				lnode := h.leafNode(node.ptrs[index-1])
				if (h.BlockSize - 6) < lnode.size() {
					rkey, rdata := lnode.removeAt(len(lnode.keys) - 1)
					mnode.insertAt(0, rkey, rdata)
					node.replaceAtKey(index-1, rkey)
					node.replaceAtPtr(index-1, h.writeLeafNode(lnode))
					node.replaceAtPtr(index, h.writeLeafNode(mnode))
				} else {
					mnode.keys = append(lnode.keys, mnode.keys...)
					mnode.data = append(lnode.data, mnode.data...)
					node.replaceAtPtr(index, h.writeLeafNode(mnode))
					node.removeAtKey(index - 1)
					node.removeAtPtr(index - 1)
					h.freelist_push(lnode.self)
				}
			} else if index+1 < len(node.ptrs) {
				rnode := h.leafNode(node.ptrs[index+1])
				if (h.BlockSize - 6) < rnode.size() {
					rkey, rdata := rnode.removeAt(0)
					mnode.insertAt(len(mnode.keys), rkey, rdata)
					node.replaceAtKey(index, rkey)
					node.replaceAtPtr(index+1, h.writeLeafNode(rnode))
					node.replaceAtPtr(index, h.writeLeafNode(mnode))
				} else {
					mnode.keys = append(mnode.keys, rnode.keys...)
					mnode.data = append(mnode.data, rnode.data...)
					node.replaceAtPtr(index, h.writeLeafNode(mnode))
					node.removeAtKey(index)
					node.removeAtPtr(index + 1)
					h.freelist_push(rnode.self)
				}
			}
			// no more siblings
		}
	} else {
		mnode := h.removeIndex(node.ptrs[index], key)
		node.height = mnode.height + 1

		if len(mnode.ptrs) <= (h.freemax+1)/2 {
			if index > 0 {
				lnode := h.indexNode(node.ptrs[index-1])
				if len(lnode.ptrs) > (h.freemax+1)/2 {
					mnode.insertAtPtr(0, lnode.removeAtPtr(len(lnode.ptrs)-1))
					mnode.insertAtKey(0, node.keys[index-1])
					node.replaceAtKey(index-1, lnode.removeAtKey(len(lnode.keys)-1))
					node.replaceAtPtr(index-1, h.writeIndexNode(lnode))
					node.replaceAtPtr(index, h.writeIndexNode(mnode))
				} else {
					lnode.keys = append(lnode.keys, node.keys[index-1])
					mnode.keys = append(lnode.keys, mnode.keys...)
					mnode.ptrs = append(lnode.ptrs, mnode.ptrs...)
					node.replaceAtPtr(index, h.writeIndexNode(mnode))
					node.removeAtKey(index - 1)
					node.removeAtPtr(index - 1)
					h.freelist_push(lnode.self)
				}
			} else if index+1 < len(node.ptrs) {
				rnode := h.indexNode(node.ptrs[index+1])
				if len(rnode.ptrs) > (h.freemax+1)/2 {
					mnode.insertAtPtr(len(mnode.ptrs), rnode.removeAtPtr(0))
					mnode.insertAtKey(len(mnode.keys), node.keys[index])
					node.replaceAtKey(index, rnode.removeAtKey(0))
					node.replaceAtPtr(index, h.writeIndexNode(mnode))
					node.replaceAtPtr(index+1, h.writeIndexNode(rnode))
				} else {
					mnode.keys = append(mnode.keys, node.keys[index])
					mnode.keys = append(mnode.keys, rnode.keys...)
					mnode.ptrs = append(mnode.ptrs, rnode.ptrs...)
					node.replaceAtPtr(index, h.writeIndexNode(mnode))
					node.removeAtKey(index)
					node.removeAtPtr(index + 1)
					h.freelist_push(rnode.self)
				}
			} else {
				// no more siblings
				h.freelist_push(node.self)
				return mnode
			}
		}
	}

	return node
}

func (h *BTreeDB5) Remove(key Key) (e error) {
	defer func() {
		k := recover()
		if k != nil {
			e = errors.Errorf("%+v\n", k)
		}
	}()

	if h.Tree.RootIsLeaf {
		lnode := h.removeLeaf(h.Tree.RootBlock, key)
		h.Tree.RootBlock = h.writeLeafNode(lnode)
	} else {
		rnode := h.removeIndex(h.Tree.RootBlock, key)
		if len(rnode.ptrs) > 1 {
			h.Tree.RootBlock = h.writeIndexNode(rnode)
		} else {
			h.freelist_push(rnode.self)
			h.Tree.RootBlock = rnode.ptrs[0]
			if rnode.height == 0 {
				h.Tree.RootIsLeaf = true
			}
		}
	}

	return nil
}
