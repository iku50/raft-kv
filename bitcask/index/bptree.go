package index

// single go routine
import (
	"bytes"
	"fmt"
	"raft-kv/bitcask/data"
	"sort"
	"strings"
)

type keys [][]byte

func (k keys) Len() int {
	return len(k)
}

func (k keys) Less(i, j int) bool {
	for kk := 0; kk < 8; kk++ {
		if k[i][kk] < k[j][kk] {
			return true
		} else if k[i][kk] > k[j][kk] {
			return false
		}
	}
	return false
}

func (k keys) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

type BPTree struct {
	root  *BPNode
	m     int
	count int
}

type BPNode struct {
	isLeaf     bool
	m          int
	unbalanced bool
	children   []*BPNode
	elements   []*BPElement
	parent     *BPNode
	next       *BPNode
	prev       *BPNode
}

func (b *BPNode) root() *BPNode {
	if b.parent == nil {
		return b
	}
	return b.parent.root()
}

func NewBPTree(m int) *BPTree {
	return &BPTree{
		root:  NewLeafNode(m),
		m:     m,
		count: 0,
	}
}

type BPElement struct {
	Key   []byte
	Value *data.LogRecordIndex
}

func (b *BPElement) copy() *BPElement {
	nb := new(BPElement)
	nb.Key = make([]byte, len(b.Key))
	copy(nb.Key, b.Key)
	if b.Value == nil {
		return nb
	}
	nb.Value = new(data.LogRecordIndex)
	*nb.Value = *b.Value
	return nb
}

func NewLeafNode(m int) *BPNode {
	return &BPNode{
		isLeaf:   true,
		children: make([]*BPNode, 0),
		elements: make([]*BPElement, 0),
		m:        m,
	}
}

func NewNonLeafNode(m int) *BPNode {
	return &BPNode{
		children: make([]*BPNode, 0),
		elements: make([]*BPElement, 0),
		m:        m,
	}
}

func (b *BPNode) search(key []byte) int {
	return sort.Search(len(b.elements), func(i int) bool {
		return bytes.Compare(b.elements[i].Key, key) != -1
	})
}

func (b *BPNode) elementInsert(index int) {
	b.elements = append(b.elements, &BPElement{})
	for i := len(b.elements) - 1; i > index; i-- {
		b.elements[i] = b.elements[i-1].copy()
	}
}

func (b *BPNode) insert(key []byte, value *data.LogRecordIndex) bool {
	index := b.search(key)
	if !b.isLeaf {
		if index == 0 {
			return b.children[0].insert(key, value)
		} else {
			return b.children[index-1].insert(key, value)
		}
	}
	ok := false
	exact := len(b.elements) > 0 && index < len(b.elements) && bytes.Equal(key, b.elements[index].Key)
	if !exact {
		b.elementInsert(index)
		ok = true
	}
	b.elements[index].Key = key
	b.elements[index].Value = value
	if len(b.elements) > b.m {
		b.split()
	}
	return ok
}

func (b *BPNode) delete(key []byte) (*data.LogRecordIndex, bool) {
	if len(b.elements) == 0 {
		return nil, true
	}
	index := b.search(key)
	var e data.LogRecordIndex
	if b.isLeaf {
		if len(b.elements) > 0 && index < len(b.elements) && bytes.Equal(key, b.elements[index].Key) {
			b.elements = append(b.elements[:index], b.elements[index+1:]...)
			e = *b.elements[index].Value
		}
	} else {
		if 0 <= index && index < len(b.elements) && bytes.Equal(b.elements[index].Key, key) {
			b.children[index].delete(key)
		} else if index == 0 {
			b.children[0].delete(key)
		} else {
			b.children[index-1].delete(key)
		}
	}
	if len(b.elements) == 0 {
		b.parent.elements = append(b.parent.elements[:index], b.parent.elements[index+1:]...)
		b.parent.children = append(b.parent.children[:index], b.parent.children[index+1:]...)
		return &e, true
	} else if len(b.elements) < b.m/2 {
		b.reBalance()
		return &e, true
	}
	if index == 0 && bytes.Equal(b.elements[0].Key, key) {
		ind := b.parent.search(key)
		// could be deleted if merge
		if 0 < ind && ind < len(b.parent.elements) && bytes.Equal(b.parent.elements[ind-1].Key, key) {
			b.parent.elements[ind].Key = b.elements[0].Key
		}
	}
	return &e, true
}

func (b *BPNode) split() {
	index := len(b.elements) / 2
	var left *BPNode
	var right *BPNode
	if b.isLeaf {
		left = NewLeafNode(b.m)
		right = NewLeafNode(b.m)
	} else {
		left = NewNonLeafNode(b.m)
		right = NewNonLeafNode(b.m)
		left.children = make([]*BPNode, index)
		copy(left.children, b.children[:index])
		for i := range left.children {
			left.children[i].parent = left
		}
		right.children = make([]*BPNode, len(b.children[index:]))
		copy(right.children, b.children[index:])
		for i := range right.children {
			right.children[i].parent = right
		}
	}
	left.elements = make([]*BPElement, index)
	for i := range left.elements {
		left.elements[i] = b.elements[i].copy()
	}
	right.elements = make([]*BPElement, len(b.elements)-index)
	for i := range right.elements {
		right.elements[i] = b.elements[i+index].copy()
	}
	if b.parent == nil {
		b.isLeaf = false
		b.children = append(b.children[:0], left, right)
		left.parent = b
		right.parent = b
		left.next = right
		right.prev = left
		b.elements = append(b.elements[:0], &BPElement{Key: left.elements[0].Key}, &BPElement{Key: right.elements[0].Key})
	} else {
		// delete b and insert left, right into parent
		ind := b.parent.search(b.elements[0].Key)
		if len(b.parent.elements) > ind && !bytes.Equal(b.elements[0].Key, b.parent.elements[ind].Key) {
			ind--
		}
		if ind < 0 {
			ind = 0
		}
		b.parent.children = append(b.parent.children[:ind], append([]*BPNode{left, right}, b.parent.children[ind+1:]...)...)
		b.parent.elements = append(b.parent.elements[:ind], append([]*BPElement{&BPElement{Key: left.elements[0].Key}, &BPElement{Key: right.elements[0].Key}}, b.parent.elements[ind+1:]...)...)
		left.parent = b.parent
		right.parent = b.parent
		left.next = right
		right.prev = left
		if b.prev != nil {
			b.prev.next = left
			left.prev = b.prev
		}
		if b.next != nil {
			b.next.prev = right
			right.next = b.next
		}
		if len(b.parent.elements) > b.m {
			b.parent.split()
		}
	}
}

func (b *BPNode) reBalance() {
	// borrow one element from right sibling
	if b.next != nil {
		right := b.next
		if len(right.elements) > right.m/2 {
			// borrow from right sibling
			e := right.elements[0]
			right.elements = right.elements[1:]
			b.elements = append(b.elements, e)
			if !b.isLeaf {
				c := right.children[0]
				right.children = append(right.children[:0], right.children[1:]...)
				b.children = append(b.children, c)
				c.parent = b
			}
			ind := b.parent.search(e.Key)
			if ind > len(b.parent.elements)-1 {
				ind = len(b.parent.elements) - 1
			}
			b.parent.elements[ind].Key = right.elements[0].Key
			b.parent.elements[ind].Value = right.elements[0].Value
		} else {
			// merge with right sibling
			e := right.elements[0]
			b.elements = append(b.elements, right.elements...)
			if !b.isLeaf {
				b.children = append(b.children, right.children...)
				for i := range right.children {
					right.children[i].parent = b
				}
			}
			ind := b.parent.search(e.Key)
			if ind >= len(b.parent.elements)-1 {
				b.parent.elements = b.parent.elements[:ind]
				b.parent.children = b.parent.children[:ind]
			} else {
				b.parent.elements = append(b.parent.elements[:ind], b.parent.elements[ind+1:]...)
				b.parent.children = append(b.parent.children[:ind], b.parent.children[ind+1:]...)
			}
			b.next = right.next
		}
	} else if b.prev != nil {
		left := b.prev
		if len(left.elements) > left.m/2 {
			// borrow from left sibling
			e := left.elements[len(left.elements)-1]
			left.elements = left.elements[:len(left.elements)-1]
			b.elements = append([]*BPElement{e}, b.elements...)
			if !b.isLeaf {
				c := left.children[len(left.children)-1]
				left.children = append(left.children[:0], left.children[:len(left.children)-1]...)
				b.children = append([]*BPNode{c}, b.children...)
				c.parent = b
			}
			ind := b.parent.search(e.Key)
			b.parent.elements[ind].Key = left.elements[len(left.elements)-1].Key
			b.parent.elements[ind].Value = left.elements[len(left.elements)-1].Value
		} else {
			e := left.elements[len(left.elements)-1]
			b.elements = append(left.elements, b.elements...)
			if !b.isLeaf {
				b.children = append(left.children, b.children...)
				for i := range left.children {
					left.children[i].parent = b
				}
			}
			ind := b.parent.search(e.Key)
			b.parent.elements = append(b.parent.elements[:ind], b.parent.elements[ind+1:]...)
			b.parent.children = append(b.parent.children[:ind], b.parent.children[ind+1:]...)
			b.prev = left.prev
		}
	}
}

func (b *BPNode) String(n int) string {
	var builder strings.Builder

	indent := strings.Repeat("  ", n)

	if b.isLeaf {
		builder.WriteString(indent + "Leaf: ")
	} else {
		builder.WriteString(indent + "Node: ")
	}

	for i, e := range b.elements {
		builder.WriteString(fmt.Sprintf("%v", e))
		if i < len(b.elements)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString("\n")

	if !b.isLeaf {
		for _, c := range b.children {
			builder.WriteString(c.String(n + 1))
		}
	}

	return builder.String()
}

func (b *BPNode) find(key []byte) *data.LogRecordIndex {
	if b.isLeaf {
		for _, e := range b.elements {
			if bytes.Equal(e.Key, key) {
				return e.Value
			}
		}
		return nil
	} else {
		ind := b.search(key)
		if 0 <= ind && ind < len(b.elements) && bytes.Equal(b.elements[ind].Key, key) {
			return b.children[ind].find(key)
		}
		return b.children[ind-1].find(key)
	}
}

func (b *BPNode) scan(f func(key []byte, value *data.LogRecordIndex)) {
	if b.isLeaf {
		for _, e := range b.elements {
			f(e.Key, e.Value)
		}
		if b.next != nil {
			b.next.scan(f)
		}
	} else {
		if b.children != nil {
			b.children[0].scan(f)
		}
	}
}

func (b *BPTree) Put(key []byte, value *data.LogRecordIndex) *data.LogRecordIndex {
	ok := b.root.insert(key, value)
	if ok {
		b.count++
	}
	return value
}

func (b *BPTree) Iterator(reverse bool) Iterator {
	return nil
}
func (b *BPTree) Size() int {
	return b.count
}
func (b *BPTree) Close() error {
	return nil
}
func (b *BPTree) Get(key []byte) *data.LogRecordIndex {
	return b.root.find(key)
}

func (b *BPTree) Delete(key []byte) (*data.LogRecordIndex, bool) {
	return b.root.delete(key)
}

func (b *BPTree) Scan() map[string]*data.LogRecordIndex {
	result := make(map[string]*data.LogRecordIndex)
	b.root.scan(func(key []byte, value *data.LogRecordIndex) {
		result[string(key)] = value
	})
	return result
}
