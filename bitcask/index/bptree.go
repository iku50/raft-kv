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

func (b *BPNode) insert(key []byte, value *data.LogRecordIndex) {
	index := b.search(key)
	if !b.isLeaf {
		if index == 0 {
			b.children[0].insert(key, value)
		} else {
			b.children[index-1].insert(key, value)
		}
		return
	}
	exact := len(b.elements) > 0 && index < len(b.elements) && bytes.Equal(key, b.elements[index].Key)
	if !exact {
		b.elementInsert(index)
	}
	b.elements[index].Key = key
	b.elements[index].Value = value
	if len(b.elements) > b.m {
		b.split()
	}
}

func (b *BPNode) delete(key []byte) {
	index := b.search(key)
	if len(b.elements) > 0 && index < len(b.elements) && bytes.Equal(key, b.elements[index].Key) {
		b.elements = append(b.elements[:index], b.elements[index+1:]...)
	}
	if len(b.elements) < b.m/2 {
		b.unbalanced = false
	}
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
		b.elements = append(b.elements[:0], &BPElement{Key: left.elements[0].Key}, &BPElement{Key: right.elements[0].Key})
	} else {
		// delete b and insert left, right into parent
		ind := b.parent.search(b.elements[0].Key)
		if len(b.parent.elements) > ind && !bytes.Equal(b.elements[0].Key, b.parent.elements[ind].Key) {
			ind--
		}
		b.parent.children = append(b.parent.children[:ind], append([]*BPNode{left, right}, b.parent.children[ind+1:]...)...)
		b.parent.elements = append(b.parent.elements[:ind], append([]*BPElement{&BPElement{Key: left.elements[0].Key}, &BPElement{Key: right.elements[0].Key}}, b.parent.elements[ind+1:]...)...)
		left.parent = b.parent
		right.parent = b.parent
		if len(b.parent.elements) > b.m {
			b.parent.split()
		}
	}
}

func (b *BPNode) reBalance() {
	childlen := make([]int, len(b.children))
	for i := range childlen {
		childlen[i] = len(b.children[i].elements)
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
		return b.children[ind].find(key)
	}
}

func (b *BPTree) Put(key []byte, value *data.LogRecordIndex) {
	b.root.insert(key, value)
}

func (b *BPTree) Get(key []byte) *data.LogRecordIndex {
	return b.root.find(key)
}

func (b *BPTree) Delete(key []byte) {
	b.root.delete(key)
}
