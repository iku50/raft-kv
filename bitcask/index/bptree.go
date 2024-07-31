package index

// single go routine
import (
	"bytes"
	"fmt"
	"raft-kv/bitcask/data"
	"sort"
	"strings"
	"sync"
)

type BPTree struct {
	root  *BPNode
	m     int
	count int
}

type BPNode struct {
	isLeaf     bool
	m          int
	mu         sync.RWMutex
	unbalanced bool
	children   []*BPNode
	elements   BPElements
	parent     *BPNode
	next       *BPNode
	prev       *BPNode
}

func (b *BPNode) Len() int {
	return len(b.elements)
}

func (b *BPNode) Less(i, j int) bool {
	return bytes.Compare(b.elements[i].Key, b.elements[j].Key) == -1
}

func (b *BPNode) Swap(i, j int) {
	b.elements[i], b.elements[j] = b.elements[j], b.elements[i]
	if !b.isLeaf {
		b.children[i], b.children[j] = b.children[j], b.children[i]
	}
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

type BPElements []*BPElement

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
		elements: make(BPElements, 0),
		mu:       sync.RWMutex{},
		m:        m,
	}
}

func NewNonLeafNode(m int) *BPNode {
	return &BPNode{
		children: make([]*BPNode, 0),
		elements: make(BPElements, 0),
		m:        m,
		mu:       sync.RWMutex{},
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
	b.mu.Lock()
	defer b.mu.Unlock()
	index := b.search(key)
	if !b.isLeaf {
		if index == 0 {
			ok := b.children[0].insert(key, value)
			b.elements[0] = b.children[0].elements[0]
			return ok
		} else {
			return b.children[index-1].insert(key, value)
		}
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
	return true
}

func (b *BPNode) delete(key []byte) (*data.LogRecordIndex, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.elements) == 0 {
		return nil, true
	}
	index := b.search(key)
	var e data.LogRecordIndex
	if b.isLeaf {
		if len(b.elements) > 0 && index < len(b.elements) && bytes.Equal(key, b.elements[index].Key) {
			e = *b.elements[index].Value
			b.elements = append(b.elements[:index], b.elements[index+1:]...)
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
	if len(b.elements) < b.m/2 {
		if b.parent == nil {
			return &e, true
		}
		b.reBalance()
		return &e, true
	}
	return &e, true
}

func (b *BPNode) copy() *BPNode {
	nb := new(BPNode)
	nb.m = b.m
	nb.isLeaf = b.isLeaf
	nb.elements = make([]*BPElement, len(b.elements))
	nb.mu = sync.RWMutex{}
	nb.parent = b.parent
	nb.next = b.next
	nb.prev = b.prev
	for i := range b.elements {
		nb.elements[i] = b.elements[i].copy()
	}
	if !b.isLeaf {
		nb.children = make([]*BPNode, len(b.children))
		for i := range b.children {
			nb.children[i] = b.children[i].copy()
		}
	}
	return nb
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
		copy(left.children, b.children)
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
	left.next = right
	right.prev = left
	if b.parent == nil {
		b.isLeaf = false
		b.children = []*BPNode{left, right}
		b.elements = []*BPElement{{Key: left.elements[0].Key}, {Key: right.elements[0].Key}}
		left.parent = b
		right.parent = b
	} else {
		// delete b and insert left, right into parent
		ind := b.parent.search(b.elements[0].Key)
		if len(b.parent.elements) > ind && !bytes.Equal(b.elements[0].Key, b.parent.elements[ind].Key) {
			ind--
		}
		if ind < 0 {
			ind = 0
		}
		if ind > len(b.parent.elements)-1 {
			b.parent.children = append(b.parent.children, left, right)
			b.parent.elements = append(b.parent.elements, &BPElement{Key: left.elements[0].Key}, &BPElement{Key: right.elements[0].Key})
		} else {
			b.parent.children = append(b.parent.children[:ind], append([]*BPNode{left, right}, b.parent.children[ind+1:]...)...)
			b.parent.elements = append(b.parent.elements[:ind], append([]*BPElement{{Key: left.elements[0].Key}, {Key: right.elements[0].Key}}, b.parent.elements[ind+1:]...)...)
		}
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

func (b *BPNode) changeParent(node *BPNode) {
	ind := b.parent.search(b.elements[0].Key)
	if ind == 0 {
		b.parent.deleteFirst()
	}
	b.parent = node
	node.children = append(node.children, b)
	node.elements = append(node.elements, b.elements[0])
	return
}

// lend mean borrow one child node or element form neighbor
//
// dir: 1 means borrow from right, -1 means borrow from left
//
// dir == 1:
//
//	1        3         ->     1    	 4
//	1 2      3 4 5            1 2 3  4 5
//
// dir == -1:
//
//	1         4         ->     1    3
//	1 2 3     4 5              1 2  3 4 5
func (b *BPNode) lend(node *BPNode, dir int) {
	if dir == 1 {
		b.elements = append(b.elements, node.elements[0])
	} else if dir == -1 {
		b.elements = append([]*BPElement{node.elements[len(node.elements)-1]}, b.elements...)
		node.elements = node.elements[:len(node.elements)-1]
	}
	if !node.isLeaf {
		if dir == 1 {
			node.children[0].changeParent(b)
			node.deleteFirst()
		} else if dir == -1 {
			node.children[len(node.children)-1].changeParent(b)
		}
	}
}

func (b *BPNode) deleteFirst() {
	p := b.parent
	v := b.elements[0]
	nv := b.elements[1]
	for p != nil {
		ind := p.search(v.Key)
		p.elements[ind].Key = nv.Key
		if ind > 0 {
			break
		}
		p = p.parent
	}
	b.elements = b.elements[1:]
	if !b.isLeaf {
		b.children = b.children[1:]
	}
}

func (b *BPNode) append(k *BPNode) {
	ind := b.search(k.elements[0].Key)
	if ind <= 0 {
		b.elements = append([]*BPElement{b.elements[0]}, b.elements...)
		if !b.isLeaf {
			b.children = append([]*BPNode{b}, b.children...)
		}

	}
}

func (b *BPNode) merge(node *BPNode, dir int) {
	if b.parent != node.parent {
		node.changeParent(b.parent)
	}
	if !b.isLeaf {
		for i := range node.children {
			node.children[i].parent = b
		}
		b.children = append(b.children, node.children...)
	}
	b.elements = append(b.elements, node.elements...)
	index := b.parent.search(node.elements[0].Key)
	b.parent.elements = append(b.parent.elements[:index], b.parent.elements[index+1:]...)
	b.parent.children = append(b.parent.children[:index], b.parent.children[index+1:]...)
	if dir == 1 {
		b.next = node.next
	} else {
		b.prev = node.prev
	}
}

func (b *BPNode) reBalance() {
	// borrow one element from right sibling
	if b.next != nil {
		if len(b.next.elements) > b.m/2 {
			b.lend(b.next, 1)
		} else {
			b.merge(b.next, 1)
		}
	} else if b.prev != nil {
		if len(b.prev.elements) > b.m/2 {
			b.lend(b.prev, -1)
		} else {
			b.merge(b.prev, -1)
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
	b.mu.RLock()
	defer b.mu.RUnlock()
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
		} else if ind >= len(b.elements) {
			return b.children[len(b.elements)-1].find(key)
		} else if ind <= 0 {
			return b.children[0].find(key)
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
