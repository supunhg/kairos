package sync

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bits []uint64
	k    int
	n    uint
	m    uint
}

func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	n := float64(expectedItems)
	p := falsePositiveRate
	m := uint(math.Ceil(-n * math.Log(p) / (math.Ln2 * math.Ln2)))
	k := int(math.Round(float64(m) / n * math.Ln2))
	if k < 1 {
		k = 1
	}
	if m < 64 {
		m = 64
	}
	return &BloomFilter{
		bits: make([]uint64, (m+63)/64),
		k:    k,
		m:    m,
	}
}

func NewBloomFilterFromBits(bits []uint64, k int) *BloomFilter {
	return &BloomFilter{
		bits: bits,
		k:    k,
		m:    uint(len(bits) * 64),
	}
}

func (bf *BloomFilter) Add(data []byte) {
	bf.n++
	h1, h2 := hash64(data)
	for i := 0; i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(bf.m)
		bf.bits[idx/64] |= 1 << (idx % 64)
	}
}

func (bf *BloomFilter) AddString(s string) {
	bf.Add([]byte(s))
}

func (bf *BloomFilter) Contains(data []byte) bool {
	h1, h2 := hash64(data)
	for i := 0; i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(bf.m)
		if bf.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) ContainsString(s string) bool {
	return bf.Contains([]byte(s))
}

func (bf *BloomFilter) Count() uint {
	return bf.n
}

func (bf *BloomFilter) Marshal() []byte {
	buf := make([]byte, 8+8+8+len(bf.bits)*8)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(bf.k))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(bf.m))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(len(bf.bits)))
	for i, v := range bf.bits {
		binary.LittleEndian.PutUint64(buf[24+i*8:24+(i+1)*8], v)
	}
	return buf
}

func UnmarshalBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 24 {
		return nil, ErrInvalidOp
	}
	k := int(binary.LittleEndian.Uint64(data[0:8]))
	m := uint(binary.LittleEndian.Uint64(data[8:16]))
	numWords := int(binary.LittleEndian.Uint64(data[16:24]))
	if len(data) < 24+numWords*8 {
		return nil, ErrInvalidOp
	}
	bits := make([]uint64, numWords)
	for i := 0; i < numWords; i++ {
		bits[i] = binary.LittleEndian.Uint64(data[24+i*8 : 24+(i+1)*8])
	}
	bf := NewBloomFilterFromBits(bits, k)
	bf.m = m
	return bf, nil
}

func hash64(data []byte) (uint64, uint64) {
	h := fnv.New128a()
	h.Write(data)
	sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(sum[0:8]), binary.LittleEndian.Uint64(sum[8:16])
}

func (bf *BloomFilter) EstimatedItems() float64 {
	x := float64(bf.countSetBits()) / float64(bf.m)
	return -float64(bf.m) / float64(bf.k) * math.Log(1-x)
}

func (bf *BloomFilter) countSetBits() int {
	count := 0
	for _, v := range bf.bits {
		count += popcount(v)
	}
	return count
}

func popcount(x uint64) int {
	count := 0
	for x != 0 {
		x &= x - 1
		count++
	}
	return count
}

func (bf *BloomFilter) Merge(other *BloomFilter) error {
	if len(bf.bits) != len(other.bits) {
		return ErrInvalidOp
	}
	for i := range bf.bits {
		bf.bits[i] |= other.bits[i]
	}
	return nil
}

func (bf *BloomFilter) FalsePositiveRate() float64 {
	return math.Pow(1-math.Exp(-float64(bf.k)*float64(bf.n)/float64(bf.m)), float64(bf.k))
}
