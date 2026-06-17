package sync

import (
	"testing"
)

func TestBloomFilterAddContains(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.AddString("hello")
	bf.AddString("world")

	if !bf.ContainsString("hello") {
		t.Fatal("expected 'hello' to be in bloom filter")
	}
	if !bf.ContainsString("world") {
		t.Fatal("expected 'world' to be in bloom filter")
	}
}

func TestBloomFilterFalsePositive(t *testing.T) {
	bf := NewBloomFilter(10, 0.1)
	for i := 0; i < 10; i++ {
		bf.AddString(string(rune('A' + i)))
	}
	fpRate := bf.FalsePositiveRate()
	if fpRate > 0.2 {
		t.Fatalf("false positive rate too high: %f", fpRate)
	}
}

func TestBloomFilterNotFound(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.AddString("present")

	if bf.ContainsString("missing") {
		t.Fatal("expected 'missing' to not be in bloom filter")
	}
}

func TestBloomFilterMarshalUnmarshal(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.AddString("event-1")
	bf.AddString("event-2")
	bf.AddString("event-3")

	data := bf.Marshal()
	restored, err := UnmarshalBloomFilter(data)
	if err != nil {
		t.Fatal(err)
	}

	if !restored.ContainsString("event-1") {
		t.Fatal("expected 'event-1' after unmarshal")
	}
	if !restored.ContainsString("event-3") {
		t.Fatal("expected 'event-3' after unmarshal")
	}
}

func TestBloomFilterMerge(t *testing.T) {
	a := NewBloomFilter(100, 0.01)
	b := NewBloomFilter(100, 0.01)

	a.AddString("from-a")
	b.AddString("from-b")

	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}

	if !a.ContainsString("from-a") {
		t.Fatal("expected 'from-a' after merge")
	}
	if !a.ContainsString("from-b") {
		t.Fatal("expected 'from-b' after merge")
	}
}

func TestBloomFilterCount(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	for i := 0; i < 100; i++ {
		bf.AddString(string(rune(i)))
	}
	if bf.Count() != 100 {
		t.Fatalf("expected count 100, got %d", bf.Count())
	}
}

func TestBloomFilterEstimatedItems(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	for i := 0; i < 50; i++ {
		bf.AddString(string(rune(i)))
	}
	est := bf.EstimatedItems()
	if est < 30 || est > 70 {
		t.Fatalf("estimated items %f out of range", est)
	}
}

func TestBloomFilterEmpty(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	if bf.ContainsString("anything") {
		t.Fatal("empty bloom filter should not contain anything")
	}
}
