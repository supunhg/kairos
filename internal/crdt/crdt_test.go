package crdt

import (
	"testing"
	"time"
)

func TestLWWRegister(t *testing.T) {
	r := NewLWWRegister()
	r.Set("hello", "node1")
	if r.Get() != "hello" {
		t.Fatalf("expected hello, got %v", r.Get())
	}
}

func TestLWWRegisterMerge(t *testing.T) {
	a, b := NewLWWRegister(), NewLWWRegister()
	a.Set("from_a", "node1")
	time.Sleep(time.Millisecond)
	b.Set("from_b", "node2")

	a.Merge(b)
	if a.Get() != "from_b" {
		t.Fatalf("expected from_b (later), got %v", a.Get())
	}
}

func TestLWWRegisterConcurrentSameValue(t *testing.T) {
	a, b := NewLWWRegister(), NewLWWRegister()

	a.Set("same", "node1")
	b.Set("same", "node2")

	a.Merge(b)
	b.Merge(a)

	if a.Get() != b.Get() {
		t.Fatalf("expected convergence: %v vs %v", a.Get(), b.Get())
	}
}

func TestGCounter(t *testing.T) {
	c := NewGCounter()
	c.Increment("a", 5)
	c.Increment("b", 3)
	c.Increment("a", 2)
	if v := c.Value(); v != 10 {
		t.Fatalf("expected 10, got %d", v)
	}
}

func TestGCounterMerge(t *testing.T) {
	a, b := NewGCounter(), NewGCounter()
	a.Increment("node1", 5)
	b.Increment("node2", 3)
	a.Increment("node1", 2)
	b.Increment("node1", 7)

	a.Merge(b)
	if v := a.Value(); v != 10 {
		t.Fatalf("after merge expected %d, got %d", 10, v)
	}
}

func TestPNCounter(t *testing.T) {
	c := NewPNCounter()
	c.Increment("a", 10)
	c.Increment("a", -3)
	if v := c.Value(); v != 7 {
		t.Fatalf("expected 7, got %d", v)
	}
}

func TestLWWMap(t *testing.T) {
	m := NewLWWMap()
	m.Set("key1", "value1", "node1")
	m.Set("key2", 42, "node2")

	if v := m.Get("key1"); v != "value1" {
		t.Fatalf("expected value1, got %v", v)
	}
	if v := m.Get("key2"); v != 42 {
		t.Fatalf("expected 42, got %v", v)
	}

	m.Delete("key2", "node3")
	if m.Get("key2") != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestLWWMapMerge(t *testing.T) {
	a, b := NewLWWMap(), NewLWWMap()
	a.Set("shared", "from_a", "node1")
	b.Set("shared", "from_b", "node2")

	a.Merge(b)
	val := a.Get("shared")
	if val != "from_b" {
		t.Fatalf("expected from_b (later), got %v", val)
	}
}

func TestRGAInsertAndText(t *testing.T) {
	r := NewRGA()
	r.Insert(0, "Hello", "node1")
	if text := r.Text(); text != "Hello" {
		t.Fatalf("expected Hello, got %s", text)
	}
}

func TestRGASequential(t *testing.T) {
	r := NewRGA()
	r.Insert(0, "Hello", "node1")
	r.Insert(5, " World", "node1")
	if text := r.Text(); text != "Hello World" {
		t.Fatalf("expected 'Hello World', got '%s'", text)
	}
}

func TestRGADelete(t *testing.T) {
	r := NewRGA()
	r.Insert(0, "Hello", "node1")
	r.Delete(1, 3)
	if text := r.Text(); text != "Ho" {
		t.Fatalf("expected 'Ho' (delete ell), got '%s'", text)
	}
}

func TestRGAMerge(t *testing.T) {
	a, b := NewRGA(), NewRGA()
	a.Insert(0, "abc", "n1")
	b.Insert(0, "abc", "n2")

	a.Merge(b)
	b.Merge(a)

	if a.Text() != b.Text() {
		t.Fatalf("expected convergence: '%s' vs '%s'", a.Text(), b.Text())
	}
}

func TestRGAMergeNoDataLoss(t *testing.T) {
	a, b := NewRGA(), NewRGA()
	a.Insert(0, "Hello", "n1")
	b.Insert(0, " World", "n2")

	a.Merge(b)
	b.Merge(a)

	if a.Text() != b.Text() {
		t.Fatalf("expected convergence: '%s' vs '%s'", a.Text(), b.Text())
	}

	aChars := make(map[rune]int)
	for _, c := range "Hello World" {
		aChars[c]++
	}
	for _, c := range a.Text() {
		aChars[c]--
	}
	for c, count := range aChars {
		if count != 0 {
			t.Fatalf("missing or extra character %c (count=%d)", c, count)
		}
	}
}

func TestConcurrentIncrement(t *testing.T) {
	c := NewGCounter()
	done := make(chan bool, 50)
	for i := 0; i < 50; i++ {
		go func(n int) {
			c.Increment("node", 1)
			done <- true
		}(i)
	}
	for i := 0; i < 50; i++ {
		<-done
	}
	if v := c.Value(); v != 50 {
		t.Fatalf("expected 50, got %d", v)
	}
}

func TestLWWMapKeys(t *testing.T) {
	m := NewLWWMap()
	m.Set("a", 1, "n1")
	m.Set("b", 2, "n1")
	m.Set("c", 3, "n1")
	m.Delete("b", "n1")

	keys := m.Keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d: %v", len(keys), keys)
	}
}

func TestCRDTCommutativity(t *testing.T) {
	g1, g2 := NewGCounter(), NewGCounter()
	g1.Increment("a", 5)
	g1.Increment("b", 3)

	g2.Increment("b", 3)
	g2.Increment("a", 5)

	if g1.Value() != g2.Value() {
		t.Fatal("GCounter should be commutative")
	}

	m1, m2 := NewLWWMap(), NewLWWMap()
	m1.Set("x", 1, "n1")
	m2.Set("x", 2, "n2")

	m1cp, m2cp := NewLWWMap(), NewLWWMap()
	m1cp.Merge(m1)
	m2cp.Merge(m2)

	if m1cp.Keys()[0] != m2cp.Keys()[0] {
		t.Fatal("LWWMap merge should converge order")
	}
}
