package fill

import (
	"sync"
	"testing"
)

func TestPins(t *testing.T) {
	var p Pins
	if p.Pinned(1) {
		t.Fatal("pinned before acquire")
	}
	r1 := p.Acquire(1)
	r2 := p.Acquire(1)
	r1()
	r1() // second call is a no-op
	if !p.Pinned(1) {
		t.Fatal("released twice by one release func")
	}
	r2()
	if p.Pinned(1) {
		t.Fatal("still pinned")
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); p.Acquire(2)() }()
	}
	wg.Wait()
	if p.Pinned(2) {
		t.Fatal("concurrent pins leaked")
	}
}
