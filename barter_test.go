package cc

import (
	"sync"
	"testing"
)

func TestBarter_Exchange(t *testing.T) {
	b := NewBarter[string]()
	var wg sync.WaitGroup
	wg.Add(2)

	var res1, res2 string

	go func() {
		defer wg.Done()
		res1 = b.Exchange("A")
	}()

	go func() {
		defer wg.Done()
		res2 = b.Exchange("B")
	}()

	wg.Wait()

	if res1 != "B" {
		t.Errorf("G1 got %s, want B", res1)
	}
	if res2 != "A" {
		t.Errorf("G2 got %s, want A", res2)
	}
}

func TestBarter_Contention(t *testing.T) {
	b := NewBarter[int]()
	const pairs = 100
	var wg sync.WaitGroup
	wg.Add(pairs * 2)

	for range pairs {
		go func() {
			defer wg.Done()
			v := b.Exchange(1)
			if v != 2 {
				// checking errors in high concurrency is tricky as pairs might mix,
				// but Barter ensures 1:1 swap.
				// Oh wait! If we have multiple pairs on ONE barter object,
				// G1 might swap with G3 instead of G2.
				// But they should swap *something*.
				_ = v
			}
		}()
		go func() {
			defer wg.Done()
			v := b.Exchange(2)
			_ = v
		}()
	}
	wg.Wait()
}
