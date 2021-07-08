package service

import (
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/stretchr/testify/assert"
)

func TestNewBuffer(t *testing.T) {
	b := NewQueue()
	assert.NotNil(t, b.buf)
	assert.NotNil(t, b.notify)
	assert.NotNil(t, b.out)
	assert.NotNil(t, b.stop)
}

func TestQueue_Push_simple(t *testing.T) {
	b := NewQueue()
	b.Schedule(&peer.AddrInfo{ID: peer.ID(0)})
	b.Schedule(&peer.AddrInfo{ID: peer.ID(1)})
	b.Schedule(&peer.AddrInfo{ID: peer.ID(2)})

	i := 0
	for elem := range b.out {
		assert.EqualValues(t, i, elem)
		i++
		if i == 3 {
			break
		}
	}
	b.Close()
}

func TestQueue_Push_concurrent(t *testing.T) {
	b := NewQueue()
	var wg sync.WaitGroup
	wg.Add(3)
	count := 10000
	go func() {
		for i := 0; i < count; i++ {
			b.Schedule(&peer.AddrInfo{ID: peer.ID(i)})
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < count; i++ {
			b.Schedule(&peer.AddrInfo{ID: peer.ID(i)})
		}
		wg.Done()
	}()
	i := 0
	go func() {
		for range b.out {
			i++
			if i == 2*count {
				b.Close()
			}
		}
		wg.Done()
	}()
	wg.Wait()
}
