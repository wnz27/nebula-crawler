package queue

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBuffer(t *testing.T) {
	b := NewBuffer()
	assert.NotNil(t, b.queue)
	assert.NotNil(t, b.in)
	assert.NotNil(t, b.notify)
	assert.NotNil(t, b.out)
	assert.NotNil(t, b.stop)
}

func TestBuffer_Push_simple(t *testing.T) {
	b := NewBuffer()
	b.Push(0)
	b.Push(1)
	b.Push(2)

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

func TestBuffer_Push_concurrent(t *testing.T) {
	b := NewBuffer()
	var wg sync.WaitGroup
	wg.Add(3)
	count := 10000
	go func() {
		for i := 0; i < count; i++ {
			b.Push(i)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < count; i++ {
			b.Push(i)
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
