package queue

import (
	"fmt"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewBuffer(t *testing.T) {
	b := NewBuffer()
	assert.NotNil(t, b.queue)
	assert.NotNil(t, b.in)
	assert.NotNil(t, b.buf)
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
		if i == 2 {
			b.Cleanup()
		}
	}
}

func TestBuffer_Push_concurrent(t *testing.T) {
	go func() {
		for range time.Tick(5 * time.Second) {
			_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		}
	}()
	for j := 0; j < 10; j++ {

		b := NewBuffer()
		var wg sync.WaitGroup
		wg.Add(3)
		count := 10000
		go func() {
			for i := 0; i < count; i++ {
				b.Push(i)
			}
			wg.Done()
			fmt.Println("DONE", j)
		}()
		go func() {
			for i := 0; i < count; i++ {
				b.Push(i)
			}
			wg.Done()
			fmt.Println("DONE", j)
		}()
		j := 0
		go func() {
			for range b.out {
				j++
				if j == 2*count {
					b.Cleanup()
				}
			}
			wg.Done()
		}()
		wg.Wait()
	}
}

func TestNewFifo(t *testing.T) {
	fifo := NewFifo()
	assert.NotNil(t, fifo.elems)
	assert.NotNil(t, fifo.consume)
	assert.NotNil(t, fifo.next)
}

func TestFifo_Push_simple(t *testing.T) {
	fifo := NewFifo()
	fifo.Push("item1")
	fifo.Push("item2")
	fifo.Push("item3")

	assert.Equal(t, 3, fifo.Length())
}

func TestFifo_Push_concurrent(t *testing.T) {
	fifo := NewFifo()
	var wg sync.WaitGroup
	wg.Add(2)
	count := 100
	go func() {
		for i := 0; i < count; i++ {
			fifo.Push(i)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < count; i++ {
			fifo.Push(i)
		}
		wg.Done()
	}()
	wg.Wait()
	assert.Equal(t, 2*count, fifo.Length())
}

func TestFifo_Pop_simple(t *testing.T) {
	fifo := NewFifo()
	fifo.Push("item1")
	fifo.Push("item2")
	fifo.Push("item3")
	assert.Equal(t, "item1", fifo.Pop())
	assert.Equal(t, "item2", fifo.Pop())
	assert.Equal(t, "item3", fifo.Pop())
}

func TestFifo_Pop_empty(t *testing.T) {
	fifo := NewFifo()
	assert.Nilf(t, fifo.Pop(), "")
}

func TestFifo_Pop_concurrent(t *testing.T) {
	fifo := NewFifo()

	count := 100000
	for i := 0; i < count; i++ {
		fifo.Push(i)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	gr1 := 0
	go func() {
		for {
			if fifo.Pop() == nil {
				break
			}
			gr1++
		}
		wg.Done()
	}()

	gr2 := 0
	go func() {
		for {
			if fifo.Pop() == nil {
				break
			}
			gr2++
		}
		wg.Done()
	}()
	wg.Wait()
	assert.Equal(t, count, gr1+gr2)
}

func TestFifo_Length_simple(t *testing.T) {
	fifo := NewFifo()

	count := 1000
	for i := 0; i < count; i++ {
		fifo.Push(i)
	}
	assert.Equal(t, count, fifo.Length())
}

func TestFifo_Consume_simple(t *testing.T) {
	fifo := NewFifo()

	count := 1000
	for i := 0; i < count; i++ {
		fifo.Push(i)
	}

	var wg sync.WaitGroup
	wg.Add(count)
	i := 0
	go func() {
		for range fifo.Consume() {
			i++
			wg.Done()
		}
	}()
	wg.Wait()

	assert.Equal(t, count, i)
}
