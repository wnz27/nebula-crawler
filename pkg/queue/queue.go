package queue

import (
	"fmt"
	"sync"

	"go.uber.org/atomic"
)

type Buffer struct {
	wg    sync.WaitGroup
	lk    sync.RWMutex
	queue []interface{}
	in    chan interface{}
	buf   chan interface{}
	out   chan interface{}
	stop  chan struct{}
}

func NewBuffer() *Buffer {
	b := &Buffer{
		queue: make([]interface{}, 0),
		in:    make(chan interface{}),
		buf:   make(chan interface{}),
		out:   make(chan interface{}),
		stop:  make(chan struct{}),
	}

	b.wg.Add(2)

	go b.read()
	go b.write()

	return b
}

func (b *Buffer) Cleanup() {
	close(b.stop)
	close(b.in)
	b.wg.Wait()
	close(b.out)
	close(b.buf)
}

func (b *Buffer) read() {
	defer b.wg.Done()
	for in := range b.in {
		b.enqueue(in)
	}
}

func (b *Buffer) write() {
	defer b.wg.Done()
	for {
		select {
		case <-b.stop:
			return
		default:
		}

		b.lk.Lock()
		if len(b.queue) > 0 {
			item := b.queue[0]
			b.queue = b.queue[1:]
			b.lk.Unlock()
			b.out <- item
			continue
		}
		b.lk.Unlock()

		select {
		case elem := <-b.buf:
			b.out <- elem
		case <-b.stop:
			return
		}
	}
}

func (b *Buffer) enqueue(item interface{}) {
	select {
	case b.buf <- item:
		return
	case <-b.stop:
		return
	default:
	}

	b.lk.Lock()
	defer b.lk.Unlock()
	b.queue = append(b.queue, item)
}

func (b *Buffer) Push(elem interface{}) {
	select {
	case b.in <- elem:
	case <-b.stop:
		return
	}
}

func (b *Buffer) Consume() <-chan interface{} {
	return b.out
}

type Fifo struct {
	mutex sync.RWMutex
	elems []interface{}

	queued  atomic.Uint32
	next    chan interface{}
	consume chan interface{}
}

func NewFifo() *Fifo {
	fifo := &Fifo{
		elems:   make([]interface{}, 0),
		next:    make(chan interface{}, 1),
		consume: make(chan interface{}),
	}

	go func() {
		for elem := range fifo.next {
			fifo.consume <- elem
			fifo.queued.Dec()
			fifo.mutex.Lock()
			if next := fifo.pop(); next != nil {
				fifo.next <- next
			}
			fifo.mutex.Unlock()
		}
	}()

	return fifo
}

func (f *Fifo) Push(elem interface{}) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	select {
	case f.next <- elem:
		f.queued.Inc()
	default:
		f.elems = append(f.elems, elem)
	}
}

func (f *Fifo) Pop() interface{} {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.pop()
}

func (f *Fifo) pop() interface{} {
	if len(f.elems)+int(f.queued.Load()) == 0 {
		return nil
	}

	select {
	case elem := <-f.next:
		f.queued.Dec()
		if next := f.pop(); next != nil {
			f.next <- next
			f.queued.Inc()
		}
		fmt.Println("RETURN CHANNEL", elem)
		return elem
	default:
		elem := f.elems[0]
		f.elems = f.elems[1:]
		fmt.Println("RETURN OTHER", elem)
		return elem
	}
}

func (f *Fifo) Length() int {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	return len(f.elems) + int(f.queued.Load())
}

func (f *Fifo) Consume() <-chan interface{} {
	return f.consume
}
