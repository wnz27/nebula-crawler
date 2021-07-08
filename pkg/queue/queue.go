package queue

import (
	"sync"
)

// Buffer exposes an API that allows sending events/jobs/items on a go channel that won't block. On the other side
// it exposes a channel that can be read which allows consuming these event. This buffer is an unbound FIFO queue.
type Buffer struct {
	wg     sync.WaitGroup
	lk     sync.RWMutex
	queue  []interface{}
	in     chan interface{}
	out    chan interface{}
	notify chan struct{}
	stop   chan struct{}
}

func NewBuffer() *Buffer {
	b := &Buffer{
		queue:  make([]interface{}, 0),
		in:     make(chan interface{}),
		out:    make(chan interface{}),
		notify: make(chan struct{}),
		stop:   make(chan struct{}),
	}

	b.wg.Add(2)

	go b.read()
	go b.write()

	return b
}

// read reads from the in channel and enqueues the items in the local buffer. It also notifies the writer go routine
// about new items if it isn't busy at the moment.
func (b *Buffer) read() {
	defer b.wg.Done()
	for in := range b.in {
		b.enqueue(in)
		b.notifyWriter()
	}
}

// enqueue just puts the item in the slice in a thread safe way.
func (b *Buffer) enqueue(in interface{}) {
	b.lk.Lock()
	defer b.lk.Unlock()
	b.queue = append(b.queue, in)
}

// notifyWriter notifies the writer go routine that there are new items to be written to the consumers in a non-blocking way.
func (b *Buffer) notifyWriter() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

// write drains the queue of elements and sends them to the out channel. If there are no elements anymore it waits
// until notified.
func (b *Buffer) write() {
	defer b.wg.Done()
	for {
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
		case <-b.notify:
		case <-b.stop:
			return
		}
	}
}

// Close stops all go routines in a blocking way
func (b *Buffer) Close() {
	close(b.stop)
	close(b.in)
	b.wg.Wait()
	close(b.out)
}

func (b *Buffer) Push(elem interface{}) {
	b.in <- elem
}

func (b *Buffer) Schedule() chan<- interface{} {
	return b.in
}

func (b *Buffer) Consume() <-chan interface{} {
	return b.out
}
