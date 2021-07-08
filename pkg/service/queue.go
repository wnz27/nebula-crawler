package service

// resize logic taken from

import (
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	"go.uber.org/atomic"
)

const minLen = 16

// Queue is a thread safe Queue implementation that allows pushing items to it and reading them in a go channel in
// various workers. This Queue is using an auto-resizing ring buffer and is FIFO in nature.
type Queue struct {
	wg sync.WaitGroup

	lk    sync.RWMutex
	buf   []*peer.AddrInfo
	tail  int
	head  int
	count int

	// popped indicates whether an item was popped but not yet published due to no consumers. This item is technically still in the Queue, so this flag is used for the Queue length calculation.
	popped atomic.Bool

	out    chan *peer.AddrInfo
	notify chan struct{}
	stop   chan struct{}
}

// NewQueue initializes a new Queue instance.
func NewQueue() *Queue {
	q := &Queue{
		buf:    make([]*peer.AddrInfo, minLen),
		out:    make(chan *peer.AddrInfo),
		notify: make(chan struct{}),
		stop:   make(chan struct{}),
	}

	q.wg.Add(1)

	go q.write()

	return q
}

// push puts an element at the end of the Queue.
// Adapted from: https://github.com/eapache/queue/blob/master/queue.go
func (q *Queue) push(in *peer.AddrInfo) {
	q.lk.Lock()
	defer q.lk.Unlock()

	if q.count == len(q.buf) {
		q.resize()
	}

	q.buf[q.tail] = in
	// bitwise modulus
	q.tail = (q.tail + 1) & (len(q.buf) - 1)
	q.count++
}

// pop removes and returns the element from the front of the Queue. It returns nil if the Queue is empty
// Adapted from: https://github.com/eapache/queue/blob/master/queue.go
func (q *Queue) pop() *peer.AddrInfo {
	q.lk.Lock()
	defer q.lk.Unlock()

	if q.count <= 0 {
		return nil
	}
	ret := q.buf[q.head]
	q.buf[q.head] = nil

	// bitwise modulus
	q.head = (q.head + 1) & (len(q.buf) - 1)
	q.count--

	// Resize down if buffer 1/4 full.
	if len(q.buf) > minLen && (q.count<<2) == len(q.buf) {
		q.resize()
	}
	return ret
}

// resizes the Queue to fit exactly twice its current contents
// this can result in shrinking if the Queue is less than half-full
// Adapted from: https://github.com/eapache/queue/blob/master/queue.go
func (q *Queue) resize() {
	newBuf := make([]*peer.AddrInfo, q.count<<1)

	if q.tail > q.head {
		copy(newBuf, q.buf[q.head:q.tail])
	} else {
		n := copy(newBuf, q.buf[q.head:])
		copy(newBuf[n:], q.buf[:q.tail])
	}

	q.head = 0
	q.tail = q.count
	q.buf = newBuf
}

// notifyWriter notifies the writer go routine that there are new items to be written to the consumers in a non-blocking way.
func (q *Queue) notifyWriter() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// write drains the buf of elements and sends them to the out channel. If there are no elements anymore it waits
// until notified.
func (q *Queue) write() {
	defer q.wg.Done()
	for {
		if item := q.pop(); item != nil {
			q.popped.Store(true)
			select {
			case q.out <- item:
				q.popped.Store(false)
			case <-q.stop:
				return
			}
			continue
		}

		select {
		case <-q.notify:
		case <-q.stop:
			return
		}
	}
}

// Close stops all go routines in a blocking way
func (q *Queue) Close() {
	close(q.stop)
	q.wg.Wait()
	close(q.out)
}

func (q *Queue) Consume() <-chan *peer.AddrInfo {
	return q.out
}

// Schedule pushes the item onto the Queue and notifies the writer that there are new items to be published.
func (q *Queue) Schedule(item *peer.AddrInfo) {
	q.push(item)
	q.notifyWriter()
}

// Length returns the current length of the Queue.
func (q *Queue) Length() int {
	q.lk.RLock()
	defer q.lk.RUnlock()

	if q.popped.Load() {
		return q.count + 1
	} else {
		return q.count
	}
}
