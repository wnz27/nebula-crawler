package queue

// resize logic taken from

import (
	"sync"
)

const minLen = 16

// Queue exposes an API that allows sending events/jobs/items on a go channel that won't block. On the other side
// it exposes a channel that can be read which allows consuming these event. This buffer is an unbound FIFO buf.
type Queue struct {
	wg sync.WaitGroup

	lk    sync.RWMutex
	buf   []interface{}
	tail  int
	head  int
	count int

	out    chan interface{}
	notify chan struct{}
	stop   chan struct{}
}

func NewQueue() *Queue {
	q := &Queue{
		buf:    make([]interface{}, minLen),
		out:    make(chan interface{}),
		notify: make(chan struct{}),
		stop:   make(chan struct{}),
	}

	q.wg.Add(1)

	go q.write()

	return q
}

// push puts an element at the end of the queue.
// Adapted from: https://github.com/eapache/queue/blob/master/queue.go
func (q *Queue) push(in interface{}) {
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

// pop removes and returns the element from the front of the queue. It returns nil if the queue is empty
// Adapted from: https://github.com/eapache/queue/blob/master/queue.go
func (q *Queue) pop() interface{} {
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

// resizes the queue to fit exactly twice its current contents
// this can result in shrinking if the queue is less than half-full
// Adapted from: https://github.com/eapache/queue/blob/master/queue.go
func (q *Queue) resize() {
	newBuf := make([]interface{}, q.count<<1)

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
			select {
			case q.out <- item:
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

func (q *Queue) Consume() <-chan interface{} {
	return q.out
}

// Schedule pushes the item onto the queue and notifies the writer that there are new items to be published.
func (q *Queue) Schedule(item interface{}) {
	q.push(item)
	q.notifyWriter()
}
