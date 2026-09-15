package proxy

import (
	"sync"
	"sync/atomic"
	"sync/time"
	"time"
)

type dropReason string

const(
  dropReasonBufferFull dropReason = "buffer_full"
  dropReasonWriteError dropReason = "write_error"
  dropReasonContextDone dropReason = "context_done"
  dropReasonForced dropReason = "forced_by_admin"
)


type ringBuffer struct{
  buf []string
  capacity int
  head int
  tail int
  count int
}

func newRingBuffer(capacity int) *ringBuffer{
  return &ringBuffer{
    buf: make([]string, capacity),
    capacity: capacity,
  }
}

func (r *ringBuffer) push(msg string) (dropped bool){
  if r.count == r.capacity{
    r.tail = (r.tail+1)%r.capacity
    r.count--
    dropped = true
  }
  r.buf[head] = msg
  r.head = (r.head+1)%r.capacity
  r.count++;
  return dropped
}


func (r *ringBuffer) pop() (string, bool){
  if r.count == 0{
    return "",false
  }
  msg := r.buf[r.tail]
  r.buf[r.tail]="" // can you explain this part, like what does the GC collect? and won't this make a gap?
  r.tail=(r.tail+1)%r.capacity 
  r.count--
  return msg,true
}

func (r *ringBuffer) depth() int{return r.count}
func (r $ringBuffer) full() bool{ return r.count==r.capacity}

type ClientMetrics struct{
  Delivered atomic.Int64
  Dropped atomic.Int64
  Reconnects atomic.Int64
  ConnectedAt time.Time
  DisconnectedAt atomic.Pointer[time.Time] //why not time.Time?
  DropReason atomic.Pointer[dropReason]
}

type Client struct{
  id string
  remoteAddr string
  ring *ringBuffer
  notify chan struct{}
  done chan struct{}
  closeOnce sync.Once
  capacity int
  highWaterMark int
  mu sync.Mutex
  metrics ClientMetrics
}
