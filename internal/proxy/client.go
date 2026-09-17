package proxy

import (
	"sync"
	"sync/atomic"
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
  r.buf[r.head] = msg
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
func (r *ringBuffer) full() bool{ return r.count==r.capacity}

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

func newClient(id, remoteAddr string, capacity int, highWaterRatio float64) *Client{
  c := &Client{
    id: id,
    remoteAddr: remoteAddr,
    ring: newRingBuffer(capacity),
    notify: make(chan struct{},1),
    done: make(chan struct{},1),
    capacity: capacity,
    highWaterMark: int(float64(capacity)*highWaterRatio),
  }
  c.metrics.ConnectedAt = time.Now()
  return c
}

func (c *Client) push(msg string){
  dropped := c.ring.push(msg)
  if dropped{
    c.metrics.Dropped.Add(1)
  }
  select{
  case c.notify<-struct{}{}:
  default:
  }
}

func (c *Client) disconnect(reason dropReason){ // explain this
  c.closeOnce.Do(func(){
    now := time.Now()
    c.metrics.DisconnectedAt.Store(&now)
    c.metrics.DropReason.Store(&reason)
    close(c.done)
  })
}

func (c *Client) isOverHighWaterMark() bool{
  return c.ring.depth() >= c.highWaterMark
}

type ClientSnapshot struct{
 	ID            string     `json:"id"`
	RemoteAddr    string     `json:"remote_addr"`
	BufferDepth   int        `json:"buffer_depth"`
	BufferCap     int        `json:"buffer_cap"`
	HighWaterMark int        `json:"high_water_mark"`
	Delivered     int64      `json:"delivered"`
	Dropped       int64      `json:"dropped"`
	ConnectedAt   time.Time  `json:"connected_at"`
	UptimeSeconds float64    `json:"uptime_seconds"` 
}

func (c *Client) snapshot() ClientSnapshot{
  c.mu.Lock()
  depth := c.ring.depth()
  c.mu.Unlock()

  return ClientSnapshot{
    ID: c.id,
    RemoteAddr: c.remoteAddr,
    BufferDepth: depth,
    BufferCap: c.capacity,
    HighWaterMark: c.highWaterMark,
    Delivered: c.metrics.Delivered.Load(),
    Dropped: c.metrics.Dropped.Load(),
    ConnectedAt: c.metrics.ConnectedAt,
    UptimeSeconds: time.Since(c.metrics.ConnectedAt).Seconds(),
  }

}


func (c *Client) Done() <-chan struct{}   { return c.done }
func (c *Client) Notify() <-chan struct{} { return c.notify }

func (c *Client) Pop() (string, bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.ring.pop()
}

func (c *Client) Disconnect(reason dropReason) {
    c.disconnect(reason)
}
