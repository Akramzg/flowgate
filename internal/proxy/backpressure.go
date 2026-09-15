package proxy

import "log/slog"

type backpressurePolicy struct{
  logger *slog.Logger
}

// back pressure policy to decide what to do when a client's buffer is under pressure
func newBackpressurePolicy(logger *slog.Logger) *backpressurePolicy{
  return &backpressurePolicy{logger: logger}
}


// to evaluate whether the client should be disconnected or not
func (p *backpressurePolicy) apply(c *Client, msg string) (disconnect bool){
  if c.isOverHighWaterMark(){
    p.logger.Warn("client over high water mark ",
      "client_id:",c.id,
      " buffer_depth",c.ring.depth(),
      " high_water_mark:",c.highWaterMark,
      " capacity:",c.capacity)
  }

  c.push(msg)

  if c.ring.full(){
    p.logger.Error("client buffer is full after push - disconnecting",
      "client_id: ",c.id,
      " remote_addr: ", c.remoteAddr,
      " delivered: ", c.metrics.Delivered.Load(),
      " dropped: ",c.metrics.Dropped.Load(),
    )
    return true
  }
  return false
}

