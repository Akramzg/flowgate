package proxy

import(
  "context"
  "fmt"
  "log/slog"
  "sync"
  "sync/atomic"
  "time"

  "github.com/Akramzg/flowgate/config"
  "github.com/Akramzg/flowgate/internal/upstream"

)

type BrokerMetrics struct{
  TotalConnections atomic.Int64
  ActiveClients atomic.Int64
  TotalEventsReceived atomic.Int64
  TotalEventsDelivered atomic.Int64
  TotalEventsDropped atomic.Int64
  UpstreamReconnects atomic.Int64
}


// Lock ordering: b.mu must always be acquired before c.mu (Client.mu).
// Never acquire Client.mu first and then attempt to acquire b.mu.
// Violation of this order will deadlock.

type Broker struct{
  cfg *config.Config
  upstream *upstream.Client
  clients map[string]*Client
  mu sync.RWMutex
  policy *backpressurePolicy
  logger *slog.Logger
  Metrics BrokerMetrics 
  register chan *Client
  unregister chan *Client
}


func NewBroker(cfg *config.Config, up *upstream.Client, logger *slog.Logger,
) *Broker{
  return &Broker{
    cfg: cfg,
    upstream: up,
    clients: make(map[string]*Client),
    policy: newBackpressurePolicy(logger),
    logger: logger,
    register: make(chan *Client, 64),
    unregister: make(chan *Client, 64),
  }
}

func (b *Broker) Run(ctx context.Context){
  for{
    select{
    case <-ctx.Done():
      return
    
    case client := <-b.register:
      b.mu.Lock() 
      b.clients[client.id]=client
      b.mu.Unlock()
      b.Metrics.ActiveClients.Add(1)
      b.Metrics.TotalConnections.Add(1)
      b.logger.Info("client connected", "client_id", client.id, "remote_addr", client.remoteAddr, "active", b.Metrics.ActiveClients.Load())
      
    case client := <-b.unregister:
      b.mu.Lock()
      if _, exists := b.clients[client.id]; exists {
        delete(b.clients, client.id)
        b.Metrics.ActiveClients.Add(-1)
        b.logger.Info("client disconnected",
          "client_id", client.id,
          "delivered", client.metrics.Delivered.Load(),
          "dropped", client.metrics.Dropped.Load(),
          "uptime", time.Since(client.metrics.ConnectedAt).Round(time.Millisecond),
        )
      }
      b.mu.Unlock()
    case event := <-b.upstream.Event():
      b.Metrics.TotalEventsReceived.Add(1)
      b.fanOut(event)
    }
  }
}


func (b *Broker) fanOut(event upstream.Event) {
	msg := event.Data
	if event.Type != "" {
		msg = fmt.Sprintf("[%s] %s", event.Type, event.Data)
	}

	var toDisconnect []*Client

	b.mu.RLock()
	for _, client := range b.clients {
		client.mu.Lock()
		shouldDisconnect := b.policy.apply(client, msg)
		if shouldDisconnect {
			toDisconnect = append(toDisconnect, client)
		} else {
			b.Metrics.TotalEventsDelivered.Add(1)
			client.metrics.Delivered.Add(1)
		}
		client.mu.Unlock()
	}
	b.mu.RUnlock()

	if len(toDisconnect) == 0 {
		return
	}

	b.mu.Lock()
	for _, client := range toDisconnect {
		if _, exists := b.clients[client.id]; exists {
			delete(b.clients, client.id)
			b.Metrics.ActiveClients.Add(-1)
			b.Metrics.TotalEventsDropped.Add(1)
			client.disconnect(DropReasonBufferFull)
			b.logger.Error("client buffer full — disconnected",
				"client_id", client.id,
				"remote_addr", client.remoteAddr,
				"delivered", client.metrics.Delivered.Load(),
				"dropped", client.metrics.Dropped.Load(),
			)
		}
	}
	b.mu.Unlock()
}

func (b *Broker) RegisterClient(id, remoteAddr string) *Client{
  c := newClient(id, remoteAddr, b.cfg.ClientBufferSize, b.cfg.HighWaterRatio)
  b.register <- c
  return c
}


func (b *Broker) UnregisterClient(c *Client){
  b.unregister <- c
}

func (b *Broker) ForceDisconnect(id string) bool{
  b.mu.RLock()
  client, ok := b.clients[id]
  b.mu.RUnlock()

  if !ok{
    return false
  }
  client.disconnect(DropReasonForced)
  b.unregister <- client
  return true
}

func (b *Broker) Snapshots() []ClientSnapshot{
  b.mu.RLock()
  defer b.mu.RUnlock()
  out := make([]ClientSnapshot, 0, len(b.clients))
  for _, c:= range b.clients{
    out = append(out, c.snapshot())
  }
  return out
}
