package transport

import(
  "encoding/json"
  "fmt"
  "log/slog"
  "net/http"
  "os"
  "strings"
  "time"

  "github.com/Akramzg/flowgate/config"
  "github.com/Akramzg/flowgate/internal/proxy"
)

type Handler struct{
  broker *proxy.Broker
  cfg *config.Config
  logger *slog.Logger
}

func NewHandler(broker *proxy.Broker, cfg *config.Config, logger *slog.Logger) *Handler{
  return &Handler{broker: broker, cfg: cfg, logger: logger}
}

func (h *Handler) Register(mux *http.ServeMux){
  mux.HandleFunc("/events",h.handleSSE)
  mux.HandleFunc("/slow",h.handleSlowSSE)
  mux.HandleFunc("/health",h.handleHealth)
  mux.HandleFunc("/metrics",h.handleMetrics)
  mux.HandleFunc("/admin/clients",h.handleAdminClients)
  mux.HandleFunc("/admin/disconnect/",h.handleAdminDisconnect)
  mux.HandleFunc("/tcp-stats",h.handleTCPStats)

}

func (h *Handler) handleSSE(w http.ResponseWriter, r *http.Request){
  h.serveSSE(w,r,0)
}

func (h *Handler) handleSlowSSE(w http.ResponseWriter, r*http.Request){
  h.serveSSE(w,r,h.cfg.SlowClientDelay)
}

func (h *Handler) serveSSE(w http.ResponseWriter, r *http.Request, delay time.Duration) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	rc := http.NewResponseController(w)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // tells nginx to disable buffering
	w.Header().Set("Access-Control-Allow-Origin", "*")

	clientID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), sanitizeAddr(r.RemoteAddr))
	client := h.broker.RegisterClient(clientID, r.RemoteAddr)
	defer h.broker.UnregisterClient(client)

	h.logger.Info("sse connection opened",
		"client_id", clientID,
		"remote_addr", r.RemoteAddr,
		"slow", delay > 0,
	)

	for {
		select {
		case <-client.Done():
			// broker disconnected this client — send a close event
			fmt.Fprintf(w, "event: close\ndata: disconnected by server\n\n")
			flusher.Flush()
			return

		case <-r.Context().Done():
			return

		case <-client.Notify():
			for {
				msg, ok := client.Pop()
				if !ok {
					break
				}

				if delay > 0 {
					time.Sleep(delay)
				}

				// extend write deadline per event — compatible with long-lived connections
				rc.SetWriteDeadline(time.Now().Add(h.cfg.WriteDeadline))

				if _, err := fmt.Fprintf(w, "data: %s\n\n", msg); err != nil {
					client.Disconnect(proxy.DropReasonWriteError)
					return
				}
			}
			flusher.Flush()
		}
	}
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":         "ok",
		"active_clients": h.broker.Metrics.ActiveClients.Load(),
		"uptime":         time.Since(startTime).String(),
	})
}

type metricsResponse struct {
	ActiveClients        int64 `json:"active_clients"`
	TotalConnections     int64 `json:"total_connections"`
	TotalEventsReceived  int64 `json:"total_events_received"`
	TotalEventsDelivered int64 `json:"total_events_delivered"`
	TotalEventsDropped   int64 `json:"total_events_dropped"`
	UpstreamReconnects   int64 `json:"upstream_reconnects"`
}

func (h *Handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metricsResponse{
		ActiveClients:        h.broker.Metrics.ActiveClients.Load(),
		TotalConnections:     h.broker.Metrics.TotalConnections.Load(),
		TotalEventsReceived:  h.broker.Metrics.TotalEventsReceived.Load(),
		TotalEventsDelivered: h.broker.Metrics.TotalEventsDelivered.Load(),
		TotalEventsDropped:   h.broker.Metrics.TotalEventsDropped.Load(),
		UpstreamReconnects:   h.broker.Metrics.UpstreamReconnects.Load(),
	})
}

func (h *Handler) handleAdminClients(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.broker.Snapshots())
}

func (h *Handler) handleAdminDisconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/admin/disconnect/")
	if id == "" {
		http.Error(w, "missing client id", http.StatusBadRequest)
		return
	}

	if h.broker.ForceDisconnect(id) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "client %s disconnected", id)
	} else {
		http.Error(w, "client not found", http.StatusNotFound)
	}
}

func (h *Handler) handleTCPStats(w http.ResponseWriter, r *http.Request) {
	data, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stateMap := map[string]string{
		"01": "ESTABLISHED", "02": "SYN_SENT", "03": "SYN_RECV",
		"04": "FIN_WAIT1", "05": "FIN_WAIT2", "06": "TIME_WAIT",
		"07": "CLOSE", "08": "CLOSE_WAIT", "09": "LAST_ACK",
		"0A": "LISTEN", "0B": "CLOSING",
	}

	type connStat struct {
		Local   string `json:"local"`
		Remote  string `json:"remote"`
		State   string `json:"state"`
		TxQueue string `json:"tx_queue"`
		RxQueue string `json:"rx_queue"`
	}

	var stats []connStat
	for _, line := range strings.Split(string(data), "\n")[1:] {
		fields := strings.Fields(line)
		if len(fields) < 12 {
			continue
		}
		queues := strings.Split(fields[4], ":")
		if len(queues) != 2 {
			continue
		}
		state := stateMap[strings.ToUpper(fields[3])]
		if state == "" {
			state = fields[3]
		}
		stats = append(stats, connStat{
			Local:   fields[1],
			Remote:  fields[2],
			State:   state,
			TxQueue: queues[0],
			RxQueue: queues[1],
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// sanitizeAddr strips port from remote addr for use in client IDs.
func sanitizeAddr(addr string) string {
	if i := strings.LastIndex(addr, ":"); i != -1 {
		return addr[:i]
	}
	return addr
}

var startTime = time.Now()

