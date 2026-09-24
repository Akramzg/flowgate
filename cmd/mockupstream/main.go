package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		ticker := time.NewTicker(5 * time.Millisecond) // 200 events/sec
		defer ticker.Stop()

		i := 0
		for {
			select {
			case t := <-ticker.C:
				i++
				fmt.Fprintf(w, "id: %d\ndata: token-%d at %s\n\n", i, i, t.Format(time.RFC3339Nano))
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	})

	log.Println("mock upstream on :9000")
	log.Fatal(http.ListenAndServe(":9000", nil))
}
