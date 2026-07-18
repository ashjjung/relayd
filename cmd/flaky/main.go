// flaky is a test webhook receiver that fails the first N requests, then
// succeeds — for watching relayd's retries do their thing.
//
//	go run ./cmd/flaky -fail 2 -addr :9090
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
)

func main() {
	failN := flag.Int("fail", 2, "number of requests to fail before succeeding")
	addr := flag.String("addr", ":9090", "listen address")
	flag.Parse()

	var count atomic.Int64
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		n := count.Add(1)
		body, _ := io.ReadAll(r.Body)
		if n <= int64(*failN) {
			log.Printf("request %d (event=%s attempt=%s): FAILING with 500",
				n, r.Header.Get("X-Relayd-Event-Id"), r.Header.Get("X-Relayd-Attempt"))
			http.Error(w, "nope, not this time", http.StatusInternalServerError)
			return
		}
		log.Printf("request %d (event=%s attempt=%s): OK — body: %s",
			n, r.Header.Get("X-Relayd-Event-Id"), r.Header.Get("X-Relayd-Attempt"), body)
		fmt.Fprintln(w, "got it")
	})

	log.Printf("flaky receiver on %s — failing first %d request(s)", *addr, *failN)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
