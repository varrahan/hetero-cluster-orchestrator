package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func main() {
	config, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	socket := "/run/gputpu-checkpoint/flusher.sock"
	if err := os.MkdirAll(filepath.Dir(socket), 0770); err != nil {
		log.Fatal(err)
	}
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(socket, 0660); err != nil {
		log.Fatal(err)
	}
	httpServer := &http.Server{Handler: &server{store: objectStore{client: config.client, bucket: config.bucket, clusterUID: config.clusterUID}, sharedRoot: "/dev/shm/ai-orch", clusterUID: config.clusterUID, budget: config.budget, transactions: map[string]*transaction{}, active: map[string]string{}, runOwners: map[string]uint64{}}, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Fatal(fmt.Errorf("serve checkpoint socket: %w", err))
	}
}
