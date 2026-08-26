package main

import (
	"log/slog"
	"os"

	"github.com/varrahan/hetero-cluster-orchestrater/src/optical-dra-driver/internal/driver"
)

func main() {
	if err := driver.Run(); err != nil {
		slog.Error("optical DRA driver stopped", "error", err)
		os.Exit(1)
	}
}
