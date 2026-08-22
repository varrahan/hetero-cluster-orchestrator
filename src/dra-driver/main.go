package main

import (
	"log/slog"
	"os"

	"github.com/varrahan/hetero-cluster-orchestrater/src/dra-driver/internal/driver"
)

func main() {
	if err := driver.Run(); err != nil {
		slog.Error("DRA driver stopped", "error", err)
		os.Exit(1)
	}
}
