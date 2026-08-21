package slurm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-SLURM-USER-NAME") != "slurm" {
			t.Error("missing Slurm user header")
		}
		parts := strings.Split(r.Header.Get("X-SLURM-USER-TOKEN"), ".")
		if len(parts) != 3 {
			t.Fatal("invalid JWT")
		}
		claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			t.Fatal(err)
		}
		var claims map[string]any
		if err := json.Unmarshal(claimsJSON, &claims); err != nil {
			t.Fatal(err)
		}
		if claims["sun"] != "slurm" || claims["exp"].(float64)-claims["iat"].(float64) != 60 {
			t.Fatalf("unexpected claims: %#v", claims)
		}

		switch r.URL.Path {
		case "/slurm/v0.0.44/jobs/":
			_, _ = w.Write([]byte(`{"jobs":[{"job_id":7,"het_job_id":{"number":0,"set":false,"infinite":false},"partition":"compute","state":{"current":["PENDING"]},"state_reason":"Resources"},{"job_id":8,"state":{"current":["RUNNING"]}}]}`))
		case "/slurmdb/v0.0.44/clusters/":
			_, _ = w.Write([]byte(`{"clusters":[{"name":"research"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "slurm", []byte("01234567890123456789012345678901"), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return time.Unix(100, 0) }
	jobs, err := client.PendingJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != 7 || jobs[0].Reason != "Resources" {
		t.Fatalf("unexpected pending jobs: %#v", jobs)
	}
	if err := client.AccountingReady(context.Background(), "research"); err != nil {
		t.Fatal(err)
	}
	if err := client.AccountingReady(context.Background(), "other"); err == nil {
		t.Fatal("unregistered accounting cluster accepted")
	}
}

func TestClientFailsClosed(t *testing.T) {
	if _, err := NewClient("http://example", "slurm", []byte("short"), nil); err == nil {
		t.Fatal("short JWT key accepted")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"errors":[{"description":"denied"}]}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "slurm", []byte("01234567890123456789012345678901"), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PendingJobs(context.Background()); err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("unexpected error: %v", err)
	}

	trailing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[]} {}`))
	}))
	defer trailing.Close()
	client, err = NewClient(trailing.URL, "slurm", []byte("01234567890123456789012345678901"), trailing.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PendingJobs(context.Background()); err == nil {
		t.Fatal("trailing REST payload accepted")
	}
}
