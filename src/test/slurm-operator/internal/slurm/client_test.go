package slurm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func TestClient(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-SLURM-USER-NAME") != "slurm" {
			t.Error("missing Slurm user header")
		}
		token, err := jwt.Parse(r.Header.Get("X-SLURM-USER-TOKEN"), func(*jwt.Token) (any, error) { return key, nil }, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
		if err != nil {
			t.Error(err)
			return
		}
		claims := token.Claims.(jwt.MapClaims)
		if claims["sun"] != "slurm" || claims["exp"].(float64)-claims["iat"].(float64) != 60 {
			t.Errorf("unexpected claims: %#v", claims)
		}

		switch r.URL.Path {
		case "/slurm/v0.0.45/jobs/":
			_, _ = w.Write([]byte(`{"jobs":[{"job_id":7,"het_job_id":{"number":0,"set":false,"infinite":false},"partition":"compute","job_state":["PENDING"],"state_reason":"Resources"},{"job_id":8,"state":{"current":["RUNNING"]},"nodes":"worker-[01-02]"}]}`))
		case "/slurmdb/v0.0.45/clusters/":
			_, _ = w.Write([]byte(`{"clusters":[{"name":"research"}]}`))
		case "/slurm/v0.0.45/nodes/":
			_, _ = w.Write([]byte(`{"nodes":[{"name":"worker-a","state":["IDLE"],"alloc_cpus":{"number":0,"set":true,"infinite":false},"alloc_memory":0,"partitions":["compute"]}]}`))
		case "/slurm/v0.0.45/node/worker-a":
			if r.Method != http.MethodPost && r.Method != http.MethodDelete {
				t.Errorf("unexpected node method %s", r.Method)
			}
			_, _ = w.Write([]byte(`{}`))
		case "/slurm/v0.0.45/job/8":
			if r.Method != http.MethodDelete || r.URL.Query().Get("signal") != "USR1" {
				t.Errorf("unexpected signal request %s %s", r.Method, r.URL.String())
			}
			_, _ = w.Write([]byte(`{}`))
		case "/slurm/v0.0.45/job/8/requeue":
			if r.Method != http.MethodGet {
				t.Errorf("unexpected requeue method %s", r.Method)
			}
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, key, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := client.PendingJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != 7 || jobs[0].Reason != "Resources" {
		t.Fatalf("unexpected pending jobs: %#v", jobs)
	}
	active, err := client.Jobs(context.Background())
	if err != nil || len(active) != 2 || !active[1].UsesAnyNode(map[string]struct{}{"worker-02": {}}) {
		t.Fatalf("active jobs=%#v err=%v", active, err)
	}
	if err := client.AccountingReady(context.Background(), "research"); err != nil {
		t.Fatal(err)
	}
	if err := client.AccountingReady(context.Background(), "other"); err == nil {
		t.Fatal("unregistered accounting cluster accepted")
	}
	nodes, err := client.Nodes(context.Background())
	if err != nil || len(nodes) != 1 || nodes[0].Name != "worker-a" || nodes[0].AllocCPUs != 0 {
		t.Fatalf("nodes=%#v err=%v", nodes, err)
	}
	if err := client.DrainNode(context.Background(), "worker-a", "test"); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteNode(context.Background(), "worker-a"); err != nil {
		t.Fatal(err)
	}
	if err := client.SignalJob(context.Background(), 8, "USR1"); err != nil {
		t.Fatal(err)
	}
	if err := client.RequeueJob(context.Background(), 8); err != nil {
		t.Fatal(err)
	}
}

func TestExpandHostlist(t *testing.T) {
	nodes, err := expandHostlist("rack[1-2]node[01-02,04],plain", 10)
	want := []string{"rack1node01", "rack1node02", "rack1node04", "rack2node01", "rack2node02", "rack2node04", "plain"}
	if err != nil || !slices.Equal(nodes, want) {
		t.Fatalf("nodes=%v err=%v", nodes, err)
	}
	if _, err := expandHostlist("worker[1-100]", 10); err == nil {
		t.Fatal("oversized hostlist was accepted")
	}
}

func TestClientFailsClosed(t *testing.T) {
	if _, err := NewClient("http://example", []byte("short"), nil); err == nil {
		t.Fatal("short JWT key accepted")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"errors":[{"description":"denied"}]}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, []byte("01234567890123456789012345678901"), server.Client())
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
	client, err = NewClient(trailing.URL, []byte("01234567890123456789012345678901"), trailing.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PendingJobs(context.Background()); err == nil {
		t.Fatal("trailing REST payload accepted")
	}
}

func TestRequeuedStateIncludesHeldTransition(t *testing.T) {
	if !requeuedState([]string{"REQUEUE_HOLD"}, "None") {
		t.Fatal("active requeue-hold state was not recognized")
	}
	if !requeuedState([]string{"CANCELLED", "REQUEUE_HOLD"}, "None") {
		t.Fatal("REST requeue-hold transition was not recognized")
	}
	if !requeuedState([]string{"CANCELLED"}, "job_requeued_in_held_state") {
		t.Fatal("held requeue transition was not recognized")
	}
	if requeuedState([]string{"PENDING", "REQUEUE_HOLD"}, "JobHeldUser") {
		t.Fatal("settled held job was recognized as transition demand")
	}
	if requeuedState([]string{"CANCELLED"}, "None") {
		t.Fatal("ordinary cancellation was recognized as a requeue")
	}
}
