package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	maxResponseBytes = 8 << 20
	slurmUsername    = "slurm"
)

type Client struct {
	baseURL    *url.URL
	key        []byte
	httpClient *http.Client
}

type PendingJob struct {
	ID            uint32
	Partition     string
	Reason        string
	Priority      int64
	EligibleTime  int64
	HetJobID      uint32
	CPUs          int64
	NodeCount     int64
	Tasks         int64
	CPUsPerTask   int64
	TasksPerNode  int64
	MemoryPerCPU  int64
	MemoryPerNode int64
	CPUsPerTRES   string
	MemoryPerTRES string
	TRESPerJob    string
	TRESPerNode   string
	TRESPerSocket string
	TRESPerTask   string
	TRESRequested string
	RequiredNodes string
	Features      string
}

type job struct {
	ID            uint32      `json:"job_id"`
	Partition     string      `json:"partition"`
	State         jobState    `json:"state"`
	JobState      jobState    `json:"job_state"`
	Reason        string      `json:"state_reason"`
	Priority      slurmNumber `json:"priority"`
	EligibleTime  slurmNumber `json:"eligible_time"`
	HetJobID      slurmNumber `json:"het_job_id"`
	CPUs          slurmNumber `json:"cpus"`
	NodeCount     slurmNumber `json:"node_count"`
	Tasks         slurmNumber `json:"tasks"`
	CPUsPerTask   slurmNumber `json:"cpus_per_task"`
	TasksPerNode  slurmNumber `json:"tasks_per_node"`
	MemoryPerCPU  slurmNumber `json:"memory_per_cpu"`
	MemoryPerNode slurmNumber `json:"memory_per_node"`
	CPUsPerTRES   string      `json:"cpus_per_tres"`
	MemoryPerTRES string      `json:"memory_per_tres"`
	TRESPerJob    string      `json:"tres_per_job"`
	TRESPerNode   string      `json:"tres_per_node"`
	TRESPerSocket string      `json:"tres_per_socket"`
	TRESPerTask   string      `json:"tres_per_task"`
	TRESRequested string      `json:"tres_req_str"`
	RequiredNodes string      `json:"required_nodes"`
	Features      string      `json:"features"`
}

type jobState struct {
	Current []string `json:"current"`
}

func (s *jobState) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &s.Current); err == nil {
		return nil
	}
	var wrapped struct {
		Current []string `json:"current"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return err
	}
	s.Current = wrapped.Current
	return nil
}

type apiError struct {
	Description string `json:"description"`
	Error       string `json:"error"`
}

type jobsResponse struct {
	Errors []apiError `json:"errors"`
	Jobs   []job      `json:"jobs"`
}

type clustersResponse struct {
	Errors   []apiError          `json:"errors"`
	Clusters []accountingCluster `json:"clusters"`
}

type accountingCluster struct {
	Name string `json:"name"`
}

type slurmNumber struct {
	Value int64
}

func (n *slurmNumber) UnmarshalJSON(data []byte) error {
	var direct int64
	if err := json.Unmarshal(data, &direct); err == nil {
		n.Value = direct
		return nil
	}
	var wrapped struct {
		Number   int64 `json:"number"`
		Infinite bool  `json:"infinite"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return err
	}
	if wrapped.Infinite {
		return fmt.Errorf("infinite Slurm numeric value is unsupported")
	}
	n.Value = wrapped.Number
	return nil
}

func NewClient(baseURL string, key []byte, httpClient *http.Client) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
		return nil, fmt.Errorf("invalid Slurm REST URL")
	}
	if len(key) < 32 {
		return nil, fmt.Errorf("Slurm JWT key must contain at least 32 bytes")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &Client{
		baseURL:    parsed,
		key:        slices.Clone(key),
		httpClient: httpClient,
	}, nil
}

func (c *Client) PendingJobs(ctx context.Context) ([]PendingJob, error) {
	var response jobsResponse
	if err := c.request(ctx, http.MethodGet, "/slurm/v0.0.44/jobs/", nil, &response); err != nil {
		return nil, err
	}
	if err := responseError(response.Errors); err != nil {
		return nil, err
	}

	pending := make([]PendingJob, 0, len(response.Jobs))
	for _, job := range response.Jobs {
		states := job.State.Current
		if len(states) == 0 {
			states = job.JobState.Current
		}
		if slices.Contains(states, "PENDING") {
			pending = append(pending, PendingJob{
				ID: job.ID, Partition: job.Partition, Reason: job.Reason,
				Priority: job.Priority.Value, EligibleTime: job.EligibleTime.Value,
				HetJobID: uint32(job.HetJobID.Value),
				CPUs:     job.CPUs.Value, NodeCount: job.NodeCount.Value, Tasks: job.Tasks.Value,
				CPUsPerTask: job.CPUsPerTask.Value, TasksPerNode: job.TasksPerNode.Value,
				MemoryPerCPU: job.MemoryPerCPU.Value, MemoryPerNode: job.MemoryPerNode.Value,
				CPUsPerTRES: job.CPUsPerTRES, MemoryPerTRES: job.MemoryPerTRES,
				TRESPerJob: job.TRESPerJob, TRESPerNode: job.TRESPerNode,
				TRESPerSocket: job.TRESPerSocket, TRESPerTask: job.TRESPerTask,
				TRESRequested: job.TRESRequested, RequiredNodes: job.RequiredNodes, Features: job.Features,
			})
		}
	}
	return pending, nil
}

func (c *Client) AccountingReady(ctx context.Context, clusterName string) error {
	var response clustersResponse
	if err := c.request(ctx, http.MethodGet, "/slurmdb/v0.0.44/clusters/", nil, &response); err != nil {
		return err
	}
	if err := responseError(response.Errors); err != nil {
		return err
	}
	if !slices.ContainsFunc(response.Clusters, func(cluster accountingCluster) bool {
		return cluster.Name == clusterName
	}) {
		return fmt.Errorf("Slurm accounting has no registered cluster %q", clusterName)
	}
	return nil
}

type Node struct {
	Name        string   `json:"name"`
	State       []string `json:"state"`
	Reservation string   `json:"reservation"`
	AllocCPUs   int64
	AllocMemory int64
	GRESUsed    string `json:"gres_used"`
}

type nodesResponse struct {
	Errors []apiError `json:"errors"`
	Nodes  []struct {
		Name        string      `json:"name"`
		State       []string    `json:"state"`
		Reservation string      `json:"reservation"`
		AllocCPUs   slurmNumber `json:"alloc_cpus"`
		AllocMemory slurmNumber `json:"alloc_memory"`
		GRESUsed    string      `json:"gres_used"`
	} `json:"nodes"`
}

func (c *Client) Nodes(ctx context.Context) ([]Node, error) {
	var response nodesResponse
	if err := c.request(ctx, http.MethodGet, "/slurm/v0.0.44/nodes/", nil, &response); err != nil {
		return nil, err
	}
	if err := responseError(response.Errors); err != nil {
		return nil, err
	}
	nodes := make([]Node, 0, len(response.Nodes))
	for _, node := range response.Nodes {
		nodes = append(nodes, Node{Name: node.Name, State: node.State, Reservation: node.Reservation, AllocCPUs: node.AllocCPUs.Value, AllocMemory: node.AllocMemory.Value, GRESUsed: node.GRESUsed})
	}
	return nodes, nil
}

func (c *Client) DrainNode(ctx context.Context, name, reason string) error {
	body := map[string]any{"state": []string{"DRAIN"}, "reason": reason}
	var response struct {
		Errors []apiError `json:"errors"`
	}
	if err := c.request(ctx, http.MethodPost, "/slurm/v0.0.44/node/"+url.PathEscape(name), body, &response); err != nil {
		return err
	}
	return responseError(response.Errors)
}

func (c *Client) DeleteNode(ctx context.Context, name string) error {
	return c.request(ctx, http.MethodDelete, "/slurm/v0.0.44/node/"+url.PathEscape(name), nil, nil)
}

func (c *Client) request(ctx context.Context, method, path string, body, output any) error {
	token, err := c.token()
	if err != nil {
		return err
	}
	target := c.baseURL.ResolveReference(&url.URL{Path: path})
	var input io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode Slurm REST request: %w", err)
		}
		input = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), input)
	if err != nil {
		return fmt.Errorf("create Slurm REST request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("X-SLURM-USER-NAME", slurmUsername)
	request.Header.Set("X-SLURM-USER-TOKEN", token)

	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("call Slurm REST: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Slurm REST returned HTTP %d", response.StatusCode)
	}
	if output == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	limited := &io.LimitedReader{R: response.Body, N: maxResponseBytes + 1}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode Slurm REST response: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("decode Slurm REST response: trailing data")
	}
	if limited.N == 0 {
		return fmt.Errorf("Slurm REST response exceeds %d bytes", maxResponseBytes)
	}
	return nil
}

func (c *Client) token() (string, error) {
	now := time.Now().Unix()
	return jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iat": now, "exp": now + 60, "sun": slurmUsername,
	}).SignedString(c.key)
}

func responseError(apiErrors []apiError) error {
	if len(apiErrors) == 0 {
		return nil
	}
	parts := make([]string, 0, len(apiErrors))
	for _, item := range apiErrors {
		message := item.Description
		if message == "" {
			message = item.Error
		}
		parts = append(parts, message)
	}
	return fmt.Errorf("Slurm REST errors: %s", strings.Join(parts, "; "))
}
