package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
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
	Requeued      bool
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
	Nodes         string      `json:"nodes"`
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
	response, err := c.fetchJobs(ctx)
	if err != nil {
		return nil, err
	}

	pending := make([]PendingJob, 0, len(response))
	for _, job := range response {
		states := job.State.Current
		if len(states) == 0 {
			states = job.JobState.Current
		}
		requeued := requeuedState(states, job.Reason)
		if slices.Contains(states, "PENDING") || requeued {
			pending = append(pending, PendingJob{
				ID: job.ID, Partition: job.Partition, Reason: job.Reason, Requeued: requeued,
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

type Job struct {
	ID       uint32
	HetJobID uint32
	State    []string
	Reason   string
	Nodes    []string
}

func (j Job) RootID() uint32 {
	if j.HetJobID != 0 {
		return j.HetJobID
	}
	return j.ID
}

func (j Job) Pending() bool {
	return slices.Contains(j.State, "PENDING") || requeuedState(j.State, j.Reason)
}

func (j Job) Terminal() bool {
	return slices.ContainsFunc(j.State, func(state string) bool {
		return slices.Contains([]string{"BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "REVOKED", "TIMEOUT"}, state)
	}) && !j.Pending()
}

func (j Job) UsesAnyNode(names map[string]struct{}) bool {
	return slices.ContainsFunc(j.Nodes, func(name string) bool {
		_, exists := names[name]
		return exists
	})
}

func (c *Client) Jobs(ctx context.Context) ([]Job, error) {
	jobs, err := c.fetchJobs(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]Job, 0, len(jobs))
	for _, item := range jobs {
		states := item.State.Current
		if len(states) == 0 {
			states = item.JobState.Current
		}
		nodes, err := expandHostlist(item.Nodes, 10000)
		if err != nil {
			return nil, fmt.Errorf("decode nodes for Slurm job %d: %w", item.ID, err)
		}
		result = append(result, Job{ID: item.ID, HetJobID: uint32(item.HetJobID.Value), State: slices.Clone(states), Reason: item.Reason, Nodes: nodes})
	}
	return result, nil
}

func expandHostlist(value string, limit int) ([]string, error) {
	var result []string
	groups, err := splitHostlist(value)
	if err != nil {
		return nil, err
	}
	for _, group := range groups {
		expanded := []string{""}
		for len(group) != 0 {
			open := strings.IndexByte(group, '[')
			if open < 0 {
				for i := range expanded {
					expanded[i] += group
				}
				break
			}
			close := strings.IndexByte(group[open:], ']')
			if close < 0 {
				return nil, fmt.Errorf("unclosed hostlist range %q", group)
			}
			close += open
			prefix, options := group[:open], strings.Split(group[open+1:close], ",")
			var values []string
			for _, option := range options {
				parts := strings.Split(option, "-")
				if len(parts) == 1 && parts[0] != "" {
					values = append(values, parts[0])
					continue
				}
				if len(parts) != 2 {
					return nil, fmt.Errorf("invalid hostlist range %q", option)
				}
				start, err1 := strconv.Atoi(parts[0])
				end, err2 := strconv.Atoi(parts[1])
				if err1 != nil || err2 != nil || start > end || end-start+1 > limit {
					return nil, fmt.Errorf("invalid hostlist range %q", option)
				}
				width := max(len(parts[0]), len(parts[1]))
				for number := start; number <= end; number++ {
					values = append(values, fmt.Sprintf("%0*d", width, number))
				}
			}
			var next []string
			for _, base := range expanded {
				for _, option := range values {
					next = append(next, base+prefix+option)
					if len(next)+len(result) > limit {
						return nil, fmt.Errorf("hostlist exceeds %d nodes", limit)
					}
				}
			}
			expanded, group = next, group[close+1:]
		}
		result = append(result, expanded...)
		if len(result) > limit {
			return nil, fmt.Errorf("hostlist exceeds %d nodes", limit)
		}
	}
	return result, nil
}

func splitHostlist(value string) ([]string, error) {
	var result []string
	start, depth := 0, 0
	for index, character := range value {
		switch character {
		case '[':
			depth++
		case ']':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("invalid hostlist %q", value)
			}
		case ',', ' ', '\t':
			if depth == 0 {
				if group := strings.TrimSpace(value[start:index]); group != "" {
					result = append(result, group)
				}
				start = index + 1
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("invalid hostlist %q", value)
	}
	if group := strings.TrimSpace(value[start:]); group != "" {
		result = append(result, group)
	}
	return result, nil
}

func (c *Client) fetchJobs(ctx context.Context) ([]job, error) {
	var response jobsResponse
	if err := c.request(ctx, http.MethodGet, "/slurm/v0.0.45/jobs/", nil, &response); err != nil {
		return nil, err
	}
	if err := responseError(response.Errors); err != nil {
		return nil, err
	}
	return response.Jobs, nil
}

func requeuedState(states []string, reason string) bool {
	if slices.Contains(states, "PENDING") {
		return false
	}
	return slices.Contains(states, "REQUEUED") || slices.Contains(states, "REQUEUE_HOLD") || slices.Contains(states, "SPECIAL_EXIT") ||
		slices.Contains(states, "CANCELLED") && strings.EqualFold(reason, "job_requeued_in_held_state")
}

func (c *Client) AccountingReady(ctx context.Context, clusterName string) error {
	var response clustersResponse
	if err := c.request(ctx, http.MethodGet, "/slurmdb/v0.0.45/clusters/", nil, &response); err != nil {
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
	if err := c.request(ctx, http.MethodGet, "/slurm/v0.0.45/nodes/", nil, &response); err != nil {
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
	if err := c.request(ctx, http.MethodPost, "/slurm/v0.0.45/node/"+url.PathEscape(name), body, &response); err != nil {
		return err
	}
	return responseError(response.Errors)
}

func (c *Client) DeleteNode(ctx context.Context, name string) error {
	err := c.request(ctx, http.MethodDelete, "/slurm/v0.0.45/node/"+url.PathEscape(name), nil, nil)
	var status *httpStatusError
	if errors.As(err, &status) && status.Code == http.StatusNotFound {
		return nil
	}
	return err
}

func (c *Client) SignalJob(ctx context.Context, id uint32, signal string) error {
	if signal != "USR1" {
		return fmt.Errorf("unsupported Slurm signal %q", signal)
	}
	var response struct {
		Errors []apiError `json:"errors"`
	}
	path := fmt.Sprintf("/slurm/v0.0.45/job/%d?signal=%s", id, url.QueryEscape(signal))
	if err := c.request(ctx, http.MethodDelete, path, nil, &response); err != nil {
		return err
	}
	return responseError(response.Errors)
}

func (c *Client) RequeueJob(ctx context.Context, id uint32) error {
	var response struct {
		Errors []apiError `json:"errors"`
	}
	if err := c.request(ctx, http.MethodGet, fmt.Sprintf("/slurm/v0.0.45/job/%d/requeue", id), nil, &response); err != nil {
		return err
	}
	return responseError(response.Errors)
}

func (c *Client) request(ctx context.Context, method, path string, body, output any) error {
	token, err := c.token()
	if err != nil {
		return err
	}
	reference, err := url.Parse(path)
	if err != nil || reference.IsAbs() || reference.Host != "" || !strings.HasPrefix(reference.Path, "/") {
		return fmt.Errorf("invalid Slurm REST path")
	}
	target := c.baseURL.ResolveReference(reference)
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
		return &httpStatusError{Code: response.StatusCode}
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

type httpStatusError struct{ Code int }

func (e *httpStatusError) Error() string { return fmt.Sprintf("Slurm REST returned HTTP %d", e.Code) }

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
