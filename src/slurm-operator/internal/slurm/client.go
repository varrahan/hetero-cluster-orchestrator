package slurm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const maxResponseBytes = 8 << 20

var usernamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)

type Client struct {
	baseURL    *url.URL
	username   string
	key        []byte
	httpClient *http.Client
}

type PendingJob struct {
	ID        uint32
	Partition string
	Reason    string
}

type job struct {
	ID        uint32   `json:"job_id"`
	Partition string   `json:"partition"`
	State     jobState `json:"state"`
	Reason    string   `json:"state_reason"`
}

type jobState struct {
	Current []string `json:"current"`
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

func NewClient(baseURL, username string, key []byte, httpClient *http.Client) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
		return nil, fmt.Errorf("invalid Slurm REST URL")
	}
	if !usernamePattern.MatchString(username) {
		return nil, fmt.Errorf("invalid Slurm username")
	}
	if len(key) < 32 {
		return nil, fmt.Errorf("Slurm JWT key must contain at least 32 bytes")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &Client{
		baseURL:    parsed,
		username:   username,
		key:        slices.Clone(key),
		httpClient: httpClient,
	}, nil
}

func (c *Client) PendingJobs(ctx context.Context) ([]PendingJob, error) {
	var response jobsResponse
	if err := c.get(ctx, "/slurm/v0.0.44/jobs/", &response); err != nil {
		return nil, err
	}
	if err := responseError(response.Errors); err != nil {
		return nil, err
	}

	pending := make([]PendingJob, 0, len(response.Jobs))
	for _, job := range response.Jobs {
		if slices.Contains(job.State.Current, "PENDING") {
			pending = append(pending, PendingJob{
				ID:        job.ID,
				Partition: job.Partition,
				Reason:    job.Reason,
			})
		}
	}
	return pending, nil
}

func (c *Client) AccountingReady(ctx context.Context, clusterName string) error {
	var response clustersResponse
	if err := c.get(ctx, "/slurmdb/v0.0.44/clusters/", &response); err != nil {
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

func (c *Client) get(ctx context.Context, path string, output any) error {
	token, err := c.token()
	if err != nil {
		return err
	}
	target := c.baseURL.ResolveReference(&url.URL{Path: path})
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return fmt.Errorf("create Slurm REST request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-SLURM-USER-NAME", c.username)
	request.Header.Set("X-SLURM-USER-TOKEN", token)

	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("call Slurm REST: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Slurm REST returned HTTP %d", response.StatusCode)
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
		"iat": now, "exp": now + 60, "sun": c.username,
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
