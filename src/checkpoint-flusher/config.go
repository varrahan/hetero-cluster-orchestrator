package main

import (
	"cmp"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
)

type config struct {
	clusterUID string
	budget     uint64
	bucket     string
	client     *minio.Client
}

func loadConfig() (config, error) {
	result := config{clusterUID: os.Getenv("CHECKPOINT_CLUSTER_UID")}
	if result.clusterUID == "" {
		return result, fmt.Errorf("CHECKPOINT_CLUSTER_UID is required")
	}
	budget, err := strconv.ParseUint(cmp.Or(os.Getenv("CHECKPOINT_SHM_BUDGET_BYTES"), "67108864"), 10, 64)
	if err != nil || budget < 16<<20 || budget > 16<<30 {
		return result, fmt.Errorf("CHECKPOINT_SHM_BUDGET_BYTES must be between 16MiB and 16GiB")
	}
	result.budget = budget
	secretDir := "/run/secrets/checkpoint-store"
	endpoint, err := secretValue(secretDir, "endpoint")
	if err != nil {
		return result, err
	}
	result.bucket, err = secretValue(secretDir, "bucket")
	if err != nil {
		return result, err
	}
	if err := s3utils.CheckValidBucketNameStrict(result.bucket); err != nil {
		return result, fmt.Errorf("invalid checkpoint bucket: %w", err)
	}
	accessKey, err := secretValue(secretDir, "accessKey")
	if err != nil {
		return result, err
	}
	secretKey, err := secretValue(secretDir, "secretKey")
	if err != nil {
		return result, err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || (parsed.Path != "" && parsed.Path != "/") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return result, fmt.Errorf("checkpoint endpoint must be an HTTPS origin without path, credentials, query, or fragment")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if ca, readErr := os.ReadFile(filepath.Join(secretDir, "ca.crt")); readErr == nil {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return result, fmt.Errorf("load system CA pool: %w", err)
		}
		if !pool.AppendCertsFromPEM(ca) {
			return result, fmt.Errorf("ca.crt contains no certificates")
		}
		transport.TLSClientConfig.RootCAs = pool
	} else if !os.IsNotExist(readErr) {
		return result, fmt.Errorf("read ca.crt: %w", readErr)
	}
	result.client, err = minio.New(parsed.Host, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: true, Transport: transport, MaxRetries: 3})
	if err != nil {
		return result, fmt.Errorf("create MinIO client: %w", err)
	}
	return result, nil
}

func secretValue(directory, name string) (string, error) {
	data, err := os.ReadFile(filepath.Join(directory, name))
	if err != nil {
		return "", fmt.Errorf("read checkpoint Secret key %q: %w", name, err)
	}
	value := strings.TrimSpace(string(data))
	if value == "" || len(value) > 4096 {
		return "", fmt.Errorf("checkpoint Secret key %q is empty or too long", name)
	}
	return value, nil
}
