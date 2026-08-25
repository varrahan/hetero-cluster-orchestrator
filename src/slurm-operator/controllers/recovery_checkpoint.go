package controllers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	checkpointapi "github.com/varrahan/hetero-cluster-orchestrater/src/shared/checkpoint"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
)

func (r *RecoveryReconciler) checkpointCommittedSince(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, jobID uint64, since time.Time) (bool, error) {
	store, bucket, err := r.checkpointStore(ctx, cluster)
	if err != nil {
		return false, err
	}
	count := 0
	for object := range store.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: "checkpoints/", Recursive: true}) {
		if object.Err != nil {
			return false, object.Err
		}
		if !strings.HasSuffix(object.Key, ".complete") {
			continue
		}
		count++
		if count > 10000 {
			return false, errors.New("checkpoint commit listing exceeds 10000 entries")
		}
		if !object.LastModified.IsZero() && object.LastModified.Before(since.Add(-time.Second)) {
			continue
		}
		reader, err := store.GetObject(ctx, bucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			return false, err
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, (64<<10)+1))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || len(data) > 64<<10 {
			continue
		}
		var marker checkpointapi.CommitMarker
		if checkpointapi.DecodeStrictJSON(strings.NewReader(string(data)), &marker) != nil || marker.CheckpointVersion != checkpointapi.Version || marker.SlurmJobID != jobID || len(marker.ManifestSHA256) != 64 {
			continue
		}
		committed, err := time.Parse(time.RFC3339Nano, marker.CommittedAt)
		if err == nil && !committed.Before(since) {
			return true, nil
		}
	}
	return false, nil
}

func (r *ClusterReconciler) checkpointStore(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster) (*minio.Client, string, error) {
	if cluster.Spec.Checkpointing == nil {
		return nil, "", errors.New("checkpointing is disabled")
	}
	var secret corev1.Secret
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Spec.Checkpointing.ObjectStoreSecretRef}
	if err := r.Reader.Get(ctx, key, &secret); err != nil {
		return nil, "", err
	}
	value := func(name string) (string, error) {
		result := strings.TrimSpace(string(secret.Data[name]))
		if result == "" || len(result) > 4096 {
			return "", fmt.Errorf("checkpoint Secret key %q is empty or too long", name)
		}
		return result, nil
	}
	endpoint, err := value("endpoint")
	if err != nil {
		return nil, "", err
	}
	bucket, err := value("bucket")
	if err != nil {
		return nil, "", err
	}
	accessKey, err := value("accessKey")
	if err != nil {
		return nil, "", err
	}
	secretKey, err := value("secretKey")
	if err != nil {
		return nil, "", err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || (parsed.Path != "" && parsed.Path != "/") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, "", errors.New("checkpoint endpoint must be an HTTPS origin")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if ca := secret.Data["ca.crt"]; len(ca) != 0 {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, "", err
		}
		if !pool.AppendCertsFromPEM(ca) {
			return nil, "", errors.New("checkpoint ca.crt contains no certificates")
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	store, err := minio.New(parsed.Host, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: true, Transport: transport, MaxRetries: 3})
	return store, bucket, err
}

func (r *ClusterReconciler) newestCommittedCheckpoint(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster) (*metav1.Time, error) {
	store, bucket, err := r.checkpointStore(ctx, cluster)
	if err != nil {
		return nil, err
	}
	objects := make([]minio.ObjectInfo, 0)
	for object := range store.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: "checkpoints/", Recursive: true, WithMetadata: true}) {
		if object.Err != nil {
			return nil, object.Err
		}
		if strings.HasSuffix(object.Key, ".complete") {
			objects = append(objects, object)
			if len(objects) > 10000 {
				return nil, errors.New("checkpoint commit listing exceeds 10000 entries")
			}
		}
	}
	return newestCheckpointTime(objects, string(cluster.UID)), nil
}

func newestCheckpointTime(objects []minio.ObjectInfo, clusterUID string) *metav1.Time {
	var newest time.Time
	for _, object := range objects {
		if object.UserMetadata["cluster-uid"] == clusterUID && object.LastModified.After(newest) {
			newest = object.LastModified
		}
	}
	if newest.IsZero() {
		return nil
	}
	result := metav1.NewTime(newest.UTC())
	return &result
}
