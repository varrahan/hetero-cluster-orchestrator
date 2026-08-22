package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
)

const cloudBurstAnnotation = "orchestration.gputpu.io/cloud-burst-request"

type cloudBurstRequest struct {
	Namespace string `json:"namespace"`
	Cluster   string `json:"cluster"`
	Action    string `json:"action"`
	Nodes     string `json:"nodes"`
}

type cloudBurstServer struct {
	reader client.Reader
	client client.Client
}

func (*cloudBurstServer) NeedLeaderElection() bool { return false }

func (server *cloudBurstServer) Start(ctx context.Context) error {
	httpServer := &http.Server{
		Addr:              ":8082",
		Handler:           newCloudBurstHandler(server.reader, server.client),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdown)
	}()
	if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func newCloudBurstHandler(reader client.Reader, writer client.Client) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/cloud-burst" {
			http.NotFound(response, request)
			return
		}
		var notification cloudBurstRequest
		decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 64<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&notification); err != nil {
			http.Error(response, "invalid request", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF || notification.Namespace == "" || notification.Cluster == "" || notification.Nodes == "" || len(notification.Nodes) > 1<<16 || (notification.Action != "resume" && notification.Action != "suspend") {
			http.Error(response, "invalid request", http.StatusBadRequest)
			return
		}

		key := types.NamespacedName{Namespace: notification.Namespace, Name: notification.Cluster}
		var cluster orchestrationv1alpha1.HeterogeneousCluster
		if err := reader.Get(request.Context(), key, &cluster); err != nil || cluster.Spec.ControlPlane.Controllers.CloudBurstTokenSecretRef == "" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		var secret corev1.Secret
		secretKey := types.NamespacedName{Namespace: notification.Namespace, Name: cluster.Spec.ControlPlane.Controllers.CloudBurstTokenSecretRef}
		if err := reader.Get(request.Context(), secretKey, &secret); err != nil {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		provided, ok := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
		expected := []byte(strings.TrimSpace(string(secret.Data["token"])))
		if !ok || len(expected) < 32 || subtle.ConstantTimeCompare([]byte(provided), expected) != 1 {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}

		before := cluster.DeepCopy()
		if cluster.Annotations == nil {
			cluster.Annotations = map[string]string{}
		}
		cluster.Annotations[cloudBurstAnnotation] = notification.Action + ":" + notification.Nodes
		if err := writer.Patch(request.Context(), &cluster, client.MergeFrom(before)); err != nil {
			http.Error(response, "update failed", http.StatusInternalServerError)
			return
		}
		response.WriteHeader(http.StatusAccepted)
	})
}
