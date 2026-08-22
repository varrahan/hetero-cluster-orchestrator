package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
)

func TestCloudBurstHandler(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := orchestrationv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &orchestrationv1alpha1.HeterogeneousCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "research"},
		Spec:       orchestrationv1alpha1.HeterogeneousClusterSpec{ControlPlane: orchestrationv1alpha1.ControlPlaneSpec{Controllers: orchestrationv1alpha1.ControllersSpec{CloudBurstTokenSecretRef: "cloud-burst"}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cloud-burst"}, Data: map[string][]byte{"token": []byte("0123456789abcdef0123456789abcdef\n")}}
	kubernetes := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, secret).Build()
	handler := newCloudBurstHandler(kubernetes, kubernetes)

	for range 2 {
		request := httptest.NewRequest(http.MethodPost, "/v1/cloud-burst", bytes.NewBufferString(`{"namespace":"tenant","cluster":"research","action":"resume","nodes":"future[1-2]"}`))
		request.Header.Set("Authorization", "Bearer 0123456789abcdef0123456789abcdef")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
		}
	}
	var updated orchestrationv1alpha1.HeterogeneousCluster
	if err := kubernetes.Get(context.Background(), types.NamespacedName{Namespace: "tenant", Name: "research"}, &updated); err != nil {
		t.Fatal(err)
	}
	if got := updated.Annotations[cloudBurstAnnotation]; got != "resume:future[1-2]" {
		t.Fatalf("annotation = %q", got)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/cloud-burst", bytes.NewBufferString(`{"namespace":"tenant","cluster":"research","action":"suspend","nodes":"future1"}`))
	request.Header.Set("Authorization", "Bearer wrong")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("bad token status = %d", response.Code)
	}
}
