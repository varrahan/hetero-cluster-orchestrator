package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCriticalKernelEvents(t *testing.T) {
	for _, line := range []string{"NVRM: Xid 79", "MCE: CPU0", "EDAC uncorrectable error"} {
		if criticalKernelEvent(line) == "" {
			t.Fatalf("critical event %q was ignored", line)
		}
	}
	if criticalKernelEvent("correctable ECC event") != "" {
		t.Fatal("correctable event was treated as a hard fault")
	}
}

func TestNVIDIACommandOverride(t *testing.T) {
	t.Setenv("NVIDIA_SMI", "/custom/nvidia-smi")
	command := nvidiaSMI("--query-gpu=uuid")
	if len(command.Args) != 2 || command.Args[0] != "/custom/nvidia-smi" || command.Args[1] != "--query-gpu=uuid" {
		t.Fatalf("NVIDIA command = %q", command.Args)
	}
}

func TestRebootAcknowledgedOnce(t *testing.T) {
	t.Setenv("WATCHDOG_REBOOT_DRY_RUN", "true")
	incident := "11111111-2222-4333-8444-555555555555"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Annotations: map[string]string{
		recoveryIncident: incident, recoveryPhase: "RebootRequested", rebootRequest: incident,
	}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeConditionType(hardwareDegraded), Status: corev1.ConditionTrue}}}}
	client := fake.NewClientset(node)
	if err := rebootOnce(context.Background(), client, node.Name); err != nil {
		t.Fatal(err)
	}
	if err := rebootOnce(context.Background(), client, node.Name); err != nil {
		t.Fatal(err)
	}
	updated, err := client.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil || updated.Annotations[rebootAck] != incident {
		t.Fatalf("reboot ack=%q err=%v", updated.Annotations[rebootAck], err)
	}
}

func TestRebootWaitsForWorkers(t *testing.T) {
	t.Setenv("WATCHDOG_REBOOT_DRY_RUN", "true")
	incident := "11111111-2222-4333-8444-555555555555"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Annotations: map[string]string{
		recoveryIncident: incident, recoveryPhase: "RebootRequested", rebootRequest: incident,
	}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeConditionType(hardwareDegraded), Status: corev1.ConditionTrue}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "slurmd-a", Namespace: "slurm-system", Labels: map[string]string{"app.kubernetes.io/component": "slurmd", "app.kubernetes.io/managed-by": "slurm-operator"}}, Spec: corev1.PodSpec{NodeName: node.Name}}
	client := fake.NewClientset(node, pod)
	if err := rebootOnce(context.Background(), client, node.Name); err != nil {
		t.Fatal(err)
	}
	updated, _ := client.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if updated.Annotations[rebootAck] != "" {
		t.Fatal("watchdog acknowledged reboot while a worker remained")
	}
}
