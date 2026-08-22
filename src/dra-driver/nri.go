package main

import (
	"context"
	"fmt"

	api "github.com/containerd/nri/pkg/api"
)

const claimAnnotation = "orchestration.gputpu.io/resource-claim"

type nriDriver struct {
	state *nodePlugin
}

func (n *nriDriver) CreateContainer(_ context.Context, pod *api.PodSandbox, _ *api.Container) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	claim := pod.Annotations[claimAnnotation]
	if claim == "" {
		return nil, nil, nil
	}
	record, ok := n.state.preparedFor(pod.Namespace, claim)
	if !ok {
		return nil, nil, fmt.Errorf("prepared claim %s/%s is missing", pod.Namespace, claim)
	}
	adjustment := &api.ContainerAdjustment{}
	adjustment.SetLinuxCPUSetCPUs(record.cpuSet())
	adjustment.SetLinuxCPUSetMems(fmt.Sprint(record.NUMA))
	return adjustment, nil, nil
}

func (n *nriDriver) Synchronize(_ context.Context, pods []*api.PodSandbox, containers []*api.Container) ([]*api.ContainerUpdate, error) {
	byID := make(map[string]*api.PodSandbox, len(pods))
	for _, pod := range pods {
		byID[pod.Id] = pod
	}
	updates := make([]*api.ContainerUpdate, 0)
	for _, container := range containers {
		pod := byID[container.PodSandboxId]
		if pod == nil || pod.Annotations[claimAnnotation] == "" {
			continue
		}
		record, ok := n.state.preparedFor(pod.Namespace, pod.Annotations[claimAnnotation])
		if !ok {
			return nil, fmt.Errorf("prepared claim %s/%s is missing during synchronization", pod.Namespace, pod.Annotations[claimAnnotation])
		}
		update := &api.ContainerUpdate{ContainerId: container.Id}
		update.SetLinuxCPUSetCPUs(record.cpuSet())
		update.SetLinuxCPUSetMems(fmt.Sprint(record.NUMA))
		updates = append(updates, update)
	}
	return updates, nil
}
