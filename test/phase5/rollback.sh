#!/usr/bin/env bash
set -euo pipefail

cluster_name=$1
prior_image=$2
namespace=slurm-system

kind load docker-image --name "$cluster_name" "$prior_image"
pvc_uid=$(kubectl -n "$namespace" get pvc/slurm-state -o jsonpath='{.metadata.uid}')
statefulset_uid=$(kubectl -n "$namespace" get statefulset/research-slurmctld -o jsonpath='{.metadata.uid}')
claims=$(kubectl -n "$namespace" get resourceclaims -o json | python3 -c 'import json,sys; print(" ".join(sorted("{}:{}".format(x["metadata"]["name"], x["metadata"]["uid"]) for x in json.load(sys.stdin)["items"])))')
checkpoints=$(kubectl -n "$namespace" exec phase3-mc -- mc ls --insecure --recursive local/checkpoints/checkpoints | sort)
quarantined=$(kubectl get nodes -o json | python3 -c 'import json,sys; print(next("{}:{}".format(x["metadata"]["name"], x["metadata"].get("annotations",{}).get("orchestration.gputpu.io/recovery-incident","")) for x in json.load(sys.stdin)["items"] if x["metadata"].get("annotations",{}).get("orchestration.gputpu.io/recovery-phase") == "ManualRepair"))')
jobs=$(kubectl -n "$namespace" exec deployment/research-login -c login -- sacct --noheader --allocations --starttime=now-1day --format=JobIDRaw | awk 'NF {print $1}' | sort -n | tr '\n' ' ')

kubectl -n "$namespace" set image deployment/slurm-operator operator="$prior_image"
kubectl -n "$namespace" rollout status deployment/slurm-operator --timeout=5m
kubectl -n "$namespace" wait --for=condition=ControlPlaneReady heterogeneouscluster/research --timeout=5m

test "$pvc_uid" = "$(kubectl -n "$namespace" get pvc/slurm-state -o jsonpath='{.metadata.uid}')"
test "$statefulset_uid" = "$(kubectl -n "$namespace" get statefulset/research-slurmctld -o jsonpath='{.metadata.uid}')"
test "$claims" = "$(kubectl -n "$namespace" get resourceclaims -o json | python3 -c 'import json,sys; print(" ".join(sorted("{}:{}".format(x["metadata"]["name"], x["metadata"]["uid"]) for x in json.load(sys.stdin)["items"])))')"
test "$checkpoints" = "$(kubectl -n "$namespace" exec phase3-mc -- mc ls --insecure --recursive local/checkpoints/checkpoints | sort)"
test "$quarantined" = "$(kubectl get nodes -o json | python3 -c 'import json,sys; print(next("{}:{}".format(x["metadata"]["name"], x["metadata"].get("annotations",{}).get("orchestration.gputpu.io/recovery-incident","")) for x in json.load(sys.stdin)["items"] if x["metadata"].get("annotations",{}).get("orchestration.gputpu.io/recovery-phase") == "ManualRepair"))')"
test "$jobs" = "$(kubectl -n "$namespace" exec deployment/research-login -c login -- sacct --noheader --allocations --starttime=now-1day --format=JobIDRaw | awk 'NF {print $1}' | sort -n | tr '\n' ' ')"

kubectl -n "$namespace" set image deployment/slurm-operator operator=slurm-operator:dev
kubectl -n "$namespace" rollout status deployment/slurm-operator --timeout=5m
echo "Prior-operator rollback preserved Slurm state, claims, checkpoints, and quarantine"
