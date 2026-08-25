#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
prior_image=${PRIOR_OPERATOR_IMAGE:?PRIOR_OPERATOR_IMAGE must name the previously released operator image}
passes=${PHASE5_PASSES:-2}
[[ $passes == 1 || $passes == 2 ]]
real_docker=$(command -v docker)
gpu_uuid=$(nvidia-smi --query-gpu=uuid --format=csv,noheader | head -n1)
clusters=()

cleanup() {
  for cluster in "${clusters[@]}"; do
    kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

for ((pass = 1; pass <= passes; pass++)); do
  cluster="phase5-$pass"
  clusters+=("$cluster")
  PATH="$root/test/phase2/gpu-bin:$PATH" ENABLE_GPU=1 EXPECTED_GPU_UUID="$gpu_uuid" GPU_REAL_DOCKER="$real_docker" \
    KIND_CLUSTER="$cluster" KEEP_KIND=1 "$root/test/phase4/e2e.sh"
  sed '0,/name: research/s//name: phase5-example/' "$root/src/manifests/workloads/example-cluster.yaml" | kubectl apply --dry-run=server -f - >/dev/null
  kubectl create namespace monitoring
  kubectl -n monitoring run metrics-probe --image=slurm-worker:dev --restart=Never --command -- \
    python3 -c 'import urllib.request; assert b"gputpu_cluster_condition" in urllib.request.urlopen("http://slurm-operator-metrics.slurm-system.svc:8080/metrics", timeout=10).read()'
  kubectl -n monitoring wait --for=jsonpath='{.status.phase}'=Succeeded pod/metrics-probe --timeout=2m
  if [[ $pass == 1 ]]; then
    "$root/test/phase5/rollback.sh" "$cluster" "$prior_image"
  fi
  kind delete cluster --name "$cluster"
  clusters=()
done

echo "Phase 5 physical matrix passed $passes consecutive time(s)"
