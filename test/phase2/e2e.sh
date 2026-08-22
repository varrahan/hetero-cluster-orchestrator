#!/usr/bin/env bash
set -euo pipefail

cluster_name=${KIND_CLUSTER:-phase2}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
keep=${KEEP_KIND:-0}

cleanup() {
  if [[ $keep != 1 ]]; then
    kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
trap 'echo "Phase 2 live gate failed near line $LINENO" >&2' ERR

for command in docker kind kubectl python3; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done

retry() {
  local deadline=$((SECONDS + $1))
  shift
  until "$@"; do
    (( SECONDS < deadline )) || return 1
    sleep 2
  done
}

KIND_CLUSTER="$cluster_name" ENABLE_NRI=1 KEEP_KIND=1 "$root/test/phase1/e2e.sh"

state_saved() {
  kubectl -n slurm-system exec research-slurmctld-1 -c slurmctld -- test -s /var/lib/slurmctld/job_state
}
retry 120 state_saved
primary_node=$(kubectl -n slurm-system get pod/research-slurmctld-0 -o jsonpath='{.spec.nodeName}')
primary_container=$(kubectl -n slurm-system get pod/research-slurmctld-0 -o jsonpath='{.status.containerStatuses[?(@.name=="slurmctld")].containerID}')
docker exec "$primary_node" ctr -n k8s.io tasks resume "${primary_container#containerd://}" >/dev/null 2>&1 || true
primary_up() {
  kubectl -n slurm-system exec research-slurmctld-0 -c slurmctld -- scontrol ping 2>/dev/null | grep -q 'Slurmctld(primary).* is UP'
}
retry 120 primary_up
kubectl -n slurm-system delete pod research-slurmctld-1 --wait=true
kubectl -n slurm-system wait --for=condition=Ready pod/research-slurmctld-1 --timeout=3m
retry 120 primary_up
login=(kubectl -n slurm-system exec deployment/research-login -c login --)
cancel_phase1_jobs() {
  "${login[@]}" squeue --noheader >/dev/null && "${login[@]}" scancel --user=root
}
retry 120 cancel_phase1_jobs
phase1_demand_gone() {
  ! "${login[@]}" squeue --noheader | grep -q . &&
    ! kubectl -n slurm-system get pods -l app.kubernetes.io/component=slurmd -o name | grep -q . &&
    ! kubectl -n slurm-system get resourceclaims -o name | grep -q .
}
retry 120 phase1_demand_gone

docker build -t orchestration-dra:dev -f "$root/src/dra-driver/Dockerfile" "$root"
docker build -t slurm-worker:dev -f "$root/src/slurm-compute-node/Dockerfile" "$root"
kind load docker-image --name "$cluster_name" orchestration-dra:dev slurm-worker:dev

compute_node=${cluster_name}-worker
kubectl annotate node "$compute_node" --overwrite \
  orchestration.gputpu.io/memory-unit=1Gi \
  orchestration.gputpu.io/reserved-cores-per-numa=1 \
  orchestration.gputpu.io/reserved-memory-per-numa=1Gi \
  'orchestration.gputpu.io/opentpu-profiles={"version":1,"profiles":[{"name":"m8","matrixSize":8,"numaNode":0,"slots":2,"cpuCores":2,"memory":"1Gi","sharedMemory":"512Mi"}]}'
kubectl label node "$compute_node" --overwrite orchestration.gputpu.io/compute=true
kubectl -n slurm-system wait --for=jsonpath='{.status.numberReady}'=1 daemonset/orchestration-dra --timeout=5m

slice_ready() {
  kubectl get resourceslices -o json | python3 -c '
import json, sys
slices=json.load(sys.stdin).get("items", [])
assert any(s.get("spec", {}).get("driver") == "orchestration.gputpu.io" and s.get("spec", {}).get("nodeName") == sys.argv[1] for s in slices)
' "$compute_node"
}
retry 120 slice_ready

if [[ ${ENABLE_GPU:-0} == 1 ]]; then
  gpu_slice_ready() {
    kubectl get resourceslices -o json | python3 -c '
import json, sys
for resource_slice in json.load(sys.stdin).get("items", []):
    for device in resource_slice.get("spec", {}).get("devices", []):
        kind = device.get("attributes", {}).get("orchestration.gputpu.io/kind", {})
        if kind.get("string") == "gpu":
            raise SystemExit(0)
raise SystemExit(1)
'
  }
  retry 120 gpu_slice_ready
fi

kubectl -n slurm-system patch heterogeneouscluster research --type=merge -p '{"spec":{"workerPools":[{"name":"compute","partition":"compute","nodeSelector":{"orchestration.gputpu.io/compute":"true"},"memoryUnit":"1Gi","scaling":{"minReady":0,"maxWorkers":4,"idleTimeout":"15s"},"profiles":[{"name":"tpu-m8","gres":"tpu:opentpu_m8","deviceClassName":"opentpu.orchestration.gputpu.io"}]}]}}'
if [[ ${ENABLE_GPU:-0} == 1 ]]; then
  kubectl -n slurm-system patch heterogeneouscluster research --type=json -p='[{"op":"add","path":"/spec/workerPools/0/profiles/-","value":{"name":"gpu-rtx4050","gres":"gpu:rtx_4050","deviceClassName":"nvidia.orchestration.gputpu.io"}}]'
fi
kubectl -n slurm-system rollout restart deployment/slurm-operator
kubectl -n slurm-system rollout status deployment/slurm-operator --timeout=3m
kubectl -n slurm-system rollout status statefulset/research-slurmctld --timeout=3m
kubectl -n slurm-system rollout status deployment/research-login --timeout=3m
cluster_reconciled() {
  kubectl -n slurm-system get heterogeneouscluster research -o json | python3 -c '
import json, sys
cluster=json.load(sys.stdin)
conditions={condition["type"]: condition for condition in cluster.get("status", {}).get("conditions", [])}
assert cluster["metadata"]["generation"] == cluster.get("status", {}).get("observedGeneration")
assert conditions.get("ControlPlaneReady", {}).get("status") == "True"
'
}
retry 180 cluster_reconciled

wait_job() {
  local job=$1
  local state
  state=$("${login[@]}" sacct --noheader --allocations --jobs "$job" --format=State 2>/dev/null | awk 'NF {print $1; exit}')
  [[ $state == COMPLETED ]]
}
job_running() { "${login[@]}" squeue --noheader --jobs "$1" --states=RUNNING | grep -q "$1"; }

live_tpu_allocation() {
  local node claim
  node=$("${login[@]}" scontrol show nodes --oneliner | awk '/Gres=tpu:opentpu_m8:1/ {sub("NodeName=", "", $1); print $1; exit}')
  [[ -n $node ]] || return 1
  claim=$(kubectl -n slurm-system get pod "$node" -o jsonpath='{.metadata.annotations.orchestration\.gputpu\.io/resource-claim}')
  [[ -n $claim ]] || return 1
  kubectl -n slurm-system get resourceclaim "$claim" -o json | python3 -c '
import json, sys
claim=json.load(sys.stdin)
status=claim.get("status", {})
assert any(result.get("request", "").split("/")[0] == "accelerator-tpu-m8" for result in status.get("allocation", {}).get("devices", {}).get("results", []))
assert any(owner.get("name") == sys.argv[1] for owner in status.get("reservedFor", []))
' "$node"
}

cpu_job=$("${login[@]}" sbatch --parsable --partition=compute --nodes=1 --cpus-per-task=2 --mem=1G --wrap='test -n "$SLURM_JOB_NODELIST"')
cpu_job=${cpu_job%%;*}
retry 300 wait_job "$cpu_job"

tpu_job=$("${login[@]}" sbatch --parsable --partition=compute --nodes=1 --cpus-per-task=2 --mem=1G --gres=tpu:opentpu_m8:1 --wrap='python3 /usr/local/bin/opentpu-runtime.py && sleep 20')
tpu_job=${tpu_job%%;*}
retry 300 job_running "$tpu_job"
retry 60 live_tpu_allocation
retry 600 wait_job "$tpu_job"

mixed_job=$("${login[@]}" sbatch --parsable --partition=compute --nodes=1 --cpus-per-task=1 : --partition=compute --nodes=1 --cpus-per-task=2 --gres=tpu:opentpu_m8:1 --wrap='test -n "$SLURM_JOB_NODELIST"')
mixed_job=${mixed_job%%;*}
retry 600 wait_job "$mixed_job"

if [[ ${ENABLE_GPU:-0} == 1 ]]; then
  "$root/test/phase2/gpu-e2e.sh"
fi

no_fit_job=$("${login[@]}" sbatch --parsable --partition=compute --nodes=1 --cpus-per-task=20 --mem=1G --wrap=true)
no_fit_job=${no_fit_job%%;*}
no_fit_stays_pending() {
  "${login[@]}" squeue --noheader --jobs "$no_fit_job" --states=PENDING | grep -q "$no_fit_job"
}
retry 60 no_fit_stays_pending
"${login[@]}" scancel "$no_fit_job"

restart_job=$("${login[@]}" sbatch --parsable --partition=compute --nodes=1 --cpus-per-task=1 --mem=1G --wrap='sleep 90')
restart_job=${restart_job%%;*}
retry 180 job_running "$restart_job"
worker_count=$(kubectl -n slurm-system get pods -l app.kubernetes.io/component=slurmd --no-headers | wc -l)
kubectl -n slurm-system rollout restart deployment/slurm-operator
kubectl -n slurm-system rollout status deployment/slurm-operator --timeout=3m
retry 60 job_running "$restart_job"
test "$worker_count" = "$(kubectl -n slurm-system get pods -l app.kubernetes.io/component=slurmd --no-headers | wc -l)"

kubectl -n slurm-system scale deployment/research-slurmrestd --replicas=0
sleep 20
test "$worker_count" = "$(kubectl -n slurm-system get pods -l app.kubernetes.io/component=slurmd --no-headers | wc -l)"
kubectl -n slurm-system scale deployment/research-slurmrestd --replicas=2
kubectl -n slurm-system rollout status deployment/research-slurmrestd --timeout=3m
"${login[@]}" scancel "$restart_job" || true

workers_gone() {
  [[ $(kubectl -n slurm-system get pods -l app.kubernetes.io/component=slurmd --no-headers 2>/dev/null | wc -l) == 0 ]] &&
    [[ $(kubectl -n slurm-system get resourceclaims --no-headers 2>/dev/null | wc -l) == 0 ]] &&
    ! "${login[@]}" scontrol show nodes --noheader 2>/dev/null | grep -q '^NodeName='
}
retry 180 workers_gone

kubectl apply -f - <<'EOF'
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: claim-gres-mismatch-resources
  namespace: slurm-system
spec:
  devices:
    requests:
    - name: cpu
      exactly:
        deviceClassName: cpu.orchestration.gputpu.io
        allocationMode: ExactCount
        count: 1
        selectors:
        - cel:
            expression: 'device.driver == "orchestration.gputpu.io" && device.attributes["orchestration.gputpu.io"].kind == "cpu"'
    - name: memory
      exactly:
        deviceClassName: memory.orchestration.gputpu.io
        allocationMode: ExactCount
        count: 1
        selectors:
        - cel:
            expression: 'device.driver == "orchestration.gputpu.io" && device.attributes["orchestration.gputpu.io"].kind == "memory" && device.attributes["orchestration.gputpu.io"]["unitBytes"] == 1073741824'
    constraints:
    - matchAttribute: orchestration.gputpu.io/numaNode
---
apiVersion: v1
kind: Pod
metadata:
  name: claim-gres-mismatch
  namespace: slurm-system
  annotations:
    orchestration.gputpu.io/resource-claim: claim-gres-mismatch-resources
    orchestration.gputpu.io/worker-shape: '{"CPUs":1,"MemoryBytes":1073741824,"SharedMemoryBytes":0,"GRES":{"tpu:opentpu_m8":1}}'
spec:
  serviceAccountName: slurm-worker
  restartPolicy: Never
  nodeSelector:
    orchestration.gputpu.io/compute: "true"
  resourceClaims:
  - name: allocation
    resourceClaimName: claim-gres-mismatch-resources
  initContainers:
  - name: gres-init
    image: slurm-worker:dev
    imagePullPolicy: IfNotPresent
    command: [/usr/local/bin/gres-init]
    env:
    - name: POD_NAME
      valueFrom: {fieldRef: {fieldPath: metadata.name}}
    - name: POD_NAMESPACE
      valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
    - name: POD_IP
      valueFrom: {fieldRef: {fieldPath: status.podIP}}
    - name: RESOURCE_CLAIM
      value: claim-gres-mismatch-resources
    - name: WORKER_POOL
      value: compute
    - name: SLURM_CONF_SERVER
      value: research-slurmctld-0.research-slurmctld.slurm-system.svc:6817,research-slurmctld-1.research-slurmctld.slurm-system.svc:6817
    resources:
      requests: {cpu: "1", memory: 1Gi}
      limits: {cpu: "1", memory: 1Gi}
      claims: [{name: allocation}]
  containers:
  - name: never-starts
    image: slurm-worker:dev
    imagePullPolicy: IfNotPresent
    command: [sleep, "3600"]
    resources:
      requests: {cpu: "1", memory: 1Gi}
      limits: {cpu: "1", memory: 1Gi}
      claims: [{name: allocation}]
EOF

mismatch_rejected() {
  local exit_code
  exit_code=$(kubectl -n slurm-system get pod claim-gres-mismatch -o jsonpath='{.status.initContainerStatuses[?(@.name=="gres-init")].state.terminated.exitCode}')
  [[ -n $exit_code && $exit_code != 0 ]]
}
retry 180 mismatch_rejected
kubectl -n slurm-system logs claim-gres-mismatch -c gres-init | grep -q 'allocated GRES.*expected'
! "${login[@]}" scontrol show node claim-gres-mismatch >/dev/null 2>&1
kubectl -n slurm-system delete pod claim-gres-mismatch --wait=true
kubectl -n slurm-system delete resourceclaim claim-gres-mismatch-resources --ignore-not-found

echo "Phase 2 CPU, NUMA, OpenTPU, mixed-job, allocation-invariant, mismatch, restart, outage, and ordered-idle gates passed"
