#!/usr/bin/env bash
set -euo pipefail

cluster_name=${KIND_CLUSTER:-phase4}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$root/test/lib.sh"
keep=${KEEP_KIND:-0}

cleanup() {
  if [[ $keep != 1 ]]; then
    kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
  fi
}
diagnose() {
  echo "Phase 4 live gate failed near line $1" >&2
  kubectl get nodes -o wide >&2 || true
  kubectl -n slurm-system get pods,jobs,resourceclaims -o wide >&2 || true
  kubectl -n slurm-system get events --sort-by=.lastTimestamp | tail -60 >&2 || true
  kubectl -n slurm-system logs deployment/slurm-operator --tail=120 >&2 || true
}
trap cleanup EXIT
trap 'diagnose "$LINENO"' ERR

require_commands docker kind kubectl python3

KIND_CLUSTER="$cluster_name" KEEP_KIND=1 "$root/test/phase3/e2e.sh"

for compute_node in "${cluster_name}-worker" "${cluster_name}-worker2"; do
  kubectl annotate node "$compute_node" --overwrite \
    orchestration.gputpu.io/memory-unit=1Gi \
    orchestration.gputpu.io/reserved-cores-per-numa=1 \
    orchestration.gputpu.io/reserved-memory-per-numa=1Gi \
    'orchestration.gputpu.io/opentpu-profiles={"version":1,"profiles":[{"name":"m8","matrixSize":8,"numaNode":0,"slots":2,"cpuCores":1,"memory":"1Gi","sharedMemory":"512Mi"}]}'
  kubectl label node "$compute_node" --overwrite orchestration.gputpu.io/compute=true
done
kubectl -n slurm-system wait --for=jsonpath='{.status.numberReady}'=2 daemonset/orchestration-dra --timeout=5m
kubectl -n slurm-system set env daemonset/watchdog-daemon WATCHDOG_REBOOT_DRY_RUN=true
kubectl -n slurm-system rollout status daemonset/watchdog-daemon --timeout=5m
kubectl -n slurm-system set env deployment/slurm-operator \
  RECOVERY_BOOT_ID_ANNOTATION=orchestration.gputpu.io/test-boot-id \
  RECOVERY_REBOOT_TIMEOUT=5m
kubectl -n slurm-system rollout status deployment/slurm-operator --timeout=3m

node_condition() {
  kubectl get node "$1" -o json | python3 -c '
import json, sys
node=json.load(sys.stdin)
kind=sys.argv[1]
print(next((item["status"] for item in node.get("status", {}).get("conditions", []) if item["type"] == kind), ""))
' "$2"
}
raw_healthy() {
  [[ $(node_condition "$1" HardwareFaultDetected) == False ]]
}
for compute_node in "${cluster_name}-worker" "${cluster_name}-worker2"; do
  retry 180 raw_healthy "$compute_node"
done

compute_workers_empty() {
  [[ $(kubectl -n slurm-system get pods -l app.kubernetes.io/component=slurmd --no-headers 2>/dev/null | wc -l) == 0 ]]
}
retry 180 compute_workers_empty

login=(kubectl -n slurm-system exec deployment/research-login -c login --)
job_running() {
  [[ $("${login[@]}" squeue --noheader --jobs "$1" --format='%T' 2>/dev/null | awk 'NF {print $1; exit}') == RUNNING ]]
}
job_completed() {
  local state
  state=$("${login[@]}" sacct --noheader --allocations --jobs "$1" --format=State 2>/dev/null | awk 'NF {print $1; exit}')
  [[ $state == COMPLETED ]] && return 0
  case $state in
    FAILED | CANCELLED | TIMEOUT | NODE_FAIL | OUT_OF_MEMORY) return 2 ;;
  esac
  return 1
}
job_restarts() {
  "${login[@]}" sacct --noheader --allocations --jobs "$1" --format=Restarts 2>/dev/null | awk 'NF {print $1; exit}'
}
marker_ready() {
  kubectl -n slurm-system exec phase3-mc -- mc stat --insecure "local/checkpoints/checkpoints/phase4_${1}/step_00000000.complete" >/dev/null 2>&1
}
worker_for_job() {
  "${login[@]}" squeue --noheader --jobs "$1" --format='%N' 2>/dev/null | awk 'NF && $1 != "(null)" {print $1; exit}'
}

affected=$("${login[@]}" sbatch --parsable --requeue --signal=B:USR1@120 \
  --output=/dev/shm/ai-orch/slurm-%j.out --partition=compute --nodes=1 --cpus-per-task=1 --mem=1G \
  --wrap='exec bash /usr/lib/python3/dist-packages/checkpointing/checkpoint-batch.sh python3 -m checkpointing.recovery_smoke')
affected=${affected%%;*}
retry 300 job_running "$affected"
retry 300 marker_ready "$affected"
affected_worker=$(worker_for_job "$affected")
[[ -n $affected_worker ]]
failed_node=$(kubectl -n slurm-system get pod "$affected_worker" -o jsonpath='{.spec.nodeName}')
[[ -n $failed_node ]]

kubectl taint node "$failed_node" phase4-placement=true:NoSchedule
unrelated=$("${login[@]}" sbatch --parsable --requeue --partition=compute --nodes=1 --cpus-per-task=1 --mem=1G --wrap='sleep 600')
unrelated=${unrelated%%;*}
retry 300 job_running "$unrelated"
unrelated_worker=$(worker_for_job "$unrelated")
unrelated_node=$(kubectl -n slurm-system get pod "$unrelated_worker" -o jsonpath='{.spec.nodeName}')
[[ -n $unrelated_node && $unrelated_node != "$failed_node" ]]
kubectl taint node "$failed_node" phase4-placement:NoSchedule-

probe=/var/lib/gputpu-watchdog/bin/watchdog-daemon
docker exec "$failed_node" cp "$probe" "$probe.healthy"
docker cp "$root/test/phase4/fault-probe.sh" "$failed_node:$probe"
docker exec "$failed_node" chmod 0755 "$probe"
raw_faulted() {
  [[ $(node_condition "$failed_node" HardwareFaultDetected) == True ]]
}
retry 120 raw_faulted

recovery_phase_is() {
  [[ $(kubectl get node "$failed_node" -o "jsonpath={.metadata.annotations.orchestration\.gputpu\.io/recovery-phase}") == "$1" ]]
}
retry 120 recovery_phase_is Checkpointing
[[ $("${login[@]}" squeue --noheader --jobs "$unrelated" --format='%T') == RUNNING ]]
[[ $(job_restarts "$unrelated") == 0 ]]

kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: quarantined-worker-probe
  namespace: slurm-system
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: $failed_node
  tolerations:
  - key: orchestration.gputpu.io/compute
    operator: Exists
    effect: NoSchedule
  containers:
  - name: probe
    image: slurm-worker:dev
    command: [sleep, "300"]
EOF
ordinary_workload_blocked() {
  [[ $(kubectl -n slurm-system get pod quarantined-worker-probe -o jsonpath='{.status.phase}') == Pending ]] &&
    [[ -z $(kubectl -n slurm-system get pod quarantined-worker-probe -o jsonpath='{.spec.nodeName}') ]]
}
retry 60 ordinary_workload_blocked
kubectl -n slurm-system delete pod quarantined-worker-probe --wait=false

kubectl -n slurm-system rollout restart deployment/slurm-operator
kubectl -n slurm-system rollout status deployment/slurm-operator --timeout=3m
reboot_acknowledged() {
  local incident ack
  incident=$(kubectl get node "$failed_node" -o 'jsonpath={.metadata.annotations.orchestration\.gputpu\.io/recovery-incident}')
  ack=$(kubectl get node "$failed_node" -o 'jsonpath={.metadata.annotations.orchestration\.gputpu\.io/reboot-ack}')
  [[ -n $incident && $ack == "$incident" ]] && recovery_phase_is Rebooting
}
retry 240 reboot_acknowledged
docker exec "$failed_node" mv "$probe.healthy" "$probe"
actual_boot=$(kubectl get node "$failed_node" -o jsonpath='{.status.nodeInfo.bootID}')
simulated_boot="phase4-${affected}"
kubectl annotate node "$failed_node" --overwrite \
  "orchestration.gputpu.io/test-boot-id=$simulated_boot" \
  "orchestration.gputpu.io/inventory-boot-id=$simulated_boot"
retry 180 recovery_phase_is Verifying
kubectl annotate node "$failed_node" --overwrite "orchestration.gputpu.io/inventory-boot-id=$actual_boot"

recovery_complete() {
  kubectl get node "$failed_node" -o json | python3 -c '
import json, sys
node=json.load(sys.stdin)
annotations=node.get("metadata", {}).get("annotations", {})
taints=node.get("spec", {}).get("taints", [])
conditions=node.get("status", {}).get("conditions", [])
assert "orchestration.gputpu.io/recovery-incident" not in annotations
assert not any(item.get("key") == "orchestration.gputpu.io/hardware-degraded" for item in taints)
assert any(item.get("type") == "HardwareDegraded" and item.get("status") == "False" for item in conditions)
'
}
retry 300 recovery_complete
retry 300 job_completed "$affected"
[[ $(job_restarts "$affected") == 1 ]]
[[ $("${login[@]}" squeue --noheader --jobs "$unrelated" --format='%T') == RUNNING ]]
[[ $(job_restarts "$unrelated") == 0 ]]
"${login[@]}" scancel "$unrelated"

kubectl annotate node "$failed_node" --overwrite "orchestration.gputpu.io/test-boot-id=$actual_boot"
failed_node_empty() {
  kubectl -n slurm-system get pods -l app.kubernetes.io/component=slurmd -o json | python3 -c '
import json, sys
assert not any(item.get("spec", {}).get("nodeName") == sys.argv[1] for item in json.load(sys.stdin).get("items", []))
' "$failed_node"
}
retry 180 failed_node_empty
kubectl -n slurm-system set env deployment/slurm-operator WORKER_IMAGE=slurm-control-plane:dev
kubectl -n slurm-system rollout status deployment/slurm-operator --timeout=3m

docker exec "$failed_node" cp "$probe" "$probe.healthy"
docker cp "$root/test/phase4/fault-probe.sh" "$failed_node:$probe"
docker exec "$failed_node" chmod 0755 "$probe"
retry 120 raw_faulted
retry 180 reboot_acknowledged
docker exec "$failed_node" mv "$probe.healthy" "$probe"
simulated_boot="phase4-failed-${affected}"
kubectl annotate node "$failed_node" --overwrite \
  "orchestration.gputpu.io/test-boot-id=$simulated_boot" \
  "orchestration.gputpu.io/inventory-boot-id=$simulated_boot"
retry 180 recovery_phase_is Verifying
kubectl annotate node "$failed_node" --overwrite "orchestration.gputpu.io/inventory-boot-id=$actual_boot"

manual_repair() {
  recovery_phase_is ManualRepair && kubectl get node "$failed_node" -o json | python3 -c '
import json, sys
node=json.load(sys.stdin)
assert any(item.get("key") == "orchestration.gputpu.io/hardware-degraded" for item in node.get("spec", {}).get("taints", []))
assert any(item.get("type") == "HardwareDegraded" and item.get("status") == "True" and item.get("reason") == "ManualRepair" for item in node.get("status", {}).get("conditions", []))
'
}
retry 300 manual_repair

echo "Phase 4 isolation, selective checkpoint/requeue, restart, reboot, verification, quarantine, and failure gates passed"
