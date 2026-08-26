#!/usr/bin/env bash
set -euo pipefail

namespace=${SLURM_NAMESPACE:-slurm-system}
cluster=${SLURM_CLUSTER:-research}
model=${GPU_GRES_MODEL:-rtx_4050}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$root/test/lib.sh"

require_commands kubectl nvidia-smi

if [[ ${ENABLE_GPU:-0} != 1 ]]; then
  require_commands docker kind
  export GPU_REAL_DOCKER
  GPU_REAL_DOCKER=$(command -v docker)
  export EXPECTED_GPU_UUID
  EXPECTED_GPU_UUID=$(nvidia-smi --query-gpu=uuid --format=csv,noheader | head -n1)
  PATH="$root/test/phase2/gpu-bin:$PATH" ENABLE_GPU=1 "$root/test/phase2/e2e.sh"
  exit
fi

expected_uuid=${EXPECTED_GPU_UUID:-$(nvidia-smi --query-gpu=uuid --format=csv,noheader | head -n1)}
[[ -n $expected_uuid ]]

login=(kubectl -n "$namespace" exec deployment/${cluster}-login -c login --)
job=$("${login[@]}" sbatch --parsable --gres="gpu:${model}:1" --wrap='python3 /usr/local/bin/cuda-smoke.py && sleep 60')
job=${job%%;*}

running_deadline=$((SECONDS + 300))
while (( SECONDS < running_deadline )); do
  node=$("${login[@]}" squeue --noheader --jobs "$job" --states=RUNNING --format=%N 2>/dev/null | awk 'NF {print $1; exit}')
  [[ -n $node ]] && break
  sleep 3
done
[[ -n ${node:-} ]]

claim=$(kubectl -n "$namespace" get pod "$node" -o jsonpath='{.metadata.annotations.orchestration\.gputpu\.io/resource-claim}')
kubectl -n "$namespace" get resourceclaim "$claim" -o json | python3 -c '
import json, sys
claim=json.load(sys.stdin)
gpu=[result for result in claim["status"]["allocation"]["devices"]["results"] if result.get("request", "").startswith("accelerator-gpu-")]
assert len(gpu) == 1
assert gpu[0]["driver"] == "orchestration.gputpu.io"
assert gpu[0]["device"] == "gpu-" + sys.argv[1].lower()
' "$expected_uuid"

device_path=$(kubectl get resourceslices -o json | python3 -c '
import json, sys
for resource_slice in json.load(sys.stdin).get("items", []):
    for device in resource_slice.get("spec", {}).get("devices", []):
        attrs=device.get("attributes", {})
        if attrs.get("orchestration.gputpu.io/uuid", {}).get("string") == sys.argv[1]:
            print(attrs["orchestration.gputpu.io/path"]["string"])
            raise SystemExit(0)
raise SystemExit(1)
' "$expected_uuid")
kubectl -n "$namespace" exec "$node" -c slurmd -- test -c "$device_path"
kubectl -n "$namespace" exec "$node" -c slurmd -- nvidia-smi --query-gpu=uuid --format=csv,noheader | grep -Fxq "$expected_uuid"

deadline=$((SECONDS + 600))
while (( SECONDS < deadline )); do
  state=$("${login[@]}" sacct --noheader --allocations --jobs "$job" --format=State 2>/dev/null | awk 'NF {print $1; exit}')
  [[ $state == COMPLETED ]] && break
  [[ $state == FAILED || $state == CANCELLED || $state == TIMEOUT ]] && { echo "GPU job $job ended in $state" >&2; exit 1; }
  sleep 3
done
[[ ${state:-} == COMPLETED ]]

"${login[@]}" scontrol show node "$node" | grep -q "Gres=gpu:${model}:1"
echo "Physical NVIDIA DRA, Slurm GRES, CDI, and CUDA gate passed"
