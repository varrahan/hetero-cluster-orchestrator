#!/usr/bin/env bash
set -euo pipefail

cluster_name=${KIND_CLUSTER:-phase3}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$root/test/lib.sh"
keep=${KEEP_KIND:-0}

cleanup() {
  if [[ $keep != 1 ]]; then
    kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
  fi
}
diagnose() {
  local node output
  echo "Phase 3 live gate failed near line $1" >&2
  if declare -p login >/dev/null 2>&1; then
    "${login[@]}" squeue --all --format='%.18i %.9T %.40R' >&2 || true
    if [[ -n ${job:-} ]]; then
      "${login[@]}" scontrol show job "$job" >&2 || true
      "${login[@]}" sacct --jobs "$job" --format=JobID,State,ExitCode,NodeList >&2 || true
      node=$("${login[@]}" scontrol show job "$job" 2>/dev/null | awk -F= '/^[[:space:]]*NodeList=/{print $2; exit}')
      output=$("${login[@]}" scontrol show job "$job" 2>/dev/null | awk -F= '/StdOut=/{print $2; exit}')
      if [[ -n $node && -n $output ]]; then
        kubectl -n slurm-system exec "$node" -c slurmd -- cat "$output" >&2 || true
      fi
      kubectl -n slurm-system exec daemonset/quantization-engine -- sh -c 'cat /dev/shm/ai-orch/slurm-*.out' >&2 || true
    fi
  fi
  kubectl -n slurm-system get pods -o wide >&2 || true
  kubectl -n slurm-system get resourceclaims -o wide >&2 || true
  kubectl -n slurm-system logs -l app.kubernetes.io/component=slurmd -c checkpoint-flusher --tail=80 --prefix >&2 || true
  kubectl -n slurm-system logs deployment/phase3-minio --tail=80 >&2 || true
  kubectl -n slurm-system get events --sort-by=.lastTimestamp | tail -40 >&2 || true
}
trap cleanup EXIT
trap 'diagnose "$LINENO"' ERR

for command in docker kind kubectl openssl; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done

KIND_CLUSTER="$cluster_name" KEEP_KIND=1 "$root/test/phase2/e2e.sh"

docker build -t quantization-engine:dev -f "$root/src/quantization-engine/Dockerfile" "$root"
kind load docker-image --name "$cluster_name" quantization-engine:dev
kubectl -n slurm-system delete pods -l app.kubernetes.io/name=quantization-engine --ignore-not-found
kubectl -n slurm-system rollout status daemonset/quantization-engine --timeout=5m

certs=$(mktemp -d)
trap 'rm -rf "$certs"; cleanup' EXIT
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=minio.slurm-system.svc \
  -addext subjectAltName=DNS:minio.slurm-system.svc,DNS:minio.slurm-system.svc.cluster.local \
  -keyout "$certs/private.key" -out "$certs/public.crt" >/dev/null 2>&1
kubectl -n slurm-system create secret generic phase3-minio-tls \
  --from-file=private.key="$certs/private.key" --from-file=public.crt="$certs/public.crt" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Service
metadata: {name: minio, namespace: slurm-system}
spec:
  selector: {app: phase3-minio}
  ports: [{name: https, port: 9000, targetPort: 9000}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: phase3-minio, namespace: slurm-system}
spec:
  replicas: 1
  selector: {matchLabels: {app: phase3-minio}}
  template:
    metadata: {labels: {app: phase3-minio}}
    spec:
      containers:
      - name: minio
        image: quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z
        args: [server, /data, --address, ":9000", --certs-dir, /certs]
        env:
        - {name: MINIO_ROOT_USER, value: phase3access}
        - {name: MINIO_ROOT_PASSWORD, value: phase3secretkey}
        ports: [{containerPort: 9000}]
        readinessProbe: {tcpSocket: {port: 9000}, periodSeconds: 2}
        volumeMounts:
        - {name: data, mountPath: /data}
        - {name: tls, mountPath: /certs, readOnly: true}
      volumes:
      - {name: data, emptyDir: {}}
      - {name: tls, secret: {secretName: phase3-minio-tls}}
EOF
kubectl -n slurm-system rollout status deployment/phase3-minio --timeout=5m
kubectl -n slurm-system run phase3-mc --image=quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z --restart=Never --command -- sh -ec \
  'mc alias set --insecure local https://minio.slurm-system.svc:9000 phase3access phase3secretkey && mc mb --insecure --ignore-existing local/checkpoints && sleep 3600'
kubectl -n slurm-system wait --for=condition=Ready pod/phase3-mc --timeout=3m
kubectl -n slurm-system create secret generic checkpoint-store \
  --from-literal=endpoint=https://minio.slurm-system.svc:9000 \
  --from-literal=bucket=checkpoints --from-literal=accessKey=phase3access \
  --from-literal=secretKey=phase3secretkey --from-file=ca.crt="$certs/public.crt" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n slurm-system patch heterogeneouscluster research --type=merge \
  -p '{"spec":{"checkpointing":{"objectStoreSecretRef":"checkpoint-store"}}}'

login=(kubectl -n slurm-system exec deployment/research-login -c login --)
job=$("${login[@]}" sbatch --parsable --requeue --signal=B:USR1@120 \
  --output=/dev/shm/ai-orch/slurm-%j.out \
  --partition=compute --nodes=1 --cpus-per-task=1 --mem=1G : \
  --partition=compute --nodes=1 --cpus-per-task=1 --mem=1G --gres=tpu:opentpu_m8:1 \
  --wrap='srun --het-group=0,1 --ntasks=1 python3 -m checkpointing.smoke')
job=${job%%;*}

marker_ready() {
  local state
  if kubectl -n slurm-system exec phase3-mc -- mc stat --insecure "local/checkpoints/checkpoints/phase3_${job}/step_00000005.complete" >/dev/null 2>&1; then
    return 0
  fi
  state=$("${login[@]}" sacct --noheader --allocations --jobs "$job" --format=State 2>/dev/null | awk 'NF {print $1; exit}')
  case $state in
    COMPLETED | FAILED | CANCELLED | TIMEOUT | NODE_FAIL | OUT_OF_MEMORY) return 2 ;;
  esac
  return 1
}
retry 300 marker_ready

kubectl -n slurm-system patch service/minio --type=merge -p '{"spec":{"selector":{"app":"phase3-minio-unavailable"}}}'
kubectl -n slurm-system wait --for=condition=CheckpointStoreReady=false heterogeneouscluster/research --timeout=3m
"${login[@]}" squeue --noheader --jobs "$job" | grep -q "$job"
kubectl -n slurm-system patch service/minio --type=merge -p '{"spec":{"selector":{"app":"phase3-minio"}}}'
kubectl -n slurm-system wait --for=condition=CheckpointStoreReady heterogeneouscluster/research --timeout=3m
retry 300 marker_ready

old_workers=$(kubectl -n slurm-system get pods -l app.kubernetes.io/component=slurmd -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}')
sleep 20
"${login[@]}" scontrol requeuehold "$job"
requeued_held() {
	"${login[@]}" scontrol show job "$job" | grep -q 'Reason=job_requeued_in_held_state'
}
retry 60 requeued_held
for worker in $old_workers; do kubectl -n slurm-system delete pod "$worker" --ignore-not-found --wait=false; done
for worker in $old_workers; do kubectl -n slurm-system wait --for=delete "pod/$worker" --timeout=120s; done
if "${login[@]}" scontrol show job "$job" | grep -q 'JobState=CANCELLED'; then
	"${login[@]}" scontrol requeue "$job"
fi
retry 120 "${login[@]}" scontrol release "$job"

completed() {
  local state
  state=$("${login[@]}" sacct --noheader --allocations --jobs "$job" --format=State 2>/dev/null | awk 'NF {print $1; exit}')
  [[ $state == COMPLETED ]] && return 0
  case $state in
    FAILED | CANCELLED | TIMEOUT | NODE_FAIL | OUT_OF_MEMORY) return 2 ;;
  esac
  return 1
}
retry 600 completed
new_workers=$(kubectl -n slurm-system get pods -l app.kubernetes.io/component=slurmd -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}')
[[ $new_workers != "$old_workers" ]]
! kubectl -n slurm-system exec phase3-mc -- mc stat --insecure "local/checkpoints/checkpoints/phase3_${job}/step_00000006.complete" >/dev/null 2>&1

echo "Phase 3 heterogeneous checkpoint, MinIO/network outage, interrupted upload, placement change, and requeue gates passed"
