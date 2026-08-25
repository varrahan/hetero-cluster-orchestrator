#!/usr/bin/env bash
set -euo pipefail

cluster_name=${KIND_CLUSTER:-phase1}
kind_image=${KIND_NODE_IMAGE:-kindest/node:v1.35.5@sha256:ce977ae6d65918d0b58a5f8b5e940429c2ce42fa3a5619ec2bbc60b949c0ac95}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$root/test/lib.sh"
work=$(mktemp -d)
port_forward_pid=
paused_node=
paused_container=
cluster_created=0

cleanup() {
  if [[ -n "$port_forward_pid" ]]; then
    kill "$port_forward_pid" 2>/dev/null || true
  fi
  if [[ -n "$paused_container" ]]; then
    docker exec "$paused_node" ctr -n k8s.io tasks resume "$paused_container" >/dev/null 2>&1 || true
  fi
  if [[ $cluster_created == 1 && ${KEEP_KIND:-0} != 1 ]]; then
    docker exec "${cluster_name}-control-plane" chown -R "$(id -u):$(id -g)" /phase1-state >/dev/null 2>&1 || true
    kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
  fi
  if [[ ${KEEP_KIND:-0} != 1 ]]; then
    rm -rf "$work"
  else
    echo "kept kind cluster $cluster_name and fixtures under $work" >&2
  fi
}
trap cleanup EXIT
trap 'echo "Phase 1 live gate failed near line $LINENO" >&2' ERR

for command in docker kind kubectl curl python3; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done
docker info >/dev/null

mkdir "$work/state"
cat >"$work/kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
$(if [[ ${ENABLE_NRI:-0} == 1 ]]; then cat <<'NRI'
containerdConfigPatches:
- |-
  [plugins."io.containerd.nri.v1.nri"]
    disable = false
    disable_connections = false
NRI
fi)
nodes:
- role: control-plane
  extraMounts:
  - hostPath: $work/state
    containerPath: /phase1-state
- role: worker
  extraMounts:
  - hostPath: $work/state
    containerPath: /phase1-state
- role: worker
  extraMounts:
  - hostPath: $work/state
    containerPath: /phase1-state
EOF

kind create cluster --name "$cluster_name" --image "$kind_image" --config "$work/kind.yaml" --wait 5m
cluster_created=1
docker build --target operator -t slurm-operator:dev -f "$root/src/slurm-operator/Dockerfile" "$root"
docker build --target slurm-control-plane -t slurm-control-plane:dev -f "$root/src/slurm-operator/Dockerfile" "$root"
kind load docker-image --name "$cluster_name" slurm-operator:dev slurm-control-plane:dev
kubectl apply -k "$root/src/manifests"

head -c 32 /dev/urandom >"$work/munge.key"
head -c 32 /dev/urandom >"$work/jwt_hs256.key"
kubectl -n slurm-system create secret generic slurm-munge --from-file=munge.key="$work/munge.key"
kubectl -n slurm-system create secret generic slurm-jwt --from-file=jwt_hs256.key="$work/jwt_hs256.key"
kubectl -n slurm-system create secret generic slurm-mariadb \
  --from-literal=host=mariadb \
  --from-literal=port=3306 \
  --from-literal=database=slurm_acct_db \
  --from-literal=username=slurm \
  --from-literal=password=phase1-test-only

cat >"$work/fixtures.yaml" <<'EOF'
apiVersion: v1
kind: PersistentVolume
metadata:
  name: slurm-state
spec:
  capacity:
    storage: 1Gi
  accessModes: [ReadWriteMany]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: manual
  hostPath:
    path: /phase1-state
    type: Directory
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: slurm-state
  namespace: slurm-system
spec:
  accessModes: [ReadWriteMany]
  storageClassName: manual
  volumeName: slurm-state
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: mariadb
  namespace: slurm-system
spec:
  selector:
    app: mariadb
  ports:
  - name: mysql
    port: 3306
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mariadb
  namespace: slurm-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mariadb
  template:
    metadata:
      labels:
        app: mariadb
    spec:
      containers:
      - name: mariadb
        image: mariadb:11.8.8
        env:
        - name: MARIADB_DATABASE
          value: slurm_acct_db
        - name: MARIADB_USER
          value: slurm
        - name: MARIADB_PASSWORD
          value: phase1-test-only
        - name: MARIADB_ROOT_PASSWORD
          value: phase1-root-test-only
        ports:
        - name: mysql
          containerPort: 3306
        readinessProbe:
          exec:
            command: [healthcheck.sh, --connect, --innodb_initialized]
          periodSeconds: 2
          timeoutSeconds: 2
          failureThreshold: 30
EOF
kubectl apply -f "$work/fixtures.yaml"
kubectl -n slurm-system rollout status deployment/mariadb --timeout=5m

cat >"$work/cluster.yaml" <<'EOF'
apiVersion: orchestration.gputpu.io/v1alpha1
kind: HeterogeneousCluster
metadata:
  name: research
  namespace: slurm-system
spec:
  controlPlane:
    controllers:
      image: slurm-control-plane:dev
      stateSaveClaim: slurm-state
    accounting:
      databaseSecretRef: slurm-mariadb
    login: {}
  authentication:
    mungeKeySecretRef: slurm-munge
    jwtKeySecretRef: slurm-jwt
  workerPools:
  - name: compute
    partition: compute
    memoryUnit: 1Gi
    scaling:
      maxWorkers: 4
      idleTimeout: 15s
EOF
kubectl apply -f "$work/cluster.yaml"
kubectl -n slurm-system wait --for=condition=ControlPlaneReady heterogeneouscluster/research --timeout=10m
kubectl -n slurm-system wait --for=condition=AccountingReady heterogeneouscluster/research --timeout=5m

login=(kubectl -n slurm-system exec deployment/research-login -c login --)
job_id=$("${login[@]}" sbatch --parsable --wrap='sleep 600')
job_id=${job_id%%;*}
"${login[@]}" squeue --noheader --jobs "$job_id" | grep -q "$job_id"

accounting_has_job() {
  "${login[@]}" sacct --noheader --allocations --jobs "$job_id" --format=JobIDRaw 2>/dev/null | grep -Eq "^[[:space:]]*$job_id"
}
retry 120 accounting_has_job

paused_node=$(kubectl -n slurm-system get pod -l app=mariadb -o jsonpath='{.items[0].spec.nodeName}')
paused_container=$(kubectl -n slurm-system get pod -l app=mariadb -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="mariadb")].containerID}')
paused_container=${paused_container#containerd://}
docker exec "$paused_node" ctr -n k8s.io tasks pause "$paused_container"
kubectl -n slurm-system wait --for=condition=AccountingReady=false heterogeneouscluster/research --timeout=3m
"${login[@]}" squeue --noheader --jobs "$job_id" | grep -q "$job_id"
docker exec "$paused_node" ctr -n k8s.io tasks resume "$paused_container"
paused_container=
kubectl -n slurm-system wait --for=condition=AccountingReady heterogeneouscluster/research --timeout=5m
retry 120 accounting_has_job

kubectl -n slurm-system port-forward service/research-slurmrestd 16820:6820 >"$work/port-forward.log" 2>&1 &
port_forward_pid=$!
retry 30 curl --silent --output /dev/null http://127.0.0.1:16820/openapi/v3

rest_has_job() {
  local token
  token=$(python3 - "$work/jwt_hs256.key" <<'PY'
import base64, hashlib, hmac, json, pathlib, sys, time
encode = lambda value: base64.urlsafe_b64encode(value).rstrip(b"=")
header = encode(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
now = int(time.time())
payload = encode(json.dumps({"iat": now, "exp": now + 60, "sun": "root"}, separators=(",", ":")).encode())
body = header + b"." + payload
signature = encode(hmac.new(pathlib.Path(sys.argv[1]).read_bytes(), body, hashlib.sha256).digest())
print((body + b"." + signature).decode())
PY
  )
  curl --fail --silent \
    -H 'X-SLURM-USER-NAME: root' \
    -H "X-SLURM-USER-TOKEN: $token" \
    http://127.0.0.1:16820/slurm/v0.0.45/jobs/ |
    python3 -c 'import json, sys; jobs=json.load(sys.stdin).get("jobs", []); assert any(str(job.get("job_id")) == sys.argv[1] for job in jobs)' "$job_id"
}
retry 60 rest_has_job

statefulset_uid=$(kubectl -n slurm-system get statefulset/research-slurmctld -o jsonpath='{.metadata.uid}')
kubectl -n slurm-system scale deployment/slurm-operator --replicas=0
kubectl -n slurm-system rollout status deployment/slurm-operator --timeout=2m
kubectl -n slurm-system delete service/research-slurmrestd
kubectl -n slurm-system scale deployment/slurm-operator --replicas=2
kubectl -n slurm-system rollout status deployment/slurm-operator --timeout=3m
retry 120 kubectl -n slurm-system get service/research-slurmrestd
kubectl -n slurm-system wait --for=condition=ControlPlaneReady heterogeneouscluster/research --timeout=3m
test "$statefulset_uid" = "$(kubectl -n slurm-system get statefulset/research-slurmctld -o jsonpath='{.metadata.uid}')"
"${login[@]}" squeue --noheader --jobs "$job_id" | grep -q "$job_id"

paused_node=$(kubectl -n slurm-system get pod/research-slurmctld-0 -o jsonpath='{.spec.nodeName}')
paused_container=$(kubectl -n slurm-system get pod/research-slurmctld-0 -o jsonpath='{.status.containerStatuses[?(@.name=="slurmctld")].containerID}')
paused_container=${paused_container#containerd://}
kubectl -n slurm-system exec research-slurmctld-1 -c slurmctld -- sh -c "sed '/^SlurmctldHost=research-slurmctld-0/d' /etc/slurm/slurm.conf > /tmp/slurm-backup.conf"
docker exec "$paused_node" ctr -n k8s.io tasks pause "$paused_container"
kubectl -n slurm-system exec research-slurmctld-1 -c slurmctld -- scontrol takeover
backup_serves_job() {
  local queue
  queue=$(kubectl -n slurm-system exec research-slurmctld-1 -c slurmctld -- env SLURM_CONF=/tmp/slurm-backup.conf timeout 15 squeue --noheader --jobs "$job_id" 2>/dev/null) || return 1
  grep -q "$job_id" <<<"$queue"
}
retry 120 backup_serves_job

echo "Phase 1 control-plane, MariaDB outage, restart reconciliation, and failover gates passed"
