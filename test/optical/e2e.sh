#!/usr/bin/env bash
set -euo pipefail

cluster_name=${KIND_CLUSTER:-optical-demo}
kind_image=${KIND_NODE_IMAGE:-kindest/node:v1.35.5@sha256:ce977ae6d65918d0b58a5f8b5e940429c2ce42fa3a5619ec2bbc60b949c0ac95}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$root/test/lib.sh"
work=$(mktemp -d)
cluster_created=0

cleanup() {
  if [[ $cluster_created == 1 && ${KEEP_KIND:-0} != 1 ]]; then
    kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
diagnose() {
  echo "Optical software demo failed near line $1" >&2
  if [[ $cluster_created == 1 ]]; then
    kubectl get nodes -o wide >&2 || true
    kubectl -n slurm-system get pods -o wide >&2 || true
    kubectl get resourceclaims -o wide >&2 || true
    kubectl get resourceslices >&2 || true
    kubectl get events --all-namespaces --sort-by=.lastTimestamp | tail -40 >&2 || true
  fi
}
trap cleanup EXIT
trap 'diagnose "$LINENO"' ERR

require_commands docker kind kubectl python3
docker info >/dev/null

cat >"$work/kind.yaml" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
containerdConfigPatches:
- |-
  [plugins."io.containerd.nri.v1.nri"]
    disable = false
    disable_connections = false
nodes:
- role: control-plane
- role: worker
EOF

kind create cluster --name "$cluster_name" --image "$kind_image" --config "$work/kind.yaml" --wait 5m
cluster_created=1

docker build -t orchestration-dra:dev -f "$root/src/dra-driver/Dockerfile" "$root"
docker build -t orchestration-optical-dra:dev -f "$root/src/optical-dra-driver/Dockerfile" "$root"
kind load docker-image --name "$cluster_name" orchestration-dra:dev orchestration-optical-dra:dev

compute_node=${cluster_name}-worker
kubectl label node "$compute_node" --overwrite orchestration.gputpu.io/compute=true
kubectl annotate node "$compute_node" --overwrite \
  orchestration.gputpu.io/memory-unit=1Gi \
  orchestration.gputpu.io/reserved-cores-per-numa=1 \
  orchestration.gputpu.io/reserved-memory-per-numa=1Gi \
  'orchestration.gputpu.io/optical-topology-v1={"version":1,"devices":[{"kind":"switch","name":"r300-demo-port-001","model":"R300","vendor":"Lumentum","partNumber":"R300","formFactor":"chassis-port","protocol":"any","managementInterface":"gNMI","sourceId":"r300-demo","topology":"demo-fabric","location":"r300-demo/port-001","ports":1},{"kind":"cpo-photonic","name":"elsfp-demo-output-01","model":"ELSFP-350","vendor":"Lumentum","partNumber":"ELSFP-350","formFactor":"ELSFP","protocol":"any","componentRole":"external-laser-source","sourceId":"elsfp-demo","topology":"demo-fabric","location":"elsfp-demo/output-01","wavelengthNm":1311,"outputPowerDbm":24},{"kind":"physical-asic","name":"coherent-aoc-demo","model":"C.wire","vendor":"Coherent","formFactor":"CXP","protocol":"any","componentRole":"active-optical-cable-endpoint","linkId":"demo-link","topology":"demo-fabric","location":"demo-host/cxp-1","bandwidthGbps":150,"lanes":12,"fullDuplex":true}]}'

kubectl apply -f "$root/src/manifests/workloads/namespace.yaml"
kubectl apply -f "$root/src/manifests/workloads/deviceclasses.yaml"
kubectl apply -f "$root/src/manifests/workloads/optical-admission-policy.yaml"
kubectl apply -f "$root/src/manifests/workloads/dra-driver.yaml"
kubectl apply -f "$root/src/manifests/workloads/optical-dra-driver.yaml"
kubectl -n slurm-system wait --for=jsonpath='{.status.numberReady}'=1 daemonset/orchestration-dra --timeout=5m
kubectl -n slurm-system wait --for=jsonpath='{.status.numberReady}'=1 daemonset/orchestration-optical-dra --timeout=5m

slices_ready() {
  kubectl get resourceslices -o json | python3 -c '
import json, sys
items = json.load(sys.stdin).get("items", [])
node = sys.argv[1]
drivers = {item.get("spec", {}).get("driver") for item in items if item.get("spec", {}).get("nodeName") == node}
assert {"orchestration.gputpu.io", "orchestration.optical.gputpu.io"} <= drivers
expected = {"switch": "lumentum", "cpo-photonic": "lumentum", "physical-asic": "coherent"}
found = {}
for item in items:
    if item.get("spec", {}).get("driver") != "orchestration.optical.gputpu.io":
        continue
    for device in item.get("spec", {}).get("devices", []):
        attrs = device.get("attributes", {})
        kind = attrs.get("orchestration.optical.gputpu.io/kind", {}).get("string")
        vendor = attrs.get("orchestration.optical.gputpu.io/vendor", {}).get("string")
        found[kind] = vendor
assert found == expected, found
' "$compute_node"
}
retry 120 slices_ready

kubectl apply -f "$root/src/optical-dra-driver/examples/dual-claim-pod.yaml"
kubectl wait --for=condition=Ready pod/dual-claim-workload --timeout=5m

kubectl get resourceclaims compute-claim optical-claim -o json | python3 -c '
import json, sys
claims = {item["metadata"]["name"]: item for item in json.load(sys.stdin)["items"]}
compute = claims["compute-claim"]["status"]["allocation"]["devices"]["results"]
optical = claims["optical-claim"]["status"]["allocation"]["devices"]["results"]
assert compute and all(result["driver"] == "orchestration.gputpu.io" for result in compute)
assert len(optical) == 3 and all(result["driver"] == "orchestration.optical.gputpu.io" for result in optical)
'
test "$(kubectl get pod dual-claim-workload -o jsonpath='{.spec.nodeName}')" = "$compute_node"
kubectl -n slurm-system exec daemonset/orchestration-optical-dra -- \
  sh -c 'find /var/lib/kubelet/plugins/orchestration.optical.gputpu.io/state -name "*.json" -print -quit | grep -q .'

if admission_output=$(kubectl create --dry-run=server -f - 2>&1 <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: invalid-optical-demo
  labels:
    orchestration.gputpu.io/optical-required: "true"
spec:
  restartPolicy: Never
  resourceClaims:
  - name: compute
    resourceClaimName: compute-claim
  containers:
  - name: workload
    image: orchestration-optical-dra:dev
    resources:
      claims:
      - name: compute
EOF
); then
  echo "admission accepted an optical-required Pod without an optical claim" >&2
  exit 1
fi
grep -q 'must declare compute and optical ResourceClaims' <<<"$admission_output"

echo "Optical software demo passed: emulated Lumentum/Coherent inventory, dual allocation, preparation, and admission"
if [[ ${KEEP_KIND:-0} == 1 ]]; then
  echo "kind cluster $cluster_name retained; inspect with: kubectl get pod,resourceclaims && kubectl get resourceslices" >&2
fi
