#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

images=(
  "slurm-operator:${OPERATOR_IMAGE:-}"
  "slurm-worker:${WORKER_IMAGE:-}"
  "orchestration-dra:${DRA_IMAGE:-}"
  "quantization-engine:${QUANTIZATION_IMAGE:-}"
  "watchdog-daemon:${WATCHDOG_IMAGE:-}"
)
for item in "${images[@]}" "slurm-control-plane:${SLURM_CONTROL_PLANE_IMAGE:-}"; do
  ref=${item#*:}
  [[ $ref =~ ^[^@[:space:]]+@sha256:[0-9a-f]{64}$ ]] || {
    echo "${item%%:*} must be an immutable image reference ending in @sha256:<64 lowercase hex>" >&2
    exit 1
  }
done

cp -R "$root/src/manifests" "$work/base"
{
  printf '%s\n' 'apiVersion: kustomize.config.k8s.io/v1beta1' 'kind: Kustomization' 'resources:' '- base' 'images:'
  for item in "${images[@]}"; do
    name=${item%%:*}
    ref=${item#*:}
    printf -- '- name: %s\n  newName: %s\n  digest: %s\n' "$name" "${ref%@*}" "${ref#*@}"
  done
} >"$work/kustomization.yaml"
kubectl kustomize "$work"
