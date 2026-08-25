#!/usr/bin/env bash
#SBATCH --requeue
#SBATCH --signal=B:USR1@120
set -euo pipefail

[[ $# -gt 0 ]] || { echo "usage: checkpoint-batch.sh COMMAND [ARG ...]" >&2; exit 2; }

"$@" &
child=$!
trap 'kill -USR1 "$child" 2>/dev/null || true' USR1
while kill -0 "$child" 2>/dev/null; do
  wait "$child" || true
done
wait "$child"
