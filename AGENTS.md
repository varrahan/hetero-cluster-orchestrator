# Project Agent Contract

## Start here

This file is the preloaded project context. Do not scan `README.md` or all of
`docs/` before routine work. Read touched source files first, then open only the
single routed document below when the task depends on details not stated here.
Do not browse for pinned versions; `versions.mk` is authoritative. Browse
official upstream sources only when explicitly upgrading a pin or introducing
a new external API.

When using the shell, prefix every command with `rtk`. Do not add `rtk` to
Makefile recipes or project scripts.

## Current snapshot

- Phases 0 through 3 are complete. Phase 4 fault recovery is next.
- Delivery status and task IDs are authoritative in `docs/progress.md`.
- Pins: Go 1.26.7, Kubernetes 1.35.6, Slurm 25.11.7, OpenTPU
  `ca5d381c93752a504e8de1fa10c9d51649853c70`.
- Module prefix: `github.com/varrahan/hetero-cluster-orchestrater/src`.
- Three Go modules are joined by `go.work` but must build independently.
- Preserve unrelated work; the repository may already be dirty.

## System contract

- Kubernetes DRA is authoritative for hardware inventory and allocation.
- Slurm is authoritative for jobs, queues, partitions, and dynamic nodes.
- MinIO stores checkpoint bytes; only a validated `.complete` object commits a
  checkpoint.
- `HeterogeneousCluster` is the only project workload/configuration CRD. Native
  Slurm remains the job submission API.
- DRA allocation is translated one-way into Slurm node/GRES configuration.
  Slurm never mutates claims, and a worker never registers on incomplete or
  mismatched allocation data.
- Kubernetes health drives Slurm drain/requeue. Recovery is implemented only
  after checkpoint resume works.

Do not introduce a custom scheduler, workload CRD, checkpoint database,
bespoke health framework, GPU sharing, physical TPU support, or project-managed
MariaDB/MinIO. Use the default Kubernetes scheduler, Node Problem Detector,
external MariaDB/MinIO, and OpenTPU simulation.

## Ownership and dependency direction

- `src/slurm-operator`: CRD, Slurm control plane, elastic workers, and recovery
  controllers.
- `src/dra-driver`: one driver/image with NVIDIA, CPU, NUMA-memory, and OpenTPU
  providers.
- `src/slurm-compute-node`: `gres-init` plus the worker image.
- `src/python-workloads`: the reference PyRTL adapter, not Go internals.
- `src/manifests`: generated CRDs/RBAC and Kustomize deployment resources.

Add `shared`, `quantization-engine`, `watchdog-daemon`, `checkpoint-flusher`,
and PyTorch adapter code only when their implementation phase uses them.

Deployable modules may import `shared`; they do not import one another. Use
Kubernetes/Slurm APIs, Unix sockets, or versioned files across processes.

## Implementation order

0. Foundation — complete.
1. Slurm control plane — complete.
2. Allocation and elastic workers: CPU/NUMA first, then NVIDIA, OpenTPU, mixed.
3. Checkpoint v2 and restart.
4. Fault isolation, drain, requeue, reboot, and verification.
5. Security, observability, fault injection, upgrade, and release hardening.

Implement only the active phase and the smallest complete vertical slice.
Create packages and add dependencies only when that slice uses them.

### Completed Phase 1 exit contract

Implement the `HeterogeneousCluster` API and leader-elected operator managing
two RWX-backed `slurmctld` instances, replicated `slurmrestd`, one `slurmdbd`,
MUNGE-enabled login, MUNGE/JWT Secrets, external MariaDB accounting, configless
operation, dynamic-node partitions, and `select/cons_tres`. MUNGE material is
limited to Slurm daemons; JWT signing material is limited to the REST trust
boundary.

Phase 1 exits when native `sbatch`/`squeue`, accounting, REST polling,
controller failover, and operator-restart reconciliation work without compute
workers.

## Working rules

- Prefer standard library, native Kubernetes/Slurm behavior, and existing code.
- No speculative interfaces, factories, packages, dependencies, or config.
- Fix shared root causes after checking every caller with `rg`.
- Fail closed at resource, topology, checkpoint, path, and authentication
  boundaries. Never log Secret or tensor contents.
- Generate CRDs, deepcopy code, and RBAC with `make generate`; do not hand-edit
  generated output.
- Keep Go `*_test.go` files under `src/test/`, mirroring their production path
  after `src/`; never place tests beside implementation files. `make test`
  stages the mirrored files into disposable module copies before running them.
- Update the relevant doc when changing a public contract. Update this file
  only when its stable snapshot, boundaries, phase, or commands change.
- Update `docs/progress.md` only after the relevant implementation and checks
  pass; partial work remains unchecked.

## Commands

- `make versions`: print authoritative pins.
- `make build`: build three binaries under ignored `bin/`.
- `make test`: test every module independently from centralized `src/test`
  sources, plus Python syntax and unit tests.
- `make generate`: run all generators.
- `make manifests`: render Kustomize without applying it.
- `make phase1-e2e`: run the disposable kind-based Phase 1 exit gates.
- `make verify`: version, drift, format, build, test, vet, JSON, and manifest
  checks. Run it before handoff.

Use the narrowest targeted test while iterating, then `make verify` once.

## Documentation routing

- Delivery status and the active phase checklist: `docs/progress.md`.
- Public API, sources of truth, security boundaries: `docs/architecture.md`.
- Directory/module ownership: `docs/repository-layout.md`.
- DRA, NUMA, claims, GRES translation: `docs/scheduling-and-resources.md`.
- Manifest v2, MinIO commit/restore, IPC: `docs/checkpointing.md`.
- Conditions, taints, drain/requeue/reboot: `docs/failure-recovery.md`.
- Phase scope and acceptance tests: `docs/implementation-plan.md`.

For implementation work, read only the active phase section in
`docs/progress.md`. Open none of the design docs by default. Use `rg` for the
needed heading and read only that section unless the task changes the
document's overall contract.
