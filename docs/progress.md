# Implementation Progress

This file is the authoritative delivery checklist. Design behavior remains
authoritative in the linked architecture documents; this file records only
whether that behavior has been implemented and verified.

## Tracking rules

- Mark an item `[x]` only after its implementation and relevant checks pass.
- Leave partial work unchecked and add a short indented note only when another
  agent needs it to resume safely.
- A phase is complete only when every required task and exit gate in that phase
  is checked. Optional tasks are explicitly labeled and never block a phase.
- Update the phase summary only when a phase starts, becomes blocked, or passes
  all exit gates. Git history records who and when.
- Run the narrowest relevant test while working and `make verify` before
  marking an implementation item complete.

## Phase summary

| Phase | Status |
| --- | --- |
| 0 — Foundation | Complete |
| 1 — Slurm control plane | Complete |
| 2 — Resource allocation and elastic workers | Not started |
| 3 — Checkpoint v2 | Not started |
| 4 — Fault recovery | Not started |
| 5 — Hardening and release | Not started |

## Phase 0 — Foundation

- [x] `P0-01` Pin Go, Kubernetes, Slurm, and OpenTPU in `versions.mk`.
- [x] `P0-02` Finalize the GitHub-backed Go module prefix for all six modules.
- [x] `P0-03` Join all modules in `go.work` while preserving independent builds.
- [x] `P0-04` Provide build, test, generate, manifest, format, vet, and verify
  Make targets.
- [x] `P0-05` Add compile-only entrypoints for all seven planned Go binaries.
- [x] `P0-06` Document architecture, ownership, phase order, and agent rules.

### Phase 0 exit gates

- [x] `P0-G01` `make build` succeeds.
- [x] `P0-G02` `make test` succeeds.
- [x] `P0-G03` `make generate` succeeds.
- [x] `P0-G04` `make manifests` succeeds without applying resources.
- [x] `P0-G05` `make verify` succeeds.

## Phase 1 — Slurm control plane

Design: [architecture](architecture.md) and
[implementation plan](implementation-plan.md#phase-1-slurm-control-plane).

### API and operator

- [x] `P1-01` Define and register the namespaced `HeterogeneousCluster`
  `v1alpha1` API.
- [x] `P1-02` Implement defaults and admission validation for unsafe or
  internally inconsistent configurations.
- [x] `P1-03` Implement status conditions and observed-generation reporting.
- [x] `P1-04` Generate deepcopy code, CRD manifests, and RBAC.
- [x] `P1-05` Start a leader-elected controller-runtime manager with health and
  readiness probes.
- [x] `P1-06` Reconcile owned resources idempotently across create, update,
  partial deletion, and operator restart.

### Slurm services and configuration

- [x] `P1-07` Render deterministic `slurm.conf`, `gres.conf` inputs, and
  configless service configuration with `select/cons_tres` and dynamic-node
  partitions.
- [x] `P1-08` Reconcile primary and backup `slurmctld` with stable identities,
  shared RWX state, required anti-affinity, and a PDB.
- [x] `P1-09` Reconcile two `slurmrestd` replicas behind a stable ClusterIP
  Service.
- [x] `P1-10` Reconcile one restartable `slurmdbd` connected to external
  MariaDB and initialize accounting safely.
- [x] `P1-11` Reconcile MUNGE-enabled login services supporting native Slurm
  clients.
- [x] `P1-12` Mount referenced MUNGE and JWT Secrets only inside their defined
  trust boundaries and prevent secret material from reaching workers or logs.
- [x] `P1-13` Implement the typed, least-privilege JWT `slurmrestd` client and
  pending-job polling needed by later phases.
- [x] `P1-14` Add deployable operator/control-plane manifests and least-privilege
  Phase 1 RBAC.

### Phase 1 tests and exit gates

- [x] `P1-T01` Test CRD defaults, validation, status, and Slurm configuration
  rendering.
- [x] `P1-T02` Test reconciliation create, update, restart, and partial deletion
  with envtest and fake Slurm/accounting endpoints.
- [x] `P1-G01` Native `sbatch` accepts a job and `squeue` observes it without
  compute workers.
- [x] `P1-G02` Accounting records and retrieves submitted job state.
- [x] `P1-G03` JWT REST polling observes the same pending job state.
- [x] `P1-G04` Backup `slurmctld` assumes control without losing pending jobs.
- [x] `P1-G05` Operator restart reconstructs desired state without duplicate or
  destructive actions.
- [x] `P1-G06` `make verify` succeeds with all generated files current.

## Phase 2 — Resource allocation and elastic workers

Design: [scheduling and resources](scheduling-and-resources.md) and
[implementation plan](implementation-plan.md#phase-2-resource-allocation-and-elastic-workers).

### Unified DRA driver

- [ ] `P2-01` Register one DRA driver/node plugin and recover prepared-device
  state after restart.
- [ ] `P2-02` Publish stable, qualified ResourceSlice identity and topology
  attributes shared by every provider.
- [ ] `P2-03` Discover exclusive physical CPU cores, reserve system headroom,
  and prepare CPU pinning.
- [ ] `P2-04` Publish fixed-size NUMA memory units, reserve system memory, and
  prepare the selected memory policy.
- [ ] `P2-05` Discover NVIDIA devices by stable UUID/topology and prepare CDI
  device/library injection.
- [ ] `P2-06` Publish configured OpenTPU simulation slots with explicit
  CPU/memory/shared-memory footprints.
- [ ] `P2-07` Implement idempotent prepare/unprepare with CDI/NRI state and
  fail-closed recovery.

### Demand, workers, and translation

- [ ] `P2-08` Poll Slurm demand every five seconds and preserve capacity when
  REST state is unavailable.
- [ ] `P2-09` Parse TRES/GRES, normalize names, map profiles, and compute demand
  deficits.
- [ ] `P2-10` Generate multi-request ResourceClaims with exact same-NUMA
  constraints.
- [ ] `P2-11` Reconcile Guaranteed-QoS worker Pods from allocated claims.
- [ ] `P2-12` Implement `gres-init` claim validation and deterministic Slurm
  CPU, memory, feature, GRES, and dynamic-node configuration.
- [ ] `P2-13` Start MUNGE-authenticated `slurmd -Z` only after complete claim,
  topology, CDI/NRI, and GRES validation.
- [ ] `P2-14` Support ordinary and Slurm heterogeneous jobs.
- [ ] `P2-15` Drain/delete idle Slurm nodes before Pod and claim cleanup.
- [ ] `P2-16` Adopt or clean orphaned workers safely after controller restart.
- [ ] `P2-O01` Optional: implement and test controller-side `cloud-burst`
  Resume/Suspend hooks without allowing direct Kubernetes mutation.

### Phase 2 tests and exit gates

- [ ] `P2-T01` Unit-test provider discovery, topology matching, TRES/GRES
  parsing, profile selection, and translation.
- [ ] `P2-T02` Integration-test claim exclusivity, prepare/unprepare, scale-up,
  scale-down, restart reconstruction, and registration failure.
- [ ] `P2-G01` CPU-only jobs receive exclusive CPU/memory on the requested NUMA
  node with no GRES.
- [ ] `P2-G02` RTX 4050 jobs receive the correct UUID/device files and execute a
  CUDA kernel.
- [ ] `P2-G03` OpenTPU jobs receive the configured simulator profile and exact
  footprint.
- [ ] `P2-G04` Mixed heterogeneous jobs become runnable together.
- [ ] `P2-G05` No-exact-NUMA-fit demand remains queued without relaxed
  placement.
- [ ] `P2-G06` Claim/GRES mismatch prevents worker registration observably.
- [ ] `P2-G07` Slurm never advertises a device without a live allocation.
- [ ] `P2-G08` Idle timeout follows the required drain/delete/claim order.
- [ ] `P2-G09` `make verify` succeeds.

## Phase 3 — Checkpoint v2

Design: [checkpointing](checkpointing.md) and
[manifest schema](schemas/checkpoint-manifest-v2.schema.json).

### Checkpoint implementation

- [ ] `P3-01` Implement manifest v2 Go types plus JSON Schema and semantic
  validation for paths, bounds, shapes, dtypes, byte ranges, hashes, and
  compatibility.
- [ ] `P3-02` Stream immutable raw `.bin` shards to and from MinIO with hashes
  and validated object paths.
- [ ] `P3-03` Implement conditional `.complete` commit creation so partial or
  conflicting uploads never become visible.
- [ ] `P3-04` Implement newest-compatible committed checkpoint discovery and
  restore.
- [ ] `P3-05` Implement the bounded, namespaced worker-local IPC/shared-memory
  contract.
- [ ] `P3-06` Implement `checkpoint-flusher` save, commit, restore, and cleanup
  Unix APIs.
- [ ] `P3-07` Implement the quantization-engine versioned Unix protocol and
  bounded FP32/BF16 to OpenTPU INT8 conversion.
- [ ] `P3-08` Implement thin PyTorch, CPU, and PyRTL checkpoint adapters.
- [ ] `P3-09` Save and restore model, optimizer, scheduler, RNG, and data-cursor
  state.
- [ ] `P3-10` Provide the batch wrapper for step-zero, periodic, and `USR1`
  checkpoints, `--requeue`, and pre-work restore.

### Phase 3 tests and exit gates

- [ ] `P3-T01` Test schema, semantic coverage, IPC bounds/ownership, hashing,
  conditional commit, and compatibility selection.
- [ ] `P3-G01` A distributed heterogeneous job resumes on different healthy
  workers with matching tensors and training state.
- [ ] `P3-G02` Killing a rank during upload leaves the new step invisible and
  restores the previous commit.
- [ ] `P3-G03` Requeued work resumes after the last checkpoint instead of step
  zero.
- [ ] `P3-G04` `make verify` succeeds.

## Phase 4 — Fault recovery

Design: [failure recovery](failure-recovery.md).

### Detection and recovery

- [ ] `P4-01` Deploy Node Problem Detector NVIDIA, CPU, and memory probes that
  publish only the raw `HardwareFaultDetected` condition.
- [ ] `P4-02` Implement operator-owned `HardwareDegraded`, the canonical
  NoSchedule taint, recovery annotations, and incident UUID state.
- [ ] `P4-03` Quarantine a degraded physical node before taking Slurm actions.
- [ ] `P4-04` Drain affected dynamic Slurm nodes and identify only jobs that
  used them.
- [ ] `P4-05` Enforce checkpoint grace, requeue affected jobs, and leave
  unrelated jobs running.
- [ ] `P4-06` Clean up affected dynamic nodes, worker Pods, and claims in safe
  order.
- [ ] `P4-07` Implement watchdog fixed inventory, health-support, reboot, and
  verification operations scoped to its own Node.
- [ ] `P4-08` Enforce one automatic reboot per incident and confirm it through
  changed Kubernetes `bootID`.
- [ ] `P4-09` Compare inventory and run CPU, memory, NVIDIA, and OpenTPU
  verification micro-workloads.
- [ ] `P4-10` Untaint only after every verifier succeeds; otherwise retain
  quarantine.
- [ ] `P4-11` Resume the same recovery incident idempotently after operator
  restart.
- [ ] `P4-12` Disable reboot, destructive cleanup, and inferred recovery when
  Slurm REST is unavailable.

### Phase 4 tests and exit gates

- [ ] `P4-T01` Unit-test recovery transitions, incident idempotency,
  affected-job selection, and the one-reboot limit.
- [ ] `P4-T02` Prove quarantined nodes admit only NPD, watchdog, and verifier
  Pods.
- [ ] `P4-G01` A single-node fault requeues only affected jobs and resumes them
  from MinIO on healthy capacity.
- [ ] `P4-G02` Successful verification returns the node to service.
- [ ] `P4-G03` Failed verification leaves the node degraded and unschedulable.
- [ ] `P4-G04` Operator restart mid-recovery causes no duplicate actions.
- [ ] `P4-G05` `make verify` succeeds.

## Phase 5 — Hardening and release

### Security, resilience, and operations

- [ ] `P5-01` Enforce least-privilege RBAC, Secret access, network policies,
  TLS, checkpoint path validation, and image digests.
- [ ] `P5-02` Implement reconciliation backoff and safe Kubernetes, Slurm REST,
  MinIO, MariaDB, and network outage behavior.
- [ ] `P5-03` Complete orphan adoption/cleanup and upgrade/rollback tests.
- [ ] `P5-04` Publish database migration and RWX backup/restore guidance.
- [ ] `P5-05` Emit attributable events/metrics by cluster, pool, node, job, and
  incident without logging secrets or tensor data.
- [ ] `P5-06` Publish dashboards and alerts for control plane, demand,
  allocation, worker startup, GRES mismatch, checkpoint, and recovery health.
- [ ] `P5-07` Expose newest committed checkpoint age without listing raw object
  contents.
- [ ] `P5-08` Provide a runbook naming the authoritative object for every
  blocked phase.
- [ ] `P5-09` Validate all generated examples against installed CRDs and the
  checkpoint schema.

### Physical acceptance matrix

- [ ] `P5-A01` CPU-only job.
- [ ] `P5-A02` RTX 4050 job.
- [ ] `P5-A03` OpenTPU simulation job.
- [ ] `P5-A04` Mixed heterogeneous job.
- [ ] `P5-A05` No exact NUMA fit.
- [ ] `P5-A06` Claim/GRES mismatch.
- [ ] `P5-A07` Idle timeout cleanup.
- [ ] `P5-A08` Primary controller loss.
- [ ] `P5-A09` Rank killed during checkpoint save.
- [ ] `P5-A10` Worker physical-node fault.
- [ ] `P5-A11` Successful reboot and verification.
- [ ] `P5-A12` Failed verification quarantine.
- [ ] `P5-A13` Operator restart during recovery.

### Phase 5 exit gates

- [ ] `P5-G01` Controller, node, MinIO, MariaDB, and network fault injection
  passes.
- [ ] `P5-G02` The complete physical acceptance matrix passes twice
  consecutively on the target cluster.
- [ ] `P5-G03` Rollback to the prior operator version preserves Slurm state,
  claims, checkpoints, and quarantines.
- [ ] `P5-G04` `make verify` succeeds and release documentation is current.
