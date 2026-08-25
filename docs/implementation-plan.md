# Implementation Plan

Delivery status is tracked in [Implementation Progress](progress.md). This
document defines scope and acceptance behavior, not completion state.

## Delivery principles

Build the smallest complete vertical slices. Kubernetes remains responsible for
allocation, Slurm remains responsible for jobs, and MinIO remains responsible
for checkpoint bytes. Do not add a custom scheduler, workload CRD, checkpoint
database, or bespoke node-health framework.

Kubernetes-facing components are Go binaries using `controller-runtime` and the
upstream DRA kubelet-plugin libraries. `checkpoint-flusher` and the node-wide
quantization engine are Go. PyTorch and PyRTL adapters stay thin and
framework-native. Node Problem Detector publishes the raw health condition;
this project adds probe configuration and the narrowly scoped
`watchdog-daemon`.

The root `go.work` ties together the active Go modules in the
[repository layout](repository-layout.md), and the root `Makefile` delegates
build, test, generation, manifest rendering, and verification without hiding
module-specific commands. Add packages only when their phase needs them.

## Phase 0: Foundation

The completed foundation pins Go 1.26.7, Kubernetes 1.35.6, Slurm 26.05.3,
and OpenTPU revision `ca5d381c93752a504e8de1fa10c9d51649853c70` in
`versions.mk`. All Go modules use the permanent
`github.com/varrahan/hetero-cluster-orchestrater/src` prefix and build and test
independently through the root Makefile.

Exit criteria: `make build`, `make test`, `make generate`, `make manifests`,
and `make verify` succeed from a clean checkout without implementing component
behavior.

## Phase 1: Slurm control plane

Implement the `HeterogeneousCluster` CRD, validation, status, and leader-elected
operator. Reconcile:

- primary and backup `slurmctld` Pods with stable identities, anti-affinity,
  PDB, configless mode, and shared RWX state;
- replicated `slurmrestd`, one `slurmdbd`, and MUNGE-enabled login services;
- MUNGE and JWT Secrets without exposing signing material to workers;
- external MariaDB connectivity and accounting initialization; and
- partitions configured for dynamic nodes and `select/cons_tres`.

Exit when native `sbatch`, `squeue`, accounting, REST polling, controller
failover, and reconciliation after an operator restart all work without compute
workers.

## Phase 2: Resource allocation and elastic workers

Implement NVIDIA, CPU, NUMA-memory, and OpenTPU simulation providers in the one
`dra-driver` module and image. Publish stable `ResourceSlice` attributes,
reserve CPU and fixed memory devices, prepare CDI/NRI state, and exclude system
headroom.

Add pending-demand polling, resource-profile mapping, multi-request claim
generation, worker Pods, `gres-init`, and dynamic-node registration. Support
ordinary and heterogeneous Slurm jobs. Add safe idle scale-down and
reconstruction across controller restarts. Use five-second operator polling as
the elasticity trigger.

Exit when CPU-only, RTX 4050, OpenTPU simulation, and mixed heterogeneous jobs
receive exact same-NUMA resources; Slurm never advertises an unclaimed device;
and an injected claim/GRES mismatch prevents registration.

## Phase 3: Checkpoint v2

Implement the shared checkpoint schema/validation package,
`checkpoint-flusher` Unix API, immutable MinIO upload/restore path, conditional
commit marker, and PyTorch, CPU, and PyRTL adapters. Implement the node-wide
`quantization-engine` for OpenTPU conversion over bounded shared-memory buffers.
Include model, optimizer, scheduler, RNG, and data-cursor state.

Provide a batch-script wrapper that installs periodic and `USR1` checkpoint
handling, commits step zero, uses `--requeue`, and restores the newest compatible
checkpoint before work begins.

Exit when a distributed heterogeneous job can save, stop, change healthy worker
placement, and resume with matching tensors and training state. Killing a rank
mid-upload must leave the new step invisible and restore the previous commit.

## Phase 4: Fault recovery

Deploy Node Problem Detector with NVIDIA, CPU, and memory probes. Implement the
canonical Node condition/taint controller and `watchdog-daemon`. Add the isolate,
drain, checkpoint-grace, affected-job requeue, dynamic-node cleanup, single
reboot, inventory comparison, and verifier workflow.

Exit when a fault injected into one node requeues only jobs that used that node,
leaves unrelated jobs running, restarts affected work from MinIO on healthy
capacity, and either returns the repaired node after all checks or keeps it
quarantined after a failed verifier.

## Phase 5: Hardening and release

- Enforce least-privilege RBAC, Secret permissions, network policies, TLS,
  checkpoint path validation, and image pinning by digest.
- Add reconciliation backoff, API outage behavior, orphan adoption/cleanup,
  upgrade tests, database migration guidance, and RWX backup/restore guidance.
- Publish dashboards and alerts for control-plane readiness, pending demand,
  allocation latency, worker startup, GRES mismatch, checkpoint age/duration,
  recovery phase, reboot count, and verifier outcome.
- Run controller, node, MinIO, MariaDB, and network fault injection before
  labeling a release production-ready.

Exit when the complete acceptance matrix passes twice consecutively on the
physical test cluster and a rollback to the prior operator version preserves
Slurm state, claims, checkpoints, and quarantines.

## Test strategy

### Unit and contract tests

- CRD defaulting and rejection of unsafe configurations.
- Slurm TRES/GRES parsing, profile selection, demand deficit, and name
  normalization.
- ResourceSlice-to-GRES translation, topology matching, and fail-closed device
  validation.
- Checkpoint schema plus semantic tensor coverage, bounds, dtype-size, hash, and
  compatibility checks.
- Recovery state transitions, incident idempotency, affected-job selection, and
  one-reboot limit.

### Kubernetes integration tests

- Reconcile create, update, operator restart, and partial deletion using
  `envtest` and fake Slurm/MinIO endpoints.
- Run mock DRA providers in a multi-node test cluster for claim exclusivity,
  scheduling, `gres-init` failure, scale-up, and scale-down.
- Prove a quarantined node admits only the required DRA/NPD/watchdog node
  services and verifier Pods, never elastic workers or ordinary workloads.
- Prove loss of Slurm REST disables reboot and destructive cleanup.

### Physical acceptance matrix

| Scenario | Expected result |
| --- | --- |
| CPU-only job | Exclusive CPU/memory units, correct NUMA policy, no GRES |
| RTX 4050 job | Correct UUID/device file, `gpu:rtx_4050`, successful CUDA kernel |
| OpenTPU job | Profile-sized CPU/memory claim, `tpu:opentpu_<profile>`, correct simulation |
| Heterogeneous job | All components become runnable together and communicate |
| No exact NUMA fit | Job remains queued; placement is never relaxed |
| Claim/GRES mismatch | Worker never registers and the reason is observable |
| Idle timeout | Slurm node drains/deletes before Pod and claim cleanup |
| Primary controller loss | Backup assumes control without losing pending jobs |
| Rank killed during save | No commit marker; previous checkpoint restores |
| Worker node fault | Only affected jobs requeue; unrelated jobs continue |
| Successful reboot | Inventory and micro-workloads pass before untaint |
| Failed verification | Node stays degraded and no worker schedules there |
| Operator restart mid-recovery | Same incident resumes without duplicate actions |

## Operational acceptance

- Every reconciliation and recovery action is attributable by cluster, pool,
  node, job, and incident UUID.
- No controller log or Event contains Slurm JWT, MariaDB password, MinIO secret,
  or checkpoint tensor data.
- The newest committed checkpoint and its age are visible without listing raw
  object contents.
- A runbook identifies the authoritative object to inspect for every blocked
  phase.
- All generated examples validate against installed CRDs and the checkpoint
  JSON Schema.

## Known ceilings

- Fixed-size memory devices make stable exact NUMA allocation possible but can
  produce many DRA devices. Increase `memoryUnit` when API volume is measured as
  a problem.
- Node-granular quarantine sacrifices healthy devices on a partly degraded host.
  Revisit device taints only after the required Kubernetes APIs are stable.
- Worker-wide MinIO credentials assume one trusted administrative tenant. Add a
  per-job credential broker before accepting mutually untrusted Slurm users.
- OpenTPU compatibility covers its PyRTL simulation and affine INT8 adapter, not
  physical TPU execution or arbitrary TPU frameworks.
- Application checkpoints cover only workloads using the v2 adapter contract;
  unsupported jobs may requeue but cannot resume saved computation.

## Primary references

- Kubernetes [Dynamic Resource Allocation](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/)
- Kubernetes [Topology Manager](https://kubernetes.io/docs/tasks/administer-cluster/topology-manager/)
- Kubernetes [Node Problem Detector](https://github.com/kubernetes/node-problem-detector)
- Slurm [dynamic nodes](https://slurm.schedmd.com/dynamic_nodes.html)
- Slurm [GRES](https://slurm.schedmd.com/gres.html)
- Slurm [high availability](https://slurm.schedmd.com/quickstart_admin.html#HA)
- Slurm [heterogeneous jobs](https://slurm.schedmd.com/heterogeneous_jobs.html)
- MinIO [S3 API compatibility](https://docs.min.io/aistor/developers/s3-api-compatibility/)
- UCSB ArchLab [OpenTPU](https://github.com/UCSBarchlab/OpenTPU)
