# Scheduling and Resources

## Scheduling boundary

The platform does not fork or replace `kube-scheduler`. The operator converts
eligible Slurm demand into Pods and stable DRA `ResourceClaim`s; the default
scheduler performs node and device allocation. The CPU and NUMA-memory
providers make cores and discrete memory units selectable devices so all
resources can be matched before a worker starts.

NVIDIA, CPU, NUMA-memory, and OpenTPU behavior ships in one multi-provider
`dra-driver` binary and node-plugin image. Providers share the same lifecycle
and topology attributes; they are not independently deployed drivers in v1.

This design uses the stable `resource.k8s.io/v1` core. It does not depend on
device taints, consumable capacity, partitionable devices, resource health, or
DRA-managed node-allocatable resources because those features are not stable in
the target baseline. See the Kubernetes
[DRA feature documentation](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/).

## Dedicated compute nodes

Strict worker pools select nodes labeled
`orchestration.gputpu.io/compute-node=true`. Those nodes carry the taint
`orchestration.gputpu.io/dedicated=compute:NoSchedule`; only platform agents,
worker Pods, and verifier Jobs tolerate it.

The CPU and memory providers reserve configurable system headroom before
publishing devices. The default memory device is one exclusive 1 GiB unit.
Large hosts may choose a larger unit to reduce the number of published devices,
trading allocation granularity for lower API volume.

Every provider publishes the shared scalar attribute
`orchestration.gputpu.io/numa-node`. GPU devices additionally publish UUID,
normalized model, VRAM, PCI address, and PCIe root. CPU devices publish core and
socket IDs. Memory devices publish their unit size. OpenTPU simulation slots
publish profile, matrix size, buffer sizes, and required runtime version.

## Exact placement

A worker uses one multi-request claim:

1. request the required number of individual CPU devices;
2. request `ceil(job memory / memoryUnit)` memory devices;
3. optionally request NVIDIA GPU or OpenTPU simulation devices; and
4. apply `matchAttribute: orchestration.gputpu.io/numa-node` across every
   request.

The worker Pod mirrors allocated CPU and rounded memory as equal Kubernetes
requests and limits. CDI/NRI applies the selected CPU set and NUMA memory
policy. Because each worker represents one NUMA cell and unrelated Pods are
excluded, the claim is both the reservation and the binding authority.

If a request exceeds one NUMA cell, the operator does not silently relax the
policy. It leaves the Slurm component pending with a topology-unsatisfied reason.
A future worker pool may expose a separate, explicitly best-effort partition.

## Demand to worker flow

```mermaid
sequenceDiagram
    participant S as slurmctld/slurmrestd
    participant O as Slurm operator
    participant K as Kubernetes scheduler
    participant D as Multi-provider DRA driver
    participant W as Worker Pod

    O->>S: Poll eligible PENDING jobs
    S-->>O: TRES/GRES per job component
    O->>O: Subtract ready and provisioning capacity
    O->>K: Create claim and Pod for deficit
    K->>D: Allocate matching devices on one node/NUMA cell
    D-->>W: Prepare CDI/NRI resources
    W->>W: Translate claim to Slurm config
    W->>S: Register dynamic node with slurmd -Z
    S->>W: Dispatch queued job
```

The five-second poll considers only jobs that are pending, eligible to run, and
waiting for resources. Priority-, dependency-, reservation-, account-, or QOS-
blocked jobs do not create workers. For a heterogeneous Slurm job, each
component is planned independently but capacity is created for the complete
co-scheduled request. Slurm natively represents these components; see
[heterogeneous job support](https://slurm.schedmd.com/heterogeneous_jobs.html).

For each resource profile, the operator computes:

```text
deficit = eligible demand - ready idle capacity - capacity already provisioning
```

It creates at most the pool's remaining `maxWorkers`. `minReady` defaults to
zero so scarce GPUs are not claimed speculatively. Administrators may configure
a warm pool for measured startup-latency needs.

The operator poll is the v1 elasticity trigger. The `cloud-burst` binary is an
optional adapter for Slurm `ResumeProgram` and `SuspendProgram`; when enabled it
runs beside `slurmctld` and sends idempotent requests to the operator. It never
uses Kubernetes credentials or mutates claims and Pods directly.

## Worker startup

The operator creates the claim and its referencing Pod together. It constrains
the Pod with node affinity but never writes `spec.nodeName`, allowing normal
DRA scheduling and reservation.

The `gres-init` init container then:

1. reads its Pod and allocated claim through a read-only service account;
2. resolves each allocation back to its authoritative `ResourceSlice`;
3. enumerates resources visible inside the prepared container;
4. verifies driver, device UUID, model, NUMA cell, count, and device path;
5. writes `gres.conf` and a dynamic-node configuration into an `emptyDir`;
6. runs `slurmd -G` as a validation step; and
7. exits successfully only when the DRA and Slurm views agree.

After `gres-init` succeeds, MUNGE starts and the main container registers with a
unique Pod-derived hostname and explicit Pod IP:

```text
slurmd -Z --conf "CPUs=8 Boards=1 SocketsPerBoard=1 CoresPerSocket=8 ThreadsPerCore=1 RealMemory=15360 Gres=gpu:rtx_4050:1 Feature=numa-0"
```

Presenting the claimed NUMA cell as one Slurm socket keeps GRES affinity within
Slurm's socket-level scheduling rules.

## DRA-to-Slurm translation

| DRA allocation | `gres.conf` / dynamic node | Validation |
| --- | --- | --- |
| NVIDIA RTX 4050 | `Name=gpu Type=rtx_4050 File=/dev/nvidia0 Cores=0-7` and `Gres=gpu:rtx_4050:1` | UUID and device path agree with NVML; `slurmd -G` passes |
| OpenTPU M8 simulation slot | `Name=tpu Type=opentpu_m8 Count=1 Flags=CountOnly` and `Gres=tpu:opentpu_m8:1` | Injected profile and runtime version match the claim |
| CPU cores | `CPUs`, socket/core fields, and feature `numa-<id>` | Effective process affinity equals claimed core IDs |
| Memory units | `RealMemory` in MiB after configured Slurm headroom | Pod limit, DRA units, and NUMA memory policy agree |

GRES types and models use lowercase ASCII with punctuation normalized to
underscores. A profile maps exactly one Slurm GRES name/type to one DRA
`DeviceClass`; aliases and fallback models are rejected in v1. Slurm requires
GPU GRES to be declared explicitly and can drain nodes on a detected mismatch,
so translation fails closed. See the Slurm
[GRES guide](https://slurm.schedmd.com/gres.html).

CPU is not represented as GRES. OpenTPU is `CountOnly` because it is a software
slot without a physical device file; Pod cgroups and DRA claims enforce its CPU
and memory isolation.

## Idempotency and cleanup

- Claim, Pod, and dynamic Slurm node names contain the owning cluster, pool, and
  Pod UID. Reconciliation adopts only resources carrying the matching owner UID.
- A `gres-init` failure leaves no registered Slurm node. The operator records
  the reason, deletes the failed Pod and claim, and retries with bounded
  exponential backoff.
- A ready worker becomes scale-down eligible only after it has been Slurm
  `IDLE`, unreserved, and above `minReady` for five minutes.
- Scale-down drains the dynamic node, confirms it has no jobs or reservations,
  deletes it through Slurm, then deletes the Pod and claim.
- An unreachable Slurm API disables scale-down and destructive cleanup.
- An operator restart reconstructs capacity from owned Pods, claims, and Slurm
  dynamic nodes before creating or deleting anything.

## Scheduling acceptance rules

A resource path is correct only when all of these hold:

- two concurrent claims never receive the same CPU, memory unit, GPU, or
  OpenTPU slot;
- all resources in a strict worker share one NUMA attribute;
- the worker's cgroup and memory policy match the allocation;
- Slurm advertises no resource absent from the claim;
- a claim or GRES mismatch prevents registration;
- a heterogeneous job receives every component or remains queued; and
- no new worker schedules onto a quarantined physical node.
