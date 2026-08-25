# Operations and release runbook

This runbook starts with the authoritative object. Do not repair a derived
view before its source of truth is healthy.

| Blocked phase | Authoritative object or API | First checks |
| --- | --- | --- |
| Control plane | `HeterogeneousCluster` and the RWX Slurm state PVC | Cluster conditions, `slurmctld` StatefulSet, PVC mount, Events |
| Accounting | External MariaDB through native `slurmdbd` | `AccountingReady`, `slurmdbd` logs, database connectivity |
| Pending demand | Native Slurm job | `squeue`, partition/reason, `gputpu_pending_job_age_seconds` |
| Hardware inventory | DRA `ResourceSlice` | Slice generation and the owning compute Node annotations |
| Allocation | DRA `ResourceClaim.status.allocation` | Claim Events and exact CPU, memory, NUMA, GPU, or OpenTPU result |
| Worker startup | Operator-owned worker Pod | Claim reference, init-container status, node placement |
| Slurm registration | Native Slurm dynamic node | `scontrol show node`, generated GRES, Pod readiness |
| Checkpoint upload | MinIO objects below `checkpoints/` | Flusher logs and bounded object metadata; never inspect tensor bytes |
| Checkpoint commit | Validated `step_*.complete` object | `CheckpointStoreReady` and `gputpu_checkpoint_age_seconds` |
| Fault isolation | Kubernetes Node condition and taint | `HardwareFaultDetected`, `HardwareDegraded`, recovery incident annotation |
| Recovery progress | Incident ConfigMap in `slurm-system` | ConfigMap labels, Node recovery phase, incident Events |
| Reboot | Node request/ack annotations and `status.nodeInfo.bootID` | One request and ack for the incident, then a changed boot ID |
| Verification | Incident-owned verifier Jobs and claims | Per-NUMA Job result; a failure remains in `ManualRepair` |

## Outages

- Kubernetes API errors use controller-runtime workqueue backoff. Existing
  Slurm workers and claims are retained until authoritative state is readable.
- Slurm REST or JWT outages mark the cluster unavailable and retry every 30
  seconds. Recovery leaves the Node isolated and performs no inferred drain,
  requeue, reboot, or cleanup.
- MariaDB outages set `AccountingReady=False`; Slurm scheduling continues and
  accounting reconnects without recreating the control plane.
- MinIO and network errors use bounded TLS requests and retries. The last known
  committed-checkpoint time is retained, `CheckpointStoreReady=False`, and no
  incomplete upload is treated as committed.

## MariaDB migration

MariaDB remains externally managed. Before changing its server or schema:

1. Stop new submissions, wait for important jobs and checkpoint commits, and
   record `sacct` totals.
2. Take a native consistent backup, for example with `mariadb-dump
   --single-transaction --routines --events`, plus the database server's normal
   physical backup.
3. Restore into a staging database at the target version and point a disposable
   `slurmdbd` of the pinned Slurm version at it. Compare jobs, associations,
   TRES, and rollups before production cutover.
4. Cut over the Secret endpoint, wait for `AccountingReady=True`, and compare
   `sacct` again. Keep the old database read-only until the rollback window
   closes.

Rollback changes only the database Secret endpoint. Never run two writable
`slurmdbd` instances against divergent copies.

## RWX state backup and restore

Use the storage provider's snapshot or backup mechanism. For a consistent cold
copy, stop submissions, scale the operator to zero, scale the
`<cluster>-slurmctld` StatefulSet to zero, snapshot the state PVC, then restore
the StatefulSet and operator. Verify controller failover and existing jobs.

Restore the snapshot into the same retained PVC. `stateSaveClaim` is immutable;
a replacement PVC requires a new `HeterogeneousCluster` created with that claim
after the old cluster is quiesced. MariaDB and MinIO are backed up separately.

## Monitoring

Import the `slurm-operator-dashboard` and `slurm-operator-alerts` ConfigMaps into
the site's existing Grafana and Prometheus configuration. Prometheus scrapes
`slurm-operator-metrics.slurm-system.svc:8080` from a namespace named
`monitoring`. Metrics contain names, IDs, phases, counts, and durations only;
Secrets and tensor contents are never labels or log fields.

## Release and rollback

1. Run `make verify` and `PRIOR_OPERATOR_IMAGE=<previous image> make phase5-e2e`.
2. Build and push the operator, worker/control-plane, DRA, quantization, and
   watchdog images. Record registry-produced `@sha256:` references.
3. Set `OPERATOR_IMAGE`, `WORKER_IMAGE`, `DRA_IMAGE`, `QUANTIZATION_IMAGE`,
   `WATCHDOG_IMAGE`, and `SLURM_CONTROL_PLANE_IMAGE`, then run
   `make release-manifests`. The command rejects mutable image references.
4. Apply the rendered platform manifests and set
   `spec.controlPlane.controllers.image` to the validated control-plane digest.
5. Upgrade one cluster, verify conditions, native `squeue`/`sacct`, claims,
   checkpoint age, and any active recovery incident before continuing.

To roll back, set the operator Deployment to the previous digest only. Do not
delete the CR, StatefulSet, PVC, ResourceClaims, MinIO objects, or recovery
ConfigMaps. The Phase 5 rollback gate verifies those objects and quarantines
retain their identities, then restores the candidate operator.
