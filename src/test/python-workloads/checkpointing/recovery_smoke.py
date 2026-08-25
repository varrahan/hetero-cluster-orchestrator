"""Phase 4 save-on-signal and same-job restore gate."""

from __future__ import annotations

import os
import time

from checkpointing.client import CheckpointClient
from checkpointing.lifecycle import CheckpointLifecycle
from checkpointing.smoke import compatibility, manifest, optimizer, rng, values


def checkpoint_manifest(run: str, step: int) -> dict[str, object]:
    value = manifest(run, step)
    value["world"] = {"size": 1, "ranks": [{"rank": 0, "hardware": "cpu", "adapter": "cpu-raw"}]}
    del value["tensors"]["opentpu.state"]  # type: ignore[index]
    del value["state"]["rng"]["1"]  # type: ignore[index]
    value["metadata"]["adapter_versions"] = {"cpu": "1.0.0"}  # type: ignore[index]
    return value


def save(client: CheckpointClient, run: str, step: int) -> None:
    model, _ = values(step)
    client.upload(run, step, "model_weight", "shards/model_weight.bin", model)
    client.upload(run, step, "optimizer_metadata", "optimizer_state.json", optimizer(step))
    client.upload(run, step, "rng_0", "rng/rank_00000.bin", rng(0, step))
    client.commit(run, step, checkpoint_manifest(run, step))


def restore(client: CheckpointClient, run: str) -> None:
    latest = client.latest(run, compatibility("cpu"))["manifest"]
    if latest["global_step"] != 5:
        raise RuntimeError(f"restored step {latest['global_step']}, expected 5")
    model, _ = values(5)
    if client.restore(run, 5, "model_weight", len(model)) != model:
        raise RuntimeError("model tensor changed across recovery")
    if client.restore(run, 5, "optimizer_metadata", len(optimizer(5))) != optimizer(5):
        raise RuntimeError("optimizer state changed across recovery")
    if client.restore(run, 5, "rng_0", 16) != rng(0, 5):
        raise RuntimeError("RNG state changed across recovery")


def main() -> None:
    job = int(os.environ["SLURM_JOB_ID"])
    run = f"phase4_{job}"
    client = CheckpointClient(job_id=job, rank=0)
    if int(os.environ.get("SLURM_RESTART_COUNT", "0")) > 0:
        restore(client, run)
        print("RECOVERY_RESTORED=5", flush=True)
        return
    save(client, run, 0)
    with CheckpointLifecycle(interval_seconds=3600) as lifecycle:
        while True:
            if lifecycle.checkpoint_if_due(5, lambda step, _reason: save(client, run, step)):
                time.sleep(300)
            time.sleep(0.2)


if __name__ == "__main__":
    main()
