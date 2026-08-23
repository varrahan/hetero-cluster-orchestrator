"""Phase 3 heterogeneous save, interrupted upload, and restore gate."""

from __future__ import annotations

import hashlib
import json
import os
import struct
import threading
import time

from checkpointing.client import CheckpointClient
from checkpointing.ring import Ring

ZERO_HASH = "0" * 64
RUN_PREFIX = "phase3_"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def values(step: int) -> tuple[bytes, bytes]:
    model = struct.pack("<4f", *(step + value for value in (1.0, 2.0, 3.0, 4.0)))
    source = bytes((0x80, 0xff, 0x00, 0x7f))
    tpu = struct.pack("<4f", *((int.from_bytes(bytes([value]), "little", signed=True) - 1) * 0.5 for value in source))
    return model, tpu


def optimizer(step: int) -> bytes:
    return json.dumps({"format_version": 1, "parameter_groups": [{"parameters": ["model.weight"], "options": {"lr": 0.01}}], "parameters": {"model.weight": {"scalars": {"step": step}, "tensors": {}}}}, separators=(",", ":"), sort_keys=True).encode()


def rng(rank: int, step: int) -> bytes:
    return struct.pack("<QQ", rank, step)


def manifest(run: str, step: int) -> dict[str, object]:
    model, tpu = values(step)
    optimizer_data = optimizer(step)
    return {
        "checkpoint_version": 2,
        "run_id": run,
        "global_step": step,
        "epoch": step / 10,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "metadata": {"total_parameters": 4, "canonical_dtype": "float32", "model_id": "phase3-smoke", "model_schema_hash": ZERO_HASH, "dataset_id": "phase3-data", "container_image_digest": "sha256:" + ZERO_HASH, "framework": "python", "framework_version": "3", "adapter_versions": {"cpu": "1.0.0", "opentpu-sim": "1.0.0"}},
        "world": {"size": 2, "ranks": [{"rank": 0, "hardware": "cpu", "adapter": "cpu-raw"}, {"rank": 1, "hardware": "opentpu-sim", "adapter": "pyrtl-int8"}]},
        "tensors": {
            "model.weight": {"role": "model_parameter", "global_shape": [4], "canonical_dtype": "float32", "byte_order": "little", "layout": "C", "chunks": [{"chunk_id": "model_weight", "storage_path": "shards/model_weight.bin", "slice": [[0, 4]], "shape": [4], "byte_offset": 0, "byte_length": len(model), "sha256": digest(model), "writer_rank": 0}]},
            "opentpu.state": {"role": "application_state", "global_shape": [4], "canonical_dtype": "float32", "byte_order": "little", "layout": "C", "quantization": {"scheme": "affine_int8", "scale": 0.5, "zero_point": 1, "rounding": "ties_to_even", "clamp": [-128, 127]}, "chunks": [{"chunk_id": "opentpu_state", "storage_path": "shards/opentpu_state.bin", "slice": [[0, 4]], "shape": [4], "byte_offset": 0, "byte_length": len(tpu), "sha256": digest(tpu), "writer_rank": 1}]},
        },
        "state": {"optimizer_metadata": {"storage_path": "optimizer_state.json", "byte_length": len(optimizer_data), "sha256": digest(optimizer_data)}, "scheduler": {"name": "smoke", "last_step": step}, "rng": {"0": {"storage_path": "rng/rank_00000.bin", "byte_length": 16, "sha256": digest(rng(0, step))}, "1": {"storage_path": "rng/rank_00001.bin", "byte_length": 16, "sha256": digest(rng(1, step))}}, "data_cursor": {"sample": step}},
    }


def compatibility(adapter: str) -> dict[str, object]:
    return {"model_id": "phase3-smoke", "model_schema_hash": ZERO_HASH, "dataset_id": "phase3-data", "container_image_digest": "sha256:" + ZERO_HASH, "framework": "python", "framework_version": "3", "adapter_versions": {adapter: "1.0.0"}}


def save_rank(client: CheckpointClient, run: str, step: int, rank: int) -> None:
    model, tpu = values(step)
    if rank == 0:
        client.upload(run, step, "model_weight", "shards/model_weight.bin", model)
        client.upload(run, step, "optimizer_metadata", "optimizer_state.json", optimizer(step))
    else:
        client.upload(run, step, "opentpu_state", "shards/opentpu_state.bin", tpu)
    client.upload(run, step, f"rng_{rank}", f"rng/rank_{rank:05d}.bin", rng(rank, step))


def restore_rank(client: CheckpointClient, run: str, rank: int) -> None:
    adapter = "cpu" if rank == 0 else "opentpu-sim"
    latest = client.latest(run, compatibility(adapter))
    checkpoint = latest["manifest"]
    if checkpoint["global_step"] != 5:
        raise RuntimeError(f"restored step {checkpoint['global_step']}, expected 5")
    model, tpu = values(5)
    if rank == 0:
        if client.restore(run, 5, "model_weight", len(model)) != model:
            raise RuntimeError("model tensor changed across requeue")
        if client.restore(run, 5, "optimizer_metadata", len(optimizer(5))) != optimizer(5):
            raise RuntimeError("optimizer state changed across requeue")
    else:
        canonical = client.restore(run, 5, "opentpu_state", len(tpu))
        restored, _ = client.convert(canonical, 4, "float32", "int8", 0.5, 1)
        if restored != bytes((0x80, 0xff, 0x00, 0x7f)):
            raise RuntimeError("OpenTPU state changed across requeue")
    if client.restore(run, 5, f"rng_{rank}", 16) != rng(rank, 5):
        raise RuntimeError("RNG state changed across requeue")
    if checkpoint["state"]["scheduler"]["last_step"] != 5 or checkpoint["state"]["data_cursor"]["sample"] != 5:
        raise RuntimeError("scheduler or data cursor did not resume")


def begin_incomplete_upload(client: CheckpointClient, run: str) -> None:
    transaction, streams = client.create_transaction({"partial": 128 << 20})

    def upload() -> None:
        client._request(client.flusher_socket, "PUT", f"/v1/checkpoints/{run}/6/chunks/incomplete", headers={"X-Checkpoint-Transaction": transaction, "X-Checkpoint-Stream": "partial", "X-Checkpoint-Storage-Path": "shards/incomplete.bin"})

    threading.Thread(target=upload, daemon=True).start()
    ring = Ring(streams["partial"], producer=True)
    ring.write(bytes(32 << 20))
    time.sleep(300)


def main() -> None:
    group = int(os.environ.get("SLURM_HET_GROUP", os.environ.get("SLURM_PROCID", "0")))
    job = int(os.environ["SLURM_JOB_ID"])
    run = RUN_PREFIX + str(job)
    client = CheckpointClient(job_id=job, rank=group)
    if int(os.environ.get("SLURM_RESTART_COUNT", "0")) > 0:
        restore_rank(client, run, group)
        return
    save_rank(client, run, 0, group)
    if group == 0:
        time.sleep(8)
        client.commit(run, 0, manifest(run, 0))
    save_rank(client, run, 5, group)
    if group == 0:
        time.sleep(8)
        client.commit(run, 5, manifest(run, 5))
        time.sleep(300)
    else:
        time.sleep(15)
        begin_incomplete_upload(client, run)


if __name__ == "__main__":
    main()
