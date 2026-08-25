"""Bounded HTTP client for checkpoint and quantizer Unix sockets."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import secrets
import socket
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .ring import Ring


class _UnixConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float = 120) -> None:
        super().__init__("localhost", timeout=timeout)
        self._path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._path)


_background = ThreadPoolExecutor().submit


def _read_ring(path: str) -> bytes:
    with Ring(path, producer=False) as ring:
        return ring.read()


class CheckpointClient:
    def __init__(self, job_id: int | None = None, rank: int | None = None, flusher_socket: str = "/run/gputpu-checkpoint/flusher.sock", quantizer_socket: str = "/run/gputpu-quantization/engine.sock") -> None:
        self.job_id = int(os.environ["SLURM_JOB_ID"] if job_id is None else job_id)
        self.rank = int(rank if rank is not None else os.environ.get("SLURM_PROCID", "0"))
        if self.job_id < 1 or self.rank < 0:
            raise ValueError("invalid Slurm job ID or rank")
        self.flusher_socket = flusher_socket
        self.quantizer_socket = quantizer_socket

    def _headers(self) -> dict[str, str]:
        return {"X-Slurm-Job-Id": str(self.job_id), "X-Slurm-Proc-Id": str(self.rank)}

    def _request(self, socket_path: str, method: str, path: str, body: bytes | None = None, headers: dict[str, str] | None = None) -> Any:
        request_headers = self._headers()
        request_headers.update(headers or {})
        if body is not None:
            request_headers["Content-Type"] = "application/json"
        with closing(_UnixConnection(socket_path)) as connection:
            connection.request(method, path, body=body, headers=request_headers)
            response = connection.getresponse()
            data = response.read(16 << 20)
        payload = json.loads(data or b"{}")
        if response.status < 200 or response.status >= 300:
            raise RuntimeError(payload.get("message") or payload.get("error") or response.reason)
        return payload

    def create_transaction(self, lengths: dict[str, int]) -> tuple[str, dict[str, str]]:
        transaction = secrets.token_hex(16)
        body = json.dumps({"streams": [{"name": name, "byte_length": length} for name, length in lengths.items()]}, separators=(",", ":")).encode()
        result = self._request(self.flusher_socket, "POST", f"/v1/transactions/{transaction}", body)
        return transaction, result["streams"]

    def delete_transaction(self, transaction: str) -> None:
        self._request(self.flusher_socket, "DELETE", f"/v1/transactions/{transaction}")

    def _cleanup(self, transaction: str) -> None:
        try:
            self.delete_transaction(transaction)
        except (OSError, RuntimeError):
            pass

    def upload(self, run: str, step: int, object_id: str, storage_path: str, data: bytes) -> dict[str, Any]:
        transaction, streams = self.create_transaction({"upload": len(data)})
        request = _background(
            lambda: self._request(
                self.flusher_socket,
                "PUT",
                f"/v1/checkpoints/{run}/{step}/chunks/{object_id}",
                headers={"X-Checkpoint-Transaction": transaction, "X-Checkpoint-Stream": "upload", "X-Checkpoint-Storage-Path": storage_path},
            )
        )
        try:
            with Ring(streams["upload"], producer=True) as ring:
                ring.write(data)
            receipt = request.result(125)
            if receipt["sha256"] != hashlib.sha256(data).hexdigest():
                raise RuntimeError("flusher upload digest mismatch")
            return receipt
        finally:
            self._cleanup(transaction)

    def restore(self, run: str, step: int, object_id: str, byte_length: int) -> bytes:
        transaction, streams = self.create_transaction({"restore": byte_length})
        request = _background(
            lambda: self._request(
                self.flusher_socket,
                "GET",
                f"/v1/checkpoints/{run}/{step}/chunks/{object_id}",
                headers={"X-Checkpoint-Transaction": transaction, "X-Checkpoint-Stream": "restore"},
            )
        )
        try:
            data = _read_ring(streams["restore"])
            receipt = request.result(125)
            if receipt["sha256"] != hashlib.sha256(data).hexdigest():
                raise RuntimeError("flusher restore digest mismatch")
            return data
        finally:
            self._cleanup(transaction)

    def latest(self, run: str, compatibility: dict[str, Any], before_step: int | None = None) -> dict[str, Any]:
        suffix = "" if before_step is None else f"?before_step={before_step}"
        return self._request(self.flusher_socket, "GET", f"/v1/checkpoints/{run}/latest{suffix}", headers={"X-Checkpoint-Compatibility": json.dumps(compatibility, separators=(",", ":"))})

    def commit(self, run: str, step: int, manifest: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
        return self._request(self.flusher_socket, "POST", f"/v1/checkpoints/{run}/{step}/commit", body)

    def convert(self, data: bytes, elements: int, source_dtype: str, target_dtype: str, scale: float, zero_point: int) -> tuple[bytes, dict[str, Any]]:
        sizes = {"int8": 1, "bfloat16": 2, "float32": 4}
        output_length = elements * sizes[target_dtype]
        transaction, streams = self.create_transaction({"input": len(data), "output": output_length})
        body = json.dumps({"input_path": streams["input"], "output_path": streams["output"], "elements": elements, "source_dtype": source_dtype, "target_dtype": target_dtype, "scale": scale, "zero_point": zero_point}, separators=(",", ":")).encode()
        conversion = _background(lambda: self._request(self.quantizer_socket, "POST", "/v1/conversions", body))
        output = _background(lambda: _read_ring(streams["output"]))
        try:
            with Ring(streams["input"], producer=True) as input_ring:
                input_ring.write(data)
            result, converted = conversion.result(125), output.result(125)
            if result["sha256"] != hashlib.sha256(converted).hexdigest():
                raise RuntimeError("quantization digest mismatch")
            return converted, result
        finally:
            self._cleanup(transaction)
