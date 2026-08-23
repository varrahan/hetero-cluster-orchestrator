"""OpenTPU INT8 conversion adapter."""

from __future__ import annotations

import numpy as np

from .client import CheckpointClient


def save_canonical(client: CheckpointClient, values: np.ndarray, canonical_dtype: str, scale: float, zero_point: int) -> bytes:
    source = np.asarray(values, dtype=np.int8, order="C")
    result, _ = client.convert(source.tobytes(), source.size, "int8", canonical_dtype, scale, zero_point)
    return result


def restore_int8(client: CheckpointClient, canonical: bytes, elements: int, canonical_dtype: str, scale: float, zero_point: int, shape: tuple[int, ...]) -> np.ndarray:
    result, _ = client.convert(canonical, elements, canonical_dtype, "int8", scale, zero_point)
    return np.frombuffer(result, dtype=np.int8).reshape(shape).copy()
