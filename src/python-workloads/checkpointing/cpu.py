"""NumPy/CPU canonical tensor and RNG helpers."""

from __future__ import annotations

import random
from math import prod
from typing import Any

import numpy as np

_DTYPES = {"float16": "<f2", "float32": "<f4", "float64": "<f8", "int8": "i1", "uint8": "u1", "int16": "<i2", "int32": "<i4", "int64": "<i8", "bool": "?"}


def encode_array(array: np.ndarray, dtype: str) -> bytes:
    if dtype not in _DTYPES:
        raise ValueError(f"unsupported canonical dtype {dtype!r}")
    return np.ascontiguousarray(array, dtype=np.dtype(_DTYPES[dtype])).tobytes()


def decode_array(data: bytes, shape: list[int], dtype: str) -> np.ndarray:
    if dtype not in _DTYPES or not shape or any(value < 1 for value in shape):
        raise ValueError("invalid dtype or shape")
    expected = prod(shape) * np.dtype(_DTYPES[dtype]).itemsize
    if expected != len(data):
        raise ValueError(f"tensor needs {expected} bytes, received {len(data)}")
    return np.frombuffer(data, dtype=np.dtype(_DTYPES[dtype])).reshape(shape).copy()


def capture_rng() -> dict[str, Any]:
    algorithm, keys, position, gaussian, cached = np.random.get_state()
    return {"python": _json_value(random.getstate()), "numpy": {"algorithm": algorithm, "keys": keys.tolist(), "position": position, "gaussian": gaussian, "cached": cached}}


def restore_rng(state: dict[str, Any]) -> None:
    random.setstate(_tuple_value(state["python"]))
    numpy = state["numpy"]
    np.random.set_state((numpy["algorithm"], np.asarray(numpy["keys"], dtype=np.uint32), int(numpy["position"]), int(numpy["gaussian"]), float(numpy["cached"])))


def _json_value(value: Any) -> Any:
    return [_json_value(item) for item in value] if isinstance(value, tuple) else value


def _tuple_value(value: Any) -> Any:
    return tuple(_tuple_value(item) for item in value) if isinstance(value, list) else value
