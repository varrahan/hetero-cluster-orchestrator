"""Pickle-free PyTorch tensor, optimizer, scheduler, and RNG adapter."""

from __future__ import annotations

from typing import Any

_DTYPES = {"float16", "bfloat16", "float32", "float64", "int8", "uint8", "int16", "int32", "int64", "bool"}


def _torch() -> Any:
    import torch
    return torch


def encode_tensor(tensor: Any, dtype: str = "float32") -> bytes:
    torch = _torch()
    if dtype not in _DTYPES:
        raise ValueError(f"unsupported canonical dtype {dtype!r}")
    value = tensor.detach().to(device="cpu", dtype=getattr(torch, dtype)).contiguous()
    return bytes(value.view(torch.uint8).numpy())


def restore_tensor(tensor: Any, data: bytes, shape: list[int], dtype: str = "float32") -> None:
    torch = _torch()
    if dtype not in _DTYPES:
        raise ValueError(f"unsupported canonical dtype {dtype!r}")
    source = torch.frombuffer(bytearray(data), dtype=getattr(torch, dtype)).reshape(shape)
    if list(source.shape) != list(tensor.shape):
        raise ValueError("restored shape does not match target tensor")
    tensor.copy_(source.to(device=tensor.device, dtype=tensor.dtype))


def optimizer_metadata(optimizer: Any, parameter_names: list[str]) -> dict[str, Any]:
    state = optimizer.state_dict()
    identifiers = [identifier for group in state["param_groups"] for identifier in group["params"]]
    if len(identifiers) != len(parameter_names):
        raise ValueError("optimizer parameter names do not match state")
    names = dict(zip(identifiers, parameter_names, strict=True))
    parameters: dict[str, Any] = {}
    for identifier, values in state["state"].items():
        scalars: dict[str, Any] = {}; tensors: dict[str, str] = {}
        for slot, value in values.items():
            if hasattr(value, "detach"):
                tensors[slot] = f"optimizer.{names[identifier]}.{slot}"
            elif isinstance(value, (bool, int, float, str)) or value is None:
                scalars[slot] = value
            else:
                raise TypeError(f"optimizer scalar {slot!r} is not JSON-compatible")
        parameters[names[identifier]] = {"scalars": scalars, "tensors": tensors}
    groups = [{"parameters": [names[value] for value in group["params"]], "options": {key: value for key, value in group.items() if key != "params"}} for group in state["param_groups"]]
    return {"format_version": 1, "parameter_groups": groups, "parameters": parameters}


def capture_rng() -> dict[str, bytes]:
    torch = _torch()
    result = {"cpu": bytes(torch.get_rng_state().numpy())}
    for index, state in enumerate(torch.cuda.get_rng_state_all() if torch.cuda.is_available() else []):
        result[f"cuda_{index}"] = bytes(state.numpy())
    return result


def restore_rng(state: dict[str, bytes]) -> None:
    torch = _torch()
    torch.set_rng_state(torch.frombuffer(bytearray(state["cpu"]), dtype=torch.uint8).clone())
    cuda = [state[key] for key in sorted(state) if key.startswith("cuda_")]
    if cuda:
        torch.cuda.set_rng_state_all([torch.frombuffer(bytearray(value), dtype=torch.uint8).clone() for value in cuda])
