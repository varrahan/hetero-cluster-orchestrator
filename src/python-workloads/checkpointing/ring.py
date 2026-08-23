"""ctypes binding for the shared C11 atomic-ring ABI."""

from __future__ import annotations

import ctypes
import os
from pathlib import Path


class Ring:
    def __init__(self, path: str | Path, producer: bool) -> None:
        library = ctypes.CDLL(os.environ.get("AIORCH_RING_LIBRARY", "/usr/local/lib/libaiorch_ring.so"))
        library.aiorch_ring_open.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.POINTER(ctypes.c_void_p)]
        library.aiorch_ring_open.restype = ctypes.c_int
        library.aiorch_ring_write.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
        library.aiorch_ring_write.restype = ctypes.c_ssize_t
        library.aiorch_ring_read.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
        library.aiorch_ring_read.restype = ctypes.c_ssize_t
        library.aiorch_ring_total.argtypes = [ctypes.c_void_p]
        library.aiorch_ring_total.restype = ctypes.c_uint64
        library.aiorch_ring_close.argtypes = [ctypes.c_void_p]
        library.aiorch_ring_close.restype = ctypes.c_int
        handle = ctypes.c_void_p()
        result = library.aiorch_ring_open(os.fsencode(path), int(producer), ctypes.byref(handle))
        if result < 0:
            raise OSError(-result, "open atomic ring")
        self._library = library
        self._handle = handle
        self.total = int(library.aiorch_ring_total(handle))
        self._producer = producer

    def write(self, data: bytes, timeout_ms: int = 120_000) -> None:
        if not self._producer:
            raise ValueError("consumer ring cannot be written")
        buffer = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
        result = self._library.aiorch_ring_write(self._handle, buffer, len(data), timeout_ms)
        if result < 0:
            raise OSError(-result, "write atomic ring")
        if result != len(data):
            raise OSError("short atomic-ring write")

    def read(self, timeout_ms: int = 120_000) -> bytes:
        if self._producer:
            raise ValueError("producer ring cannot be read")
        result = bytearray()
        while len(result) < self.total:
            length = min(1 << 20, self.total - len(result))
            buffer = (ctypes.c_ubyte * length)()
            count = self._library.aiorch_ring_read(self._handle, buffer, length, timeout_ms)
            if count < 0:
                raise OSError(-count, "read atomic ring")
            if count == 0:
                break
            result.extend(buffer[:count])
        if len(result) != self.total:
            raise OSError(f"atomic ring ended after {len(result)} of {self.total} bytes")
        return bytes(result)

    def close(self) -> None:
        if self._handle:
            result = self._library.aiorch_ring_close(self._handle)
            self._handle = None
            if result < 0:
                raise OSError(-result, "close atomic ring")

    def __enter__(self) -> "Ring":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
