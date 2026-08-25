"""Compare the pinned OpenTPU hardware and functional simulators."""

from __future__ import annotations

import builtins
import os
import re
import runpy
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pyrtl


def _run(root: Path, script: str, program: Path, host: Path, weights: Path, matrix_size: int) -> np.ndarray:
    pyrtl.reset_working_block()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    import config  # type: ignore[import-not-found]

    pyrtl.helperfuncs.mult_signed = pyrtl.signed_mult
    builtins.helperfuncs = pyrtl.helperfuncs
    config.MATSIZE = matrix_size
    sys.argv = [script, str(program), str(host), str(weights)]
    namespace = runpy.run_path(str(root / script), run_name="__main__")
    if script == "runtpu.py":
        packed = namespace["hostmem"]
        rows = []
        for address in range(len(packed)):
            value = int(packed[address])
            rows.append([(value >> (8 * column)) & 0xFF for column in range(matrix_size)])
        return np.asarray(rows, dtype=np.uint8)
    if "tpusim" in namespace:
        return np.asarray(namespace["tpusim"].host_memory).astype(np.uint8)
    raise RuntimeError(f"{script} did not expose final host memory")


def main() -> None:
    matrix_size = int(os.environ.get("OPENTPU_MATRIX_SIZE", "8"))
    if matrix_size not in (8, 16):
        raise SystemExit("OPENTPU_MATRIX_SIZE must be 8 or 16")
    root = Path(os.environ.get("OPENTPU_ROOT", "/opt/opentpu"))
    with tempfile.TemporaryDirectory(prefix="opentpu-verify-") as directory:
        work = Path(directory)
        assembly = re.sub(r"(RHM|MMC\.S|ACT\.R|WHM) ([^#]*), 8", r"\1 \2, %d" % matrix_size, (root / "simplemult.a").read_text())
        source = work / "verify.a"
        source.write_text(assembly)
        subprocess.run([sys.executable, str(root / "assembler.py"), str(source)], check=True, cwd=work)
        program = source.with_suffix(".out")
        left = np.fromfunction(lambda row, column: (row * 3 + column * 5) % 11 - 5, (matrix_size, matrix_size), dtype=int).astype(np.int8)
        right = np.fromfunction(lambda row, column: (row * 7 + column * 2) % 13 - 6, (matrix_size, matrix_size), dtype=int).astype(np.int8)
        host, weights = work / "host.npy", work / "weights.npy"
        np.save(host, left)
        np.save(weights, right.reshape(1, matrix_size, matrix_size))
        old_cwd = Path.cwd()
        os.chdir(work)
        try:
            hardware = _run(root, "runtpu.py", program, host, weights, matrix_size)
            functional = _run(root, "sim.py", program, host, weights, matrix_size)
        finally:
            os.chdir(old_cwd)
        expected = (np.maximum(left.astype(np.int32) @ right.astype(np.int32), 0) & 0xFF).astype(np.uint8)
        if not np.array_equal(hardware, functional) or not np.array_equal(hardware, expected):
            raise SystemExit("OpenTPU hardware output differs from the known NumPy matrix result")


if __name__ == "__main__":
    main()
