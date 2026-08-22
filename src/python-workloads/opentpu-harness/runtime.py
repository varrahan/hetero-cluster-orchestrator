"""Run the pinned OpenTPU PyRTL simulator for an allocated slot."""

from __future__ import annotations

import argparse
import builtins
import config  # type: ignore[import-not-found]
import os
import runpy
import sys
from pathlib import Path
import pyrtl

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("program", nargs="?", default="simplemult.out")
    parser.add_argument("host_memory", nargs="?", default="simplemult_hostmem.npy")
    parser.add_argument("weights", nargs="?", default="simplemult_weights.npy")
    args = parser.parse_args()

    devices = os.environ.get("OPENTPU_DEVICES", "")
    matrix_size = int(os.environ.get("OPENTPU_MATRIX_SIZE", "8"))
    if not devices:
        raise SystemExit("OPENTPU_DEVICES is empty; submit with an OpenTPU GRES")
    if matrix_size not in (8, 16):
        raise SystemExit("OPENTPU_MATRIX_SIZE must be 8 or 16")

    root = Path(os.environ.get("OPENTPU_ROOT", "/opt/opentpu"))
    sys.path.insert(0, str(root))

    # The pinned OpenTPU revision uses PyRTL's former mult_signed helper name.
    pyrtl.helperfuncs.mult_signed = pyrtl.signed_mult
    builtins.helperfuncs = pyrtl.helperfuncs

    config.MATSIZE = matrix_size
    sys.argv = ["runtpu.py", str(root / args.program), str(root / args.host_memory), str(root / args.weights)]
    runpy.run_path(str(root / "runtpu.py"), run_name="__main__")


if __name__ == "__main__":
    main()
