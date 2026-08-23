from __future__ import annotations

import importlib.util
import unittest


@unittest.skipUnless(importlib.util.find_spec("numpy"), "NumPy is not installed")
class CPUAdapterTest(unittest.TestCase):
    def test_array_and_rng_round_trip(self) -> None:
        import numpy as np
        from checkpointing.cpu import capture_rng, decode_array, encode_array, restore_rng
        source = np.arange(12, dtype=np.float64).reshape(3, 4)
        data = encode_array(source, "float32")
        restored = decode_array(data, [3, 4], "float32")
        np.testing.assert_array_equal(restored, source.astype(np.float32))
        state = capture_rng(); expected = np.random.random(); restore_rng(state)
        self.assertEqual(np.random.random(), expected)


if __name__ == "__main__":
    unittest.main()
