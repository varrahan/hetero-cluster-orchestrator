from __future__ import annotations

import unittest

from checkpointing.client import _background


class BackgroundTest(unittest.TestCase):
    def test_result_and_exception(self) -> None:
        def fail() -> None:
            raise ValueError("failed")

        self.assertEqual(_background(lambda: 42).result(1), 42)
        with self.assertRaisesRegex(ValueError, "failed"):
            _background(fail).result(1)


if __name__ == "__main__":
    unittest.main()
