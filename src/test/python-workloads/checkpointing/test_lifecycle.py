from __future__ import annotations

import unittest

from checkpointing.lifecycle import CheckpointLifecycle


class LifecycleTest(unittest.TestCase):
    def test_step_zero_and_usr1_request(self) -> None:
        saves: list[tuple[int, str]] = []
        lifecycle = CheckpointLifecycle(300)
        self.assertEqual(lifecycle.restore_or_initialize(lambda: None, lambda step, reason: saves.append((step, reason))), 0)
        lifecycle._signal(0, None)
        self.assertTrue(lifecycle.checkpoint_if_due(7, lambda step, reason: saves.append((step, reason))))
        self.assertEqual(saves, [(0, "step-zero"), (7, "usr1")])


if __name__ == "__main__":
    unittest.main()
