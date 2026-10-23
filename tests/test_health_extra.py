import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("health_extra", ROOT / "common/check.py")
health = importlib.util.module_from_spec(spec)
spec.loader.exec_module(health)


class CheckpointTests(unittest.TestCase):
    def test_observed_checkpoint_survives_daemon_restart(self):
        # LastCheckpointTime is the field observed in Hadoop 3.4.2 SNN JMX.
        observed = 1792718505313
        payload = {"beans": [{"name": "Hadoop:service=SecondaryNameNode,name=SecondaryNameNodeInfo",
                               "LastCheckpointTime": observed, "StartTime": observed + 60000}]}
        self.assertEqual(health.checkpoint_time(payload), observed)

    def test_available_daemon_without_checkpoint_does_not_pass(self):
        for value in (None, 0, -1, True, "1792718505313"):
            with self.subTest(value=value), self.assertRaisesRegex(AssertionError, "completed checkpoint"):
                health.checkpoint_time({"beans": [{"LastCheckpointTime": value}]})
        with self.assertRaisesRegex(AssertionError, "completed checkpoint"):
            health.checkpoint_time({"beans": [{"name": "SecondaryNameNodeInfo"}]})

    def test_missing_daemon_does_not_pass(self):
        with self.assertRaisesRegex(AssertionError, "JMX is missing"):
            health.checkpoint_time({"beans": []})
