import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from common import cluster


class StagingTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads((cluster.ROOT / 'inventory.example.json').read_text())
        for node in self.c['nodes']:
            node['local'] = False

    def test_cleanup_after_failed_install_preserves_original_error(self):
        calls = []
        def remote(c, node, command, capture=False):
            calls.append((node['name'], command))
            if 'mktemp' in command:
                return subprocess.CompletedProcess([], 0, '/tmp/teachbase-hadoop.' + node['name'] + '\n')
            if 'install-hdfs' in command:
                raise subprocess.CalledProcessError(7, 'install-hdfs')
            return subprocess.CompletedProcess([], 0, '')
        with patch.object(cluster, 'remote', side_effect=remote), patch.object(cluster.subprocess, 'run'):
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                cluster.deploy(self.c, 'hdfs', False)
        self.assertEqual(caught.exception.returncode, 7)
        cleaned = [(node, command) for node, command in calls if command.startswith('rm -rf -- ')]
        self.assertEqual(len(cleaned), 4)
        for node, command in cleaned:
            self.assertEqual(command, 'rm -rf -- /tmp/teachbase-hadoop.' + node)

    def test_unexpected_remote_path_is_never_deleted(self):
        with patch.object(cluster, 'remote', return_value=subprocess.CompletedProcess([], 0, '/home/team\n')) as remote:
            with self.assertRaisesRegex(RuntimeError, 'Unexpected staging directory'):
                cluster.deploy(self.c, 'hdfs', False)
        self.assertEqual(remote.call_count, 1)

    def test_web_is_disabled_when_enabled_key_is_absent(self):
        del self.c['web']['enabled']
        with tempfile.TemporaryDirectory() as tmp:
            cluster.render(self.c, Path(tmp))
            self.assertFalse((Path(tmp) / 'nginx.conf').exists())


if __name__ == '__main__':
    unittest.main()
