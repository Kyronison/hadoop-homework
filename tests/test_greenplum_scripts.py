from pathlib import Path
import re
import shutil
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]
HW6 = ROOT / 'hw6'
SQL = HW6 / 'sql'


def find_bash():
    bash = Path('C:/Program Files/Git/bin/bash.exe')
    if bash.is_file():
        return bash
    found = shutil.which('bash')
    return Path(found) if found else None


def sql_without_comments(path):
    """SQL без комментариев и метакоманд psql (\\echo, \\if и т. п.)."""
    lines = path.read_text(encoding='utf-8').splitlines()
    return '\n'.join(line.split('--', 1)[0] for line in lines if not line.lstrip().startswith('\\'))


class GreenplumScriptTests(unittest.TestCase):
    def test_run_sh_calls_every_sql_step_in_order(self):
        run = (HW6 / 'run.sh').read_text(encoding='utf-8')
        called = re.findall(r'run_sql (\d\d_[a-z_]+\.sql)', run)
        expected = sorted(p.name for p in SQL.glob('*.sql'))
        self.assertEqual(called, expected)

    def test_files_use_lf_line_endings(self):
        for path in [HW6 / 'run.sh', HW6 / 'remote.sh', *SQL.glob('*.sql')]:
            with self.subTest(path=path.name):
                self.assertNotIn(b'\r\n', path.read_bytes())

    def test_objects_are_created_only_through_team_prefixed_variables(self):
        allowed = {'ext', 'taxi', 'daily', 'key_day', 'key_vendor', 'key_random'}
        statement = re.compile(
            r'\b(?:CREATE(?:\s+READABLE)?(?:\s+EXTERNAL)?\s+TABLE|DROP(?:\s+EXTERNAL)?\s+TABLE'
            r'(?:\s+IF\s+EXISTS)?|INSERT\s+INTO|ANALYZE)\s+([^\s;(]+)', re.IGNORECASE)
        for path in SQL.glob('*.sql'):
            for target in statement.findall(sql_without_comments(path)):
                with self.subTest(file=path.name, target=target):
                    names = re.findall(r':"(\w+)"', target)
                    self.assertTrue(names, 'имя таблицы должно задаваться переменной psql')
                    self.assertTrue(set(names) <= allowed)

    def test_public_schema_is_only_read(self):
        write = re.compile(r'\b(?:INSERT|UPDATE|DELETE|TRUNCATE|CREATE|DROP|ALTER)\b[^;]*\bpublic\.',
                           re.IGNORECASE)
        for path in SQL.glob('*.sql'):
            with self.subTest(file=path.name):
                self.assertIsNone(write.search(sql_without_comments(path)))

    def test_every_sql_step_sets_team_schema(self):
        for path in SQL.glob('*.sql'):
            if path.name == '01_connection.sql':
                continue
            with self.subTest(file=path.name):
                self.assertIn('SET search_path TO :"schema";', path.read_text(encoding='utf-8'))

    def test_csv_is_removed_even_after_failure(self):
        run = (HW6 / 'run.sh').read_text(encoding='utf-8')
        self.assertIn('trap cleanup_csv EXIT', run)
        self.assertIn('set -euo pipefail', run)
        self.assertIn('ON_ERROR_STOP=1', run)

    def test_shell_syntax(self):
        bash = find_bash()
        if bash is None:
            self.skipTest('bash не найден')
        for name in ('run.sh', 'remote.sh'):
            with self.subTest(script=name):
                result = subprocess.run([str(bash), '-n', str(HW6 / name)], capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
