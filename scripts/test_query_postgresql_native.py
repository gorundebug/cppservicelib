import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('native_pg', Path(__file__).with_name('query-postgresql-native.py'))
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)
SDK = Path(__file__).resolve().parent.parent


class NativePostgreSQLContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.pg = self.root / 'pg'
        for name in ['include/libpq-fe.h', 'include/internal/libpq-int.h',
                     'server/postgres_fe.h', 'lib/libpq.a', 'serverlib/libpgcommon.a',
                     'serverlib/libpgport.a', 'lib/libpq.so']:
            path = self.pg / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(name.encode())

    def run_config(self, root=None):
        root = root or self.pg
        values = {'--version': 'PostgreSQL 16.11', '--includedir': root / 'include',
                  '--includedir-server': root / 'server', '--libdir': root / 'lib',
                  '--pkglibdir': root / 'serverlib'}
        with patch.object(native.subprocess, 'check_output', side_effect=lambda args, **kwargs: str(values[args[-1]])):
            return native.configure(self.root / 'userver', 'Debug', SDK,
                                    self.root / 'output', root / 'pg_config')

    def test_content_identity_is_independent_of_install_path(self):
        first = self.run_config()
        relocated = self.root / 'relocated'
        shutil.copytree(self.pg, relocated)
        self.assertEqual(first, self.run_config(relocated))
        (relocated / 'server/postgres_fe.h').write_text('different ABI')
        self.assertNotEqual(first, self.run_config(relocated))

    def test_library_changes_invalidate_identity(self):
        first = self.run_config()
        (self.pg / 'lib/libpq.a').write_bytes(b'new library')
        self.assertNotEqual(first, self.run_config())

    def test_optional_libraries_cannot_fall_back_to_conan(self):
        self.run_config()
        toolchain = (self.root / 'output/native-postgresql.cmake').read_text()
        self.assertIn('LIBRARIES_libpgcommon_shlib_a-NOTFOUND', toolchain)
        self.assertIn('LIBRARIES_libpq-oauth_a-NOTFOUND', toolchain)
        self.assertIn('CMAKE_TRY_COMPILE_CONFIGURATION "Debug"', toolchain)
        configs = (self.root / 'output/native-postgresql.conan-args').read_text().splitlines()
        identity = json.loads((self.root / 'output/native-postgresql.manifest.json').read_text())['identity']
        self.assertIn('userver/*:user.userver:postgresql_native=' + identity, configs)
        self.assertIn('user.userver:postgresql_native', json.loads(configs[-1].split('=', 1)[1]))

    def test_incomplete_installation_is_rejected(self):
        (self.pg / 'include/internal/libpq-int.h').unlink()
        with self.assertRaisesRegex(RuntimeError, 'development component missing'):
            self.run_config()
        self.assertFalse((self.root / 'output/native-postgresql.conan-args').exists())


if __name__ == '__main__':
    unittest.main()
