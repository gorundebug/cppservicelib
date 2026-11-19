#!/usr/bin/env python3
"""Configure upstream pq-extra using one native PostgreSQL development installation."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def configure(userver, build_type, sdk, output, pg_config):
    def pg(option):
        return subprocess.check_output([str(pg_config), option], text=True).strip()

    version = pg('--version')
    include, server, lib, serverlib = (Path(pg(x)) for x in
        ('--includedir', '--includedir-server', '--libdir', '--pkglibdir'))
    parts = {
        'INCLUDE_DIRS_postgres_fe_h': server,
        'INCLUDE_DIRS_libpq-int_h': include / 'internal',
        'INCLUDE_DIRS_libpq-fe_h': include,
    }
    required = [server / 'postgres_fe.h', include / 'internal/libpq-int.h', include / 'libpq-fe.h']
    for name in ('libpq.a', 'libpgcommon.a', 'libpgport.a'):
        candidates = (lib / name, serverlib / name) if name == 'libpq.a' else (serverlib / name, lib / name)
        found = next((path for path in candidates if path.is_file()), candidates[0])
        parts['LIBRARIES_' + name.replace('.', '_')] = found
        required.append(found)
    for path in required:
        if not path.is_file():
            raise RuntimeError('PostgreSQL development component missing: ' + str(path))
    for name in ('libpgcommon_shlib.a', 'libpq-oauth.a'):
        found = next((path for path in (serverlib / name, lib / name) if path.is_file()), None)
        key = 'LIBRARIES_' + name.replace('.', '_')
        # Explicit NOTFOUND prevents a fallback to an unrelated Conan installation.
        parts[key] = found if found else 'PostgreSQLInternal_' + key + '-NOTFOUND'
    client = next((lib / name for name in ('libpq.so', 'libpq.dylib') if (lib / name).is_file()), None)
    if client is None:
        raise RuntimeError('PostgreSQL shared client library missing in ' + str(lib))

    files = {}
    for label, directory in (('client', include), ('server', server)):
        for path in sorted(directory.rglob('*.h')):
            files[label + '/' + str(path.relative_to(directory))] = hashlib.sha256(path.read_bytes()).hexdigest()
    for key, path in parts.items():
        if isinstance(path, Path) and path.is_file():
            files[key] = hashlib.sha256(path.read_bytes()).hexdigest()
    files['shared-client'] = hashlib.sha256(client.read_bytes()).hexdigest()
    package_config = sdk / 'conan/native-postgresql/PostgreSQLConfig.cmake'
    files['configure-script'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    files['cmake-package'] = hashlib.sha256(package_config.read_bytes()).hexdigest()
    identity = hashlib.sha256(json.dumps({'version': version, 'files': files}, sort_keys=True).encode()).hexdigest()
    args = ['-DUSERVER_FEATURE_CHAOTIC_EXPERIMENTAL=ON',
            '-DCMAKE_MODULE_PATH=' + str(userver / 'cmake/modules'),
            '-DCMAKE_TRY_COMPILE_CONFIGURATION=' + build_type,
            '-DPostgreSQL_DIR=' + str(package_config.parent),
            '-DUSERVER_PG_LIBRARY_DIR=' + str(lib),
            '-DUSERVER_PG_INCLUDE_DIR=' + str(include),
            '-DSERVICELIB_NATIVE_POSTGRESQL_CLIENT=' + str(client)]
    args += ['-DPostgreSQLInternal_' + key + '=' + str(value) for key, value in parts.items()]
    output.mkdir(parents=True, exist_ok=True)
    toolchain = output / 'native-postgresql.cmake'
    with toolchain.open('w') as dest:
        for arg in args:
            key, value = arg[2:].split('=', 1)
            if key == 'USERVER_FEATURE_CHAOTIC_EXPERIMENTAL':
                continue
            if key == 'CMAKE_MODULE_PATH':
                dest.write('list(APPEND CMAKE_MODULE_PATH ' + json.dumps(value) + ')\n')
            else:
                dest.write('set(' + key + ' ' + json.dumps(value) + ' CACHE STRING "Native PostgreSQL Query dependency" FORCE)\n')
    confs = [
        'userver/*:tools.cmake:configure_args=' + json.dumps(args),
        '&:tools.cmake.cmaketoolchain:user_toolchain=' + json.dumps([str(toolchain)]),
        'userver/*:user.userver:postgresql_native=' + identity,
        'userver/*:tools.info.package_id:confs=' + json.dumps([
            'user.userver:chaotic_openapi', 'user.userver:sanitizer', 'user.userver:postgresql_native']),
    ]
    (output / 'native-postgresql.conan-args').write_text('\n'.join(confs) + '\n')
    (output / 'native-postgresql.manifest.json').write_text(json.dumps(
        {'version': version, 'identity': identity, 'files': files}, indent=2) + '\n')
    return identity


if __name__ == '__main__':
    native = os.environ.get('CPPSERVICELIB_PG_CONFIG')
    native = native or ('/usr/bin/pg_config' if Path('/usr/bin/pg_config').is_file() else shutil.which('pg_config'))
    if not native:
        raise SystemExit('PostgreSQL Query requires pg_config and server development libraries')
    configure(Path(sys.argv[1]).resolve(), sys.argv[2], Path(sys.argv[3]).resolve(),
              Path(sys.argv[4]).resolve(), Path(native))
