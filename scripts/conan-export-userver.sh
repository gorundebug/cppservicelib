#!/usr/bin/env bash
set -euo pipefail

userver_dir=${1:?userver source directory is required}
userver_version=${2:?userver version is required}

if [[ ! -f "$userver_dir/conanfile.py" ]]; then
  echo "userver Conan recipe is missing: $userver_dir/conanfile.py" >&2
  exit 2
fi

# userver's upstream recipe discovers export roots through Git. A normal
# checkout and a Docker build context (where BuildKit intentionally omits
# .git) must therefore see the same empty index. Keep the source tree
# read-only and provide temporary Git metadata through the environment.
git_metadata=$(mktemp -d)
trap 'rm -rf "$git_metadata"' EXIT
git init --bare --quiet "$git_metadata/repository.git"

GIT_DIR="$git_metadata/repository.git" \
GIT_WORK_TREE="$userver_dir" \
  conan export "$userver_dir" --version "$userver_version" \
    --user gorundebug --channel userver
