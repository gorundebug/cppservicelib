#!/usr/bin/env bash

dependency_script_root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source "${dependency_script_root}/scripts/userver-source.generated.env"

if [[ -z "${USERVER_SOURCE_CONTEXT:-}" ]]; then
  export USERVER_SOURCE_CONTEXT="${USERVER_REPOSITORY}#${USERVER_REVISION}"
fi

if [[ -n "${DEPENDENCY_PROXY_DIR:-}" ]]; then
  proxy_client_host="${DEPENDENCY_PROXY_HOST:-localhost}"
  proxy_host="${DEPENDENCY_PROXY_DOCKER_HOST:-host.docker.internal}"
  proxy_port="${DEPENDENCY_PROXY_PORT:-18081}"
  proxy_base="http://${proxy_host}:${proxy_port}/repository"
  git_mirror_port="${DEPENDENCY_GIT_MIRROR_PORT:-18084}"
  git_mirror_base="http://${proxy_host}:${git_mirror_port}/cgi-bin/git"

  export DEPENDENCY_CONAN_VOLUME="${DEPENDENCY_CONAN_VOLUME:-dependency-conan2}"
  export DEPENDENCY_DOCKER_REGISTRY="${proxy_client_host}:${DEPENDENCY_PROXY_DOCKER_PORT:-18083}"
  export DEPENDENCY_GITHUB_RAW_URL="${proxy_base}/github-raw"
  export DEPENDENCY_CONAN_REMOTE_URL="${proxy_base}/conan-proxy"
  export DEPENDENCY_CONAN_UPLOAD_URL="${proxy_base}/conan-hosted"
  export DEPENDENCY_CONAN_PUBLISH=1
  export DEPENDENCY_CONAN_CREDENTIAL_FILE="${DEPENDENCY_PROXY_DIR%/}/conan.publisher.credential"
  export PIP_INDEX_URL="${proxy_base}/pypi-proxy/simple"
  export PIP_TRUSTED_HOST="${proxy_host}"
  export DEPENDENCY_APT_UBUNTU_ARCHIVE_URL="${proxy_base}/apt-ubuntu-archive"
  export DEPENDENCY_APT_UBUNTU_SECURITY_URL="${proxy_base}/apt-ubuntu-security"
  export DEPENDENCY_APT_UBUNTU_PORTS_URL="${proxy_base}/apt-ubuntu-ports"
  if [[ "${USERVER_SOURCE_CONTEXT}" == "${USERVER_REPOSITORY}#${USERVER_REVISION}" ]]; then
    export USERVER_SOURCE_CONTEXT="${git_mirror_base}/${USERVER_MIRROR_REPOSITORY}#${USERVER_REVISION}"
  fi
fi
