#!/usr/bin/env bash

if [[ -n "${DEPENDENCY_PROXY_DIR:-}" ]]; then
  proxy_client_host="${DEPENDENCY_PROXY_HOST:-localhost}"
  proxy_host="${DEPENDENCY_PROXY_DOCKER_HOST:-host.docker.internal}"
  proxy_port="${DEPENDENCY_PROXY_PORT:-18081}"
  proxy_base="http://${proxy_host}:${proxy_port}/repository"
  git_mirror_port="${DEPENDENCY_GIT_MIRROR_PORT:-18084}"
  git_mirror_base="http://${proxy_host}:${git_mirror_port}/cgi-bin/git"

  export DEPENDENCY_CONAN_HOME="${DEPENDENCY_PROXY_DIR}/conan2"
  export DEPENDENCY_DOCKER_REGISTRY="${proxy_client_host}:${DEPENDENCY_PROXY_DOCKER_PORT:-18083}"
  export DEPENDENCY_GITHUB_RAW_URL="${proxy_base}/github-raw"
  export DEPENDENCY_CONAN_REMOTE_URL="${proxy_base}/conan-proxy"
  export PIP_INDEX_URL="${proxy_base}/pypi-proxy/simple"
  export PIP_TRUSTED_HOST="${proxy_host}"
  export DEPENDENCY_APT_UBUNTU_ARCHIVE_URL="${proxy_base}/apt-ubuntu-archive"
  export DEPENDENCY_APT_UBUNTU_SECURITY_URL="${proxy_base}/apt-ubuntu-security"
  export DEPENDENCY_APT_UBUNTU_PORTS_URL="${proxy_base}/apt-ubuntu-ports"
  export USERVER_SOURCE_CONTEXT="${USERVER_SOURCE_CONTEXT:-${git_mirror_base}/github.com/userver-framework/userver.git#c9f77729c0edce7e423def2d4a4450aa7fc9d259}"
fi
