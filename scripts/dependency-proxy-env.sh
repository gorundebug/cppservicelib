#!/usr/bin/env bash

if [[ -n "${SERVICEGEN_DEPENDENCY_PROXY_DIR:-}" ]]; then
  servicegen_proxy_docker_host="${SERVICEGEN_DEPENDENCY_PROXY_DOCKER_HOST:-host.docker.internal}"
  servicegen_proxy_port="${SERVICEGEN_DEPENDENCY_PROXY_PORT:-${SERVICEGEN_NEXUS_PORT:-18081}}"
  servicegen_proxy_base="http://${servicegen_proxy_docker_host}:${servicegen_proxy_port}/repository"
  servicegen_git_mirror_port="${SERVICEGEN_GIT_MIRROR_PORT:-18084}"
  servicegen_git_mirror="http://${servicegen_proxy_docker_host}:${servicegen_git_mirror_port}/cgi-bin/git"

  export SERVICEGEN_CONAN_HOME="${SERVICEGEN_DEPENDENCY_PROXY_DIR}/conan2"
  export SERVICEGEN_GITHUB_RAW_URL="${servicegen_proxy_base}/github-raw"
  export SERVICEGEN_CONAN_REMOTE_URL="${servicegen_proxy_base}/conan-proxy"
  export PIP_INDEX_URL="${servicegen_proxy_base}/pypi-proxy/simple"
  export PIP_TRUSTED_HOST="${servicegen_proxy_docker_host}"
  export SERVICEGEN_APT_UBUNTU_ARCHIVE_URL="${servicegen_proxy_base}/apt-ubuntu-archive"
  export SERVICEGEN_APT_UBUNTU_SECURITY_URL="${servicegen_proxy_base}/apt-ubuntu-security"
  export SERVICEGEN_APT_UBUNTU_PORTS_URL="${servicegen_proxy_base}/apt-ubuntu-ports"
  if [[ -z "${USERVER_SOURCE_CONTEXT:-}" ]]; then
    servicegen_proxy_script_dir="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
    servicegen_userver_revision="$(sed -nE \
      's|.*userver\.git#([0-9a-f]+).*|\1|p' \
      "${servicegen_proxy_script_dir}/../docker-compose.cmake.yml" | head -n 1)"
    if [[ -n "${servicegen_userver_revision}" ]]; then
      export USERVER_SOURCE_CONTEXT="${servicegen_git_mirror}/github.com/userver-framework/userver.git#${servicegen_userver_revision}"
    fi
  fi
fi
