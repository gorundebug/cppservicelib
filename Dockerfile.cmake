ARG DEPENDENCY_DOCKER_REGISTRY=docker.io
FROM userver-source AS userver-source
FROM --platform=$TARGETPLATFORM ${DEPENDENCY_DOCKER_REGISTRY}/library/ubuntu:24.04

ARG TARGETARCH
ARG DEPENDENCY_APT_UBUNTU_ARCHIVE_URL=
ARG DEPENDENCY_APT_UBUNTU_SECURITY_URL=
ARG DEPENDENCY_APT_UBUNTU_PORTS_URL=
ARG DEPENDENCY_CONAN_REMOTE_URL=
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=
RUN if [ -n "$DEPENDENCY_APT_UBUNTU_ARCHIVE_URL$DEPENDENCY_APT_UBUNTU_SECURITY_URL$DEPENDENCY_APT_UBUNTU_PORTS_URL" ]; then \
      find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) -exec sed -i \
        -e "s|http://archive.ubuntu.com/ubuntu|$DEPENDENCY_APT_UBUNTU_ARCHIVE_URL|g" \
        -e "s|http://security.ubuntu.com/ubuntu|$DEPENDENCY_APT_UBUNTU_SECURITY_URL|g" \
        -e "s|http://ports.ubuntu.com/ubuntu-ports|$DEPENDENCY_APT_UBUNTU_PORTS_URL|g" {} +; \
    fi
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC
ENV DEPENDENCY_CONAN_REMOTE_URL=${DEPENDENCY_CONAN_REMOTE_URL}

COPY docker/userver-packages-ubuntu-24.04.txt /tmp/userver-packages.txt

RUN rm -f /etc/apt/apt.conf.d/docker-clean
RUN --mount=type=cache,id=servicegen-apt-lists-${TARGETARCH},target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,id=servicegen-apt-cache-${TARGETARCH},target=/var/cache/apt,sharing=locked \
    apt-get update \
    && xargs apt-get install --yes --no-install-recommends \
       ca-certificates locales python3-pip \
       < /tmp/userver-packages.txt \
    && locale-gen en_US.UTF-8 \
    && rm -f /tmp/userver-packages.txt

RUN python3 -m venv /opt/conan \
    && PIP_TRUSTED_HOST="$PIP_TRUSTED_HOST" \
       /opt/conan/bin/pip install --no-cache-dir --index-url "$PIP_INDEX_URL" \
       conan==2.31.1
ENV PATH=/opt/conan/bin:$PATH
ENV CONAN_HOME=/conan
ENV PIP_INDEX_URL=${PIP_INDEX_URL}
ENV PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}

COPY --from=userver-source / /tmp/userver-source
RUN set -eu; \
    source_dir=/tmp/userver-source; \
    archive=$(find "$source_dir" -mindepth 1 -maxdepth 1 -type f \( -name context -o -name '*.tar' -o -name '*.tar.gz' -o -name '*.tgz' -o -name '*.tar.xz' \) -print -quit); \
    if [ -n "$archive" ]; then \
      mkdir -p /tmp/userver-archive; \
      tar -xf "$archive" -C /tmp/userver-archive; \
      source_dir=/tmp/userver-archive; \
    fi; \
    manifest=$(find "$source_dir" -type f -name conanfile.py -print -quit); \
    if [ -z "$manifest" ]; then echo "userver source context has no conanfile.py" >&2; exit 1; fi; \
    source_dir=${manifest%/conanfile.py}; \
    mkdir -p /opt/userver; \
    cp -a "$source_dir/." /opt/userver/; \
    rm -rf /tmp/userver-source /tmp/userver-archive

WORKDIR /workspace

ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV USERVER_SOURCE_DIR=/opt/userver
