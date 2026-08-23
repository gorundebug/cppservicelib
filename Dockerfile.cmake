# syntax=docker/dockerfile:1
FROM userver-source AS userver-source

FROM --platform=$TARGETPLATFORM ubuntu:24.04

ARG TARGETARCH
ARG SERVICEGEN_APT_UBUNTU_ARCHIVE_URL=
ARG SERVICEGEN_APT_UBUNTU_SECURITY_URL=
ARG SERVICEGEN_APT_UBUNTU_PORTS_URL=
RUN if [ -n "$SERVICEGEN_APT_UBUNTU_ARCHIVE_URL$SERVICEGEN_APT_UBUNTU_SECURITY_URL$SERVICEGEN_APT_UBUNTU_PORTS_URL" ]; then \
      find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) -exec sed -i \
        -e "s|http://archive.ubuntu.com/ubuntu|$SERVICEGEN_APT_UBUNTU_ARCHIVE_URL|g" \
        -e "s|http://security.ubuntu.com/ubuntu|$SERVICEGEN_APT_UBUNTU_SECURITY_URL|g" \
        -e "s|http://ports.ubuntu.com/ubuntu-ports|$SERVICEGEN_APT_UBUNTU_PORTS_URL|g" {} +; \
    fi
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

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

COPY --from=userver-source / /opt/userver

WORKDIR /workspace

ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV USERVER_SOURCE_DIR=/opt/userver
