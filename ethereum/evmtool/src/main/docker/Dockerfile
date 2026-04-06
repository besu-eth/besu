# syntax=docker/dockerfile:1

# Stage: Use Eclipse Temurin JRE
FROM eclipse-temurin:25-jre AS java-base

# Stage: Final Besu EVMTool image
FROM ubuntu:24.04
ARG VERSION="dev"
ENV NO_PROXY_CACHE="-o Acquire::BrokenProxy=true -o Acquire::http::No-Cache=true -o Acquire::http::Pipeline-Depth=0"

# Update and install dependencies without using any cache
RUN apt-get update $NO_PROXY_CACHE  && \
  # $NO_PROXY_CACHE must not be used here or otherwise will trigger a hadolint error
  apt-get -o Acquire::BrokenProxy=true -o Acquire::http::No-Cache=true -o Acquire::http::Pipeline-Depth=0 \
    --no-install-recommends -q --assume-yes install libjemalloc-dev=5.* && \
  # Clean apt cache  \
  apt-get clean  && \
  rm -rf /var/cache/apt/archives/* /var/cache/apt/archives/partial/*  && \
  rm -rf /var/lib/apt/lists/*  && \
  # Starting from version 23.10, Ubuntu comes with an "ubuntu" user with uid 1000. We need 1000 for besu.
  userdel ubuntu 2>/dev/null || true && rm -rf /home/ubuntu  && \
  # Ensure we use a stable UID for besu, as file permissions are tied to UIDs.
  useradd --uid 1000 --create-home --home-dir /opt/besu-evmtool --shell /bin/bash --user-group --skel /etc/skel besu && \
  chmod 0755 /opt/besu-evmtool

# Copy JRE from the jre-base stage
COPY --from=java-base /opt/java/openjdk /opt/java/openjdk

USER besu
WORKDIR /opt/besu-evmtool

COPY --chown=besu:besu besu-evmtool /opt/besu-evmtool/

ENV JAVA_HOME="/opt/java/openjdk"
ENV PATH="/opt/besu-evmtool/bin:${JAVA_HOME}/bin:${PATH}"
ENTRYPOINT ["evmtool"]

# Build-time metadata as defined at http://label-schema.org
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION
LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.name="Besu EVMTool" \
      org.label-schema.description="EVM Execution Tool" \
      org.label-schema.url="https://besu.hyperledger.org/" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/besu-eth/besu.git" \
      org.label-schema.vendor="Hyperledger" \
      org.label-schema.version=$VERSION \
      org.label-schema.schema-version="1.0"