# syntax=docker/dockerfile:1

# Stage: Use Eclipse Temurin JRE
FROM eclipse-temurin:25-jre AS java-base

# Stage: Final Besu image
FROM ubuntu:24.04
ARG VERSION="dev"
ENV NO_PROXY_CACHE="-o Acquire::BrokenProxy=true -o Acquire::http::No-Cache=true -o Acquire::http::Pipeline-Depth=0"

# Update and install dependencies without using any cache
RUN apt-get update $NO_PROXY_CACHE  && \
  # $NO_PROXY_CACHE must not be used here or otherwise will trigger a hadolint error
  apt-get -o Acquire::BrokenProxy=true -o Acquire::http::No-Cache=true -o Acquire::http::Pipeline-Depth=0 \
    --no-install-recommends -q --assume-yes install libjemalloc-dev=5.* && \
  # Clean apt cache
  apt-get clean  && \
  rm -rf /var/cache/apt/archives/* /var/cache/apt/archives/partial/*  && \
  rm -rf /var/lib/apt/lists/*  && \
  # Starting from version 23.10, Ubuntu comes with an "ubuntu" user with uid 1000. We need 1000 for besu.
  userdel ubuntu 2>/dev/null || true && rm -rf /home/ubuntu  && \
  # Ensure we use a stable UID for besu, as file permissions are tied to UIDs.
  useradd --uid 1000 --create-home --home-dir /opt/besu --shell /bin/bash --user-group --skel /etc/skel besu && \
  chmod 0755 /opt/besu

# Copy JRE from the java-base stage
COPY --from=java-base /opt/java/openjdk /opt/java/openjdk

ARG BESU_USER=besu
USER ${BESU_USER}
WORKDIR /opt/besu

COPY --chown=besu:besu besu /opt/besu/
# support for pyroscope
ADD --chown=besu:besu https://github.com/grafana/pyroscope-java/releases/download/v2.1.2/pyroscope.jar /opt/besu/pyroscope/pyroscope.jar
COPY --chown=besu:besu pyroscope.properties /etc/besu/pyroscope.properties
# https://grafana.com/docs/pyroscope/latest/configure-client/language-sdks/java/
# override the config vars as needed.
# eg: pyroscope.server.address becomes PYROSCOPE_SERVER_ADDRESS
# Use `BESU_OPTS=-javaagent:/opt/besu/pyroscope/pyroscope.jar` to enable pyroscope
ENV PYROSCOPE_CONFIGURATION_FILE=/etc/besu/pyroscope.properties

# Expose services ports
# 8545 HTTP JSON-RPC
# 8546 WS JSON-RPC
# 8547 HTTP GraphQL
# 8550 HTTP ENGINE JSON-RPC
# 8551 WS ENGINE JSON-RPC
# 30303 P2P
EXPOSE 8545 8546 8547 8550 8551 30303

# defaults for host interfaces
ENV BESU_RPC_HTTP_HOST=0.0.0.0
ENV BESU_RPC_WS_HOST=0.0.0.0
ENV BESU_GRAPHQL_HTTP_HOST=0.0.0.0
ENV BESU_PID_PATH="/tmp/pid"
ENV OTEL_RESOURCE_ATTRIBUTES="service.name=besu,service.version=$VERSION"
ENV JAVA_HOME="/opt/java/openjdk"
ENV PATH="/opt/besu/bin:${JAVA_HOME}/bin:${PATH}"

# The entry script just sets permissions as needed based on besu config
# and is replaced by the besu process running as besu user.
# Suppressing this warning as there's no risk here because the root user
# only sets permissions and does not continue running the main process.
# hadolint ignore=DL3002
USER root
RUN chmod +x /opt/besu/bin/besu-entry.sh

ENV BESU_USER_NAME=${BESU_USER}

ENTRYPOINT ["besu-entry.sh"]
HEALTHCHECK --start-period=5s --interval=5s --timeout=1s --retries=10 CMD bash -c "[ -f /tmp/pid ]"

# Build-time metadata as defined at http://label-schema.org
ARG BUILD_DATE
ARG VCS_REF
LABEL org.opencontainers.image.title="Besu" \
      org.opencontainers.image.description="Enterprise Ethereum client" \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.url="https://besu.hyperledger.org/" \
      org.opencontainers.image.source="https://github.com/besu-eth/besu.git" \
      org.opencontainers.image.revision=$VCS_REF \
      org.opencontainers.image.vendor="Hyperledger" \
      org.opencontainers.image.created=$BUILD_DATE \
      org.opencontainers.image.licenses="Apache 2.0"
