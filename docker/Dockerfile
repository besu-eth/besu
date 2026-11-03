# syntax=docker/dockerfile:1@sha256:2780b5c3bab67f1f76c781860de469442999ed1a0d7992a5efdf2cffc0e3d769

# Stage: Use Eclipse Temurin JRE
# eclipse-temurin OpenJDK 25.0.3 (2026-04-21 LTS)
FROM mirror.gcr.io/library/eclipse-temurin@sha256:68868d04fa9cfd5f5c6abec0b5cef86d8de2bf9c62c37c7d3e4f0f80f5cfd7ff AS java-base

# Remove the roots OpenJDK lists after the October 2026 eMudhra CA compromise
# (https://openjdk.org/emudhra): emSign ECC Root CA - G3, which OpenJDK identifies as related to the
# compromised intermediate CAs, and emSign Root CA - G1 as a precaution. emSign Root CA - G2 and AAA
# Certificate Services are also listed but are not in the Temurin store; they are matched anyway, so
# the step keeps working if a future base adds them. The emSign ECC Root CA - C3 and emSign Root CA -
# C1 roots (eMudhra Inc, US) are deliberately left trusted: OpenJDK does not list them, and the
# compromised intermediates chain to G3, not the C series. Matched by SHA-256 fingerprint, so this works
# whatever their alias; the build then re-checks by subject name, so a wrong fingerprint cannot ship
# a trusted root. Remove once the base drops them.
RUN set -eu; \
    DISTRUSTED_ROOTS="40:F6:AF:03:46:A9:9A:A1:CD:1D:55:5A:4E:9C:CE:62:C7:F9:63:46:03:EE:40:66:15:83:3D:C8:C8:D0:03:67|1A:A0:C2:70:9E:83:1B:D6:E3:B5:12:9A:00:BA:41:F7:EE:EF:02:08:72:F1:E6:50:4B:F0:F6:C3:F2:4F:3A:F3|86:A1:EC:BA:08:9C:4A:8D:3B:BE:27:34:C6:12:BA:34:1D:81:3E:04:3C:F9:E8:A8:62:CD:5C:57:A3:6B:BE:6B|D7:A7:A0:FB:5D:7E:27:31:D7:71:E9:48:4E:BC:DE:F7:1D:5F:0C:3E:0A:29:48:78:2B:C8:3E:E0:EA:69:9E:F4"; \
    KEYTOOL="$JAVA_HOME/bin/keytool"; \
    STORE="-keystore $JAVA_HOME/lib/security/cacerts -storepass changeit"; \
    $KEYTOOL -list $STORE > /dev/null; \
    $KEYTOOL -J-Duser.language=en -list -v $STORE \
      | awk -v roots="$DISTRUSTED_ROOTS" '/^Alias name: /{alias=substr($0,13)} $1 == "SHA256:" && $2 ~ "^(" roots ")$" {print alias}' \
      | while IFS= read -r alias; do echo "Removing $alias"; $KEYTOOL -delete $STORE -alias "$alias"; done; \
    if $KEYTOOL -J-Duser.language=en -list -v $STORE | grep -qE '^Owner:.*CN=(emSign ECC Root CA - G3|emSign Root CA - G1|emSign Root CA - G2|AAA Certificate Services)(,|$)'; then echo "A distrusted root is still in cacerts" >&2; exit 1; fi

# Stage: Final Besu image
# ubuntu 24.04.5 LTS (Noble Numbat)
FROM mirror.gcr.io/library/ubuntu:24.04@sha256:68ee22004a9109b44973313b02ff7b1de250f1de0f5ecb628d73f579aaff33ac
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
ADD --chown=besu:besu https://github.com/grafana/pyroscope-java/releases/download/v2.6.0/pyroscope.jar /opt/besu/pyroscope/pyroscope.jar
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
