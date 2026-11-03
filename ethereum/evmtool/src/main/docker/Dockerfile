# syntax=docker/dockerfile:1@sha256:2780b5c3bab67f1f76c781860de469442999ed1a0d7992a5efdf2cffc0e3d769

# Stage: Use Eclipse Temurin JRE
# eclipse-temurin OpenJDK 25.0.2 (2026-01-20 LTS)
FROM mirror.gcr.io/library/eclipse-temurin@sha256:1bda4d9e668f44f399abed30636c34e0befb727408fba27b1e6aaefcf9df346b AS java-base

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

# Stage: Final Besu EVMTool image
# ubuntu 24.04.5 LTS (Noble Numbat)
FROM mirror.gcr.io/library/ubuntu:24.04@sha256:68ee22004a9109b44973313b02ff7b1de250f1de0f5ecb628d73f579aaff33ac
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