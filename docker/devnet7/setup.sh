#!/usr/bin/env bash
#
# bal-devnet-7 — Besu (EL) + Lighthouse (CL) Docker setup
#
# Connects a local Besu execution client and Lighthouse beacon node to the
# ethPandaOps bal-devnet-7 testnet (BAL / EIP-7928, EIP-8037, eth/70, eth/71).
#
# Network spec: https://notes.ethereum.org/@ethpandaops/bal-devnet-7
# Network config: https://bal-devnet-7.ethpandaops.io/
#
# Usage examples:
#   # Start with Besu built from the current repo branch (default)
#   ./setup.sh start
#
#   # Custom Besu data directory (required mount path on host)
#   ./setup.sh start --data-dir /path/to/besu-data
#
#   # Force rebuild Besu from current branch before starting
#   ./setup.sh start --build-besu --data-dir /tmp/besu
#
#   # Build Besu image only (no containers)
#   ./setup.sh build
#
#   # Use ethPandaOps pre-built Besu instead of local branch build
#   ./setup.sh start --besu-image ethpandaops/besu:bal-devnet-7
#
#   # Follow Besu block import logs
#   ./setup.sh logs besu
#
#   # Follow only BAL/nonce/cache diagnostic lines (visible at INFO)
#   ./setup.sh logs-diag besu
#
#   # Stop and remove containers (data dirs are preserved)
#   ./setup.sh stop
#
#   # Restart Lighthouse only (Besu keeps running and syncing)
#   ./setup.sh restart-lighthouse
#
#   # Wipe Lighthouse data and restart checkpoint sync (Besu untouched; requires --confirm)
#   ./setup.sh resync-lighthouse --confirm
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- bal-devnet-7 network constants (ethPandaOps) ---
readonly CONFIG_BASE_URL="https://config.bal-devnet-7.ethpandaops.io"
readonly INVENTORY_URL="${CONFIG_BASE_URL}/api/v1/nodes/inventory"
readonly CHAIN_ID="7071885124"
readonly DEPOSIT_CONTRACT="0x00000000219ab540356cBB839Cbe05303d7705Fa"
readonly DEFAULT_CHECKPOINT_SYNC_URL="https://checkpoint-sync.bal-devnet-7.ethpandaops.io"
# Default EL bootnodes (bal-devnet-7 Besu nodes); override with BESU_BOOTNODES or --bootnodes.
readonly DEFAULT_BESU_BOOTNODES="enode://51e51e45320406c3b9498b0f8d4daa1b75b5a9f588b27b44e9e3977f3431f79ccdbc71dea1c7c7f96c6b2f66625bf982b4248b18d06305990e9239b58f0f01c9@178.105.51.90:30303?discport=30303,enode://67c314970954f08a217246dfdfb22f82d8e3729cb1fc3a489dc2f2320a670f734e8483e9cdb50d15c66a99c44a56d5c2f22bad5217f9fd246fe30ea2b99d0a1c@178.105.86.19:30303?discport=30303"

# --- Defaults (overridable via CLI) ---
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_DATA_DIR="${HOME}/.bal-devnet7"
DEFAULT_BESU_IMAGE="besu:devnet7-local"
DEFAULT_LIGHTHOUSE_IMAGE="ethpandaops/lighthouse:bal-devnet-7"

DATA_DIR=""
BESU_DATA_DIR=""
LIGHTHOUSE_DATA_DIR=""
CONFIG_DIR=""
JWT_DIR=""
BESU_IMAGE="${DEFAULT_BESU_IMAGE}"
LIGHTHOUSE_IMAGE="${DEFAULT_LIGHTHOUSE_IMAGE}"
CHECKPOINT_SYNC_URL="${DEFAULT_CHECKPOINT_SYNC_URL}"
BESU_BOOTNODES="${BESU_BOOTNODES:-}"
LIGHTHOUSE_BOOTNODES=""
P2P_HOST="${P2P_HOST:-0.0.0.0}"
EXTRA_STATIC_PEERS="${EXTRA_STATIC_PEERS:-}"
BESU_MAX_PEERS="${BESU_MAX_PEERS:-}"
BESU_USE_STATIC_NODES="${BESU_USE_STATIC_NODES:-false}"
FETCH_EL_BOOTNODES="${FETCH_EL_BOOTNODES:-false}"
REFRESH_CONFIG=false
BUILD_BESU=false
SKIP_BUILD_BESU=false
DETACH=true
LOG_SERVICE=""
LIGHTHOUSE_ONLY=false
RESYNC_LIGHTHOUSE_CONFIRM=false

usage() {
  sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
  cat <<'EOF'

Commands:
  start                 Download config (if needed) and start Besu + Lighthouse
  build                 Build Besu Docker image from the current repo branch
  stop                  Stop and remove containers (keeps data)
  restart               stop then start (or restart Lighthouse only with --lighthouse-only)
  restart-lighthouse    Restart Lighthouse container only (Besu keeps running)
  resync-lighthouse     Wipe Lighthouse datadir and restart checkpoint sync
  logs [service]        Follow logs (default: all; use 'besu' for EL block imports)
  logs-diag [service]   Follow diagnostic lines only (default: besu; grep [BAL-DIAG])
  status                Show container status
  config                Download/refresh network config only
  validate              Run 'docker compose config' to validate compose file

Options:
  -d, --data-dir PATH       Base directory for all node data (default: ~/.bal-devnet7)
      --besu-data-dir PATH  Besu data path override (default: <data-dir>/besu)
      --lighthouse-image IMAGE  Lighthouse Docker image (default: ethpandaops/lighthouse:bal-devnet-7)
      --besu-image IMAGE    Besu Docker image (default: besu:devnet7-local, built from repo)
      --build-besu          Build/rebuild Besu from current branch before start (or with build command)
      --no-build-besu       Skip auto-build when local Besu image is missing (fail instead)
      --checkpoint-sync-url URL  Lighthouse checkpoint sync beacon API URL
      --p2p-host HOST         Besu --p2p-host (default: 0.0.0.0; use public IP for routable nodes)
      --bootnodes ENODES      Override EL bootnodes (comma-separated; default: bal-devnet-7 Besu list)
      --fetch-el-bootnodes    Fetch EL bootnodes from ethPandaOps inventory instead of default list
      --use-static-nodes      Enable Besu --static-nodes-file=/config/static-nodes.json
      --static-peers ENODES   Extra EL enode URLs (comma-separated) added to static-nodes.json
      --max-peers N           Besu --max-peers (optional; no limit if omitted)
      --refresh-config        Re-download genesis/config from ethPandaOps
      --foreground            Run docker compose in foreground (no -d)
      --lighthouse-only       With restart: restart Lighthouse only (Besu untouched)
      --confirm               Required with resync-lighthouse to wipe Lighthouse data
  -h, --help                Show this help

Environment:
  BESU_DATA_DIR, LIGHTHOUSE_IMAGE, BESU_IMAGE, DATA_DIR, P2P_HOST, BESU_BOOTNODES,
  BESU_USE_STATIC_NODES, FETCH_EL_BOOTNODES, EXTRA_STATIC_PEERS, BESU_MAX_PEERS
  can also be set in the environment.
EOF
}

log() { printf '[devnet7] %s\n' "$*"; }
die() { log "ERROR: $*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

load_env_file() {
  local env_file="${SCRIPT_DIR}/.env"
  [[ -f "${env_file}" ]] || return 1
  while IFS='=' read -r key value; do
    [[ "${key}" =~ ^#.*$ || -z "${key}" ]] && continue
    case "${key}" in
      BESU_DATA_DIR|LIGHTHOUSE_DATA_DIR|CONFIG_DIR|JWT_DIR|BESU_IMAGE|LIGHTHOUSE_IMAGE)
        printf -v "${key}" '%s' "${value}"
        ;;
    esac
  done < "${env_file}"
  return 0
}

resolve_paths() {
  DATA_DIR="${DATA_DIR:-${DEFAULT_DATA_DIR}}"
  BESU_DATA_DIR="${BESU_DATA_DIR:-${DATA_DIR}/besu}"
  LIGHTHOUSE_DATA_DIR="${LIGHTHOUSE_DATA_DIR:-${DATA_DIR}/lighthouse}"
  CONFIG_DIR="${CONFIG_DIR:-${DATA_DIR}/config}"
  JWT_DIR="${JWT_DIR:-${DATA_DIR}/jwt}"
}

preserve_paths_from_env_file() {
  [[ -n "${DATA_DIR}" ]] && return 0
  load_env_file || return 0
  if [[ -n "${BESU_DATA_DIR}" ]]; then
    DATA_DIR="$(dirname "${BESU_DATA_DIR}")"
  fi
}

download_file() {
  local url="$1" dest="$2"
  log "Downloading $(basename "$dest") ..."
  curl -fsSL "$url" -o "$dest"
}

resolve_besu_bootnodes() {
  if [[ "${FETCH_EL_BOOTNODES}" == "true" ]]; then
    require_cmd jq
    log "Fetching EL bootnodes from ethPandaOps inventory ..."
    local inventory
    inventory="$(curl -fsSL "${INVENTORY_URL}")"
    BESU_BOOTNODES="$(echo "$inventory" | jq -r '[.ethereum_pairs[].execution.enode] | unique | .[:5] | join(",")')"
    [[ -n "${BESU_BOOTNODES}" ]] || die "Failed to fetch EL bootnodes from ${INVENTORY_URL}"
  else
    BESU_BOOTNODES="${BESU_BOOTNODES:-${DEFAULT_BESU_BOOTNODES}}"
  fi
}

fetch_el_peers() {
  require_cmd jq
  log "Fetching CL peers from ethPandaOps inventory ..."
  local inventory static_nodes_json
  inventory="$(curl -fsSL "${INVENTORY_URL}")"

  resolve_besu_bootnodes

  # Up to 5 unique CL ENRs to keep command lines manageable.
  LIGHTHOUSE_BOOTNODES="$(echo "$inventory" | jq -r '[.ethereum_pairs[].consensus.enr] | unique | .[:5] | join(",")')"

  [[ -n "${LIGHTHOUSE_BOOTNODES}" ]] || die "Failed to fetch CL bootnodes from ${INVENTORY_URL}"

  static_nodes_json="$(echo "$inventory" | jq -c '[.ethereum_pairs[].execution.enode] | unique')"
  if [[ -n "${EXTRA_STATIC_PEERS}" ]]; then
    static_nodes_json="$(echo "$static_nodes_json" | jq -c --arg extras "${EXTRA_STATIC_PEERS}" '
      . + ($extras | split(",") | map(gsub("^\\s+|\\s+$"; "")) | map(select(length > 0))) | unique
    ')"
  fi

  printf '%s\n' "${BESU_BOOTNODES}" > "${CONFIG_DIR}/besu-bootnodes.txt"
  printf '%s\n' "${LIGHTHOUSE_BOOTNODES}" > "${CONFIG_DIR}/lighthouse-bootnodes.txt"
  printf '%s\n' "${static_nodes_json}" > "${CONFIG_DIR}/static-nodes.json"

  local static_count
  static_count="$(echo "$static_nodes_json" | jq 'length')"
  log "Wrote ${static_count} EL static peer(s) to static-nodes.json"
}

validate_jwt_file() {
  local jwt_file="$1"
  [[ -f "${jwt_file}" ]] || die "JWT secret not found: ${jwt_file}"
  local jwt_len
  jwt_len="$(wc -c < "${jwt_file}" | tr -d ' ')"
  [[ "${jwt_len}" -eq 64 ]] || die "JWT secret must be 64 hex chars (32 bytes), got ${jwt_len} in ${jwt_file}"
  if ! grep -qE '^[0-9a-fA-F]{64}$' "${jwt_file}"; then
    die "JWT secret must be lowercase/uppercase hex only (no newlines) in ${jwt_file}"
  fi
}

ensure_jwt() {
  local jwt_file="${JWT_DIR}/execution-auth.jwt"
  local legacy_jwt_file="${JWT_DIR}/jwt.hex"
  mkdir -p "${JWT_DIR}"
  if [[ -f "${jwt_file}" ]]; then
    validate_jwt_file "${jwt_file}"
    return
  fi
  if [[ -f "${legacy_jwt_file}" ]]; then
    log "Migrating legacy jwt.hex to execution-auth.jwt ..."
    cp "${legacy_jwt_file}" "${jwt_file}"
    chmod 600 "${jwt_file}"
    validate_jwt_file "${jwt_file}"
    return
  fi
  log "Generating Engine API JWT secret ..."
  openssl rand -hex 32 | tr -d '\n' > "${jwt_file}"
  chmod 600 "${jwt_file}"
  validate_jwt_file "${jwt_file}"
}

sync_compose_path_env() {
  export JWT_DIR CONFIG_DIR BESU_DATA_DIR LIGHTHOUSE_DATA_DIR
}

besu_container_jwt_source() {
  docker inspect devnet7-besu \
    --format '{{range .Mounts}}{{if eq .Destination "/execution-auth.jwt"}}{{.Source}}{{end}}{{end}}' \
    2>/dev/null || true
}

verify_besu_jwt_mount() {
  local expected_jwt="${JWT_DIR}/execution-auth.jwt"
  local besu_jwt_source
  besu_jwt_source="$(besu_container_jwt_source)"
  [[ -n "${besu_jwt_source}" ]] || return 0
  if [[ "${besu_jwt_source}" != "${expected_jwt}" ]]; then
    die "JWT path mismatch: running Besu mounts ${besu_jwt_source} but .env JWT_DIR is ${JWT_DIR}. Restart both with the same --data-dir, or run: ./setup.sh stop && ./setup.sh start --data-dir $(dirname "$(dirname "${besu_jwt_source}")")"
  fi
  validate_jwt_file "${expected_jwt}"
}

download_network_config() {
  mkdir -p "${CONFIG_DIR}" "${BESU_DATA_DIR}" "${LIGHTHOUSE_DATA_DIR}"

  local besu_genesis="${CONFIG_DIR}/besu.json"
  local cl_config="${CONFIG_DIR}/config.yaml"
  local cl_genesis="${CONFIG_DIR}/genesis.ssz"
  local deposit_contract="${CONFIG_DIR}/deposit_contract.txt"
  local deposit_contract_block="${CONFIG_DIR}/deposit_contract_block.txt"

  if [[ "${REFRESH_CONFIG}" == "true" || ! -f "${besu_genesis}" ]]; then
    download_file "${CONFIG_BASE_URL}/el/besu.json" "${besu_genesis}"
  fi
  if [[ "${REFRESH_CONFIG}" == "true" || ! -f "${cl_config}" ]]; then
    download_file "${CONFIG_BASE_URL}/cl/config.yaml" "${cl_config}"
  fi
  if [[ "${REFRESH_CONFIG}" == "true" || ! -f "${cl_genesis}" ]]; then
    download_file "${CONFIG_BASE_URL}/cl/genesis.ssz" "${cl_genesis}"
  fi
  if [[ "${REFRESH_CONFIG}" == "true" || ! -f "${deposit_contract}" ]]; then
    download_file "${CONFIG_BASE_URL}/cl/deposit_contract.txt" "${deposit_contract}"
  fi
  if [[ "${REFRESH_CONFIG}" == "true" || ! -f "${deposit_contract_block}" ]]; then
    download_file "${CONFIG_BASE_URL}/cl/deposit_contract_block.txt" "${deposit_contract_block}"
  fi

  fetch_el_peers
}

write_env_file() {
  local env_file="${SCRIPT_DIR}/.env"
  cat > "${env_file}" <<EOF
# Auto-generated by setup.sh — do not edit manually; re-run setup.sh to regenerate.
CHAIN_ID=${CHAIN_ID}
BESU_IMAGE=${BESU_IMAGE}
LIGHTHOUSE_IMAGE=${LIGHTHOUSE_IMAGE}
BESU_DATA_DIR=${BESU_DATA_DIR}
LIGHTHOUSE_DATA_DIR=${LIGHTHOUSE_DATA_DIR}
CONFIG_DIR=${CONFIG_DIR}
JWT_DIR=${JWT_DIR}
P2P_HOST=${P2P_HOST}
BESU_BOOTNODES=${BESU_BOOTNODES}
LIGHTHOUSE_BOOTNODES=${LIGHTHOUSE_BOOTNODES}
LIGHTHOUSE_CHECKPOINT_SYNC_URL=${CHECKPOINT_SYNC_URL}
BESU_USE_STATIC_NODES=${BESU_USE_STATIC_NODES}
BESU_LOG_LEVEL=INFO
EOF
  if [[ -n "${BESU_MAX_PEERS}" ]]; then
    printf 'BESU_MAX_PEERS=%s\n' "${BESU_MAX_PEERS}" >> "${env_file}"
  fi
  log "Wrote ${env_file}"
}

validate_compose() {
  require_cmd docker
  local rendered
  rendered="$(cd "${SCRIPT_DIR}" && docker compose config)"
  if grep -qE -- '--max-peers=""|--p2p-peer-upper-bound=""' <<< "${rendered}"; then
    die "docker compose config passes empty --max-peers (set BESU_MAX_PEERS to an integer or omit it)"
  fi
  log "docker compose config: OK"
}

is_local_besu_image() {
  [[ "${BESU_IMAGE}" == "${DEFAULT_BESU_IMAGE}" ]]
}

besu_image_exists() {
  docker image inspect "${BESU_IMAGE}" >/dev/null 2>&1
}

besu_project_version() {
  local version=""
  version="$(
    cd "${REPO_ROOT}" && ./gradlew -q properties 2>/dev/null | awk -F': ' '/^version:/ {print $2; exit}'
  )" || true
  printf '%s' "${version}"
}

build_besu_image() {
  require_cmd docker
  [[ -f "${REPO_ROOT}/gradlew" ]] || die "Besu repo root not found at ${REPO_ROOT} (expected gradlew)"
  [[ -f "${REPO_ROOT}/docker/Dockerfile" ]] || die "Dockerfile not found at ${REPO_ROOT}/docker/Dockerfile"

  local version git_hash build_date docker_context
  version="$(besu_project_version)"
  [[ -n "${version}" ]] || version="devnet7-local"
  git_hash="$(git -C "${REPO_ROOT}" rev-parse --short=7 HEAD 2>/dev/null || echo "unknown")"
  build_date="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  docker_context="${REPO_ROOT}/build/docker-besu"
  local dockerfile="${docker_context}/Dockerfile"
  local dockerfile_build="${docker_context}/Dockerfile.build"
  # Avoid pulling docker/dockerfile:1 from Docker Hub (fails on some networks/IPv6 setups).
  sed '1{/^# syntax=docker\/dockerfile:1[[:space:]]*$/d;}' "${dockerfile}" > "${dockerfile_build}"

  log "Building Besu Docker image from repo: ${REPO_ROOT}"
  log "Git commit: ${git_hash}  version: ${version}  tag: ${BESU_IMAGE}"
  if ! (
    cd "${REPO_ROOT}"
    ./gradlew --no-daemon distDockerCopy
  ); then
    die "Gradle distDockerCopy failed — check JAVA_HOME and run './gradlew distDockerCopy' from ${REPO_ROOT}"
  fi

  # Regenerate after distDockerCopy may refresh the Dockerfile.
  sed '1{/^# syntax=docker\/dockerfile:1[[:space:]]*$/d;}' "${dockerfile}" > "${dockerfile_build}"

  docker build \
    --provenance=false \
    --build-arg "BUILD_DATE=${build_date}" \
    --build-arg "VERSION=${version}" \
    --build-arg "VCS_REF=${git_hash}" \
    -f "${dockerfile_build}" \
    -t "${BESU_IMAGE}" \
    "${docker_context}"

  log "Built ${BESU_IMAGE}"
}

ensure_besu_image() {
  if [[ "${SKIP_BUILD_BESU}" == "true" ]]; then
    besu_image_exists || die "Besu image not found: ${BESU_IMAGE} (run ./setup.sh build or omit --no-build-besu)"
    return
  fi

  if [[ "${BUILD_BESU}" == "true" ]]; then
    build_besu_image
    return
  fi

  if is_local_besu_image && ! besu_image_exists; then
    log "Local Besu image not found; building from current branch ..."
    build_besu_image
    return
  fi

  besu_image_exists || die "Besu image not found: ${BESU_IMAGE} (pull it or run ./setup.sh build)"
}

cmd_build() {
  require_cmd docker
  build_besu_image
}

cmd_start() {
  require_cmd docker curl openssl
  resolve_paths
  ensure_besu_image
  download_network_config
  ensure_jwt
  write_env_file
  validate_compose

  local up_args=(up)
  if [[ "${DETACH}" == "true" ]]; then
    up_args+=(-d)
  fi

  log "Starting Besu + Lighthouse (bal-devnet-7, chain ID ${CHAIN_ID}) ..."
  log "Besu image: ${BESU_IMAGE}"
  log "Besu data: ${BESU_DATA_DIR}"
  log "Lighthouse image: ${LIGHTHOUSE_IMAGE}"
  (cd "${SCRIPT_DIR}" && docker compose -f "${SCRIPT_DIR}/docker-compose.yml" "${up_args[@]}")

  if [[ "${DETACH}" == "true" ]]; then
    log ""
    log "Containers started. Watch Besu import blocks with:"
    log "  ${SCRIPT_DIR}/setup.sh logs besu"
    log "Diagnostic nonce/cache/BAL lines only:"
    log "  ${SCRIPT_DIR}/setup.sh logs-diag besu"
    log ""
    log "RPC endpoints:"
    log "  Besu HTTP RPC:      http://127.0.0.1:${BESU_RPC_PORT:-8545}"
    log "  Lighthouse HTTP:    http://127.0.0.1:${LIGHTHOUSE_HTTP_PORT:-5052}"
  fi
}

cmd_stop() {
  require_cmd docker
  (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml down)
  log "Stopped."
}

ensure_compose_env() {
  if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    return
  fi
  die ".env not found — run ./setup.sh start or ./setup.sh validate first"
}

cmd_restart_lighthouse() {
  local resync="${1:-false}"
  require_cmd docker
  ensure_compose_env
  preserve_paths_from_env_file
  resolve_paths
  sync_compose_path_env
  verify_besu_jwt_mount

  if [[ "${resync}" == "true" ]]; then
    if [[ "${RESYNC_LIGHTHOUSE_CONFIRM}" != "true" ]]; then
      die "resync-lighthouse deletes all Lighthouse data at ${LIGHTHOUSE_DATA_DIR}. Pass --confirm to proceed."
    fi
    [[ -n "${LIGHTHOUSE_DATA_DIR}" ]] || die "LIGHTHOUSE_DATA_DIR is empty"
    log "Stopping Lighthouse (Besu keeps running) ..."
    (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml stop lighthouse)
    log "Wiping Lighthouse data at ${LIGHTHOUSE_DATA_DIR} ..."
    rm -rf "${LIGHTHOUSE_DATA_DIR:?}"/*
    log "Starting Lighthouse fresh (checkpoint sync); Besu is untouched ..."
    (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml up -d --no-deps lighthouse)
  else
    log "Restarting Lighthouse only (Besu keeps running) ..."
    (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml up -d --no-deps --force-recreate lighthouse)
  fi

  log "Done. Watch Lighthouse sync with:"
  log "  ${SCRIPT_DIR}/setup.sh logs lighthouse"
}

cmd_logs() {
  require_cmd docker
  local service="${1:-}"
  (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml logs -f ${service:+"$service"})
}

cmd_logs_diag() {
  require_cmd docker
  local service="${1:-besu}"
  log "Following [BAL-DIAG] lines from ${service} (set BESU_LOG_LEVEL=DEBUG for per-tx detail) ..."
  (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml logs -f "${service}" 2>&1) \
    | grep -E '\[BAL-DIAG\]'
}

cmd_status() {
  require_cmd docker
  (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml ps)
}

parse_args() {
  local command="${1:-start}"
  shift || true

  while [[ $# -gt 0 ]]; do
    case "$1" in
      start|build|stop|restart|restart-lighthouse|resync-lighthouse|logs|logs-diag|status|config|validate)
        command="$1"
        shift
        ;;
      -d|--data-dir)
        DATA_DIR="$2"
        shift 2
        ;;
      --besu-data-dir)
        BESU_DATA_DIR="$2"
        shift 2
        ;;
      --lighthouse-image)
        LIGHTHOUSE_IMAGE="$2"
        shift 2
        ;;
      --besu-image)
        BESU_IMAGE="$2"
        shift 2
        ;;
      --build-besu)
        BUILD_BESU=true
        shift
        ;;
      --no-build-besu)
        SKIP_BUILD_BESU=true
        shift
        ;;
      --checkpoint-sync-url)
        CHECKPOINT_SYNC_URL="$2"
        shift 2
        ;;
      --p2p-host)
        P2P_HOST="$2"
        shift 2
        ;;
      --bootnodes)
        BESU_BOOTNODES="$2"
        shift 2
        ;;
      --fetch-el-bootnodes)
        FETCH_EL_BOOTNODES=true
        shift
        ;;
      --use-static-nodes)
        BESU_USE_STATIC_NODES=true
        shift
        ;;
      --static-peers)
        EXTRA_STATIC_PEERS="$2"
        shift 2
        ;;
      --max-peers)
        BESU_MAX_PEERS="$2"
        [[ "${BESU_MAX_PEERS}" =~ ^[0-9]+$ ]] || die "--max-peers requires a positive integer, got: ${BESU_MAX_PEERS}"
        shift 2
        ;;
      --refresh-config)
        REFRESH_CONFIG=true
        shift
        ;;
      --foreground)
        DETACH=false
        shift
        ;;
      --lighthouse-only)
        LIGHTHOUSE_ONLY=true
        shift
        ;;
      --confirm)
        RESYNC_LIGHTHOUSE_CONFIRM=true
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      besu|lighthouse)
        LOG_SERVICE="$1"
        shift
        ;;
      *)
        die "Unknown argument: $1 (use --help)"
        ;;
    esac
  done

  if [[ "${command}" == "restart-lighthouse" || "${command}" == "resync-lighthouse" || "${command}" == "restart" ]]; then
    preserve_paths_from_env_file
  fi
  resolve_paths

  case "${command}" in
    start) cmd_start ;;
    build) cmd_build ;;
    stop) cmd_stop ;;
    restart)
      if [[ "${LIGHTHOUSE_ONLY}" == "true" ]]; then
        cmd_restart_lighthouse false
      else
        cmd_stop
        cmd_start
      fi
      ;;
    restart-lighthouse) cmd_restart_lighthouse false ;;
    resync-lighthouse) cmd_restart_lighthouse true ;;
    logs) cmd_logs "${LOG_SERVICE}" ;;
    logs-diag) cmd_logs_diag "${LOG_SERVICE:-besu}" ;;
    status) cmd_status ;;
    config)
      require_cmd curl jq openssl
      resolve_paths
      download_network_config
      ensure_jwt
      log "Network config ready in ${CONFIG_DIR}"
      ;;
    validate)
      require_cmd docker curl jq openssl
      preserve_paths_from_env_file
      resolve_paths
      download_network_config
      ensure_jwt
      write_env_file
      validate_compose
      ;;
    *) die "Unknown command: ${command}" ;;
  esac
}

# Allow env overrides before CLI parsing
DATA_DIR="${DATA_DIR:-}"
BESU_DATA_DIR="${BESU_DATA_DIR:-}"
LIGHTHOUSE_IMAGE="${LIGHTHOUSE_IMAGE:-${DEFAULT_LIGHTHOUSE_IMAGE}}"
BESU_IMAGE="${BESU_IMAGE:-${DEFAULT_BESU_IMAGE}}"

parse_args "$@"
