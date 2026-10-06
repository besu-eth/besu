#!/bin/bash
##
## Copyright contributors to Besu.
##
## Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
## the License. You may obtain a copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
## an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
## specific language governing permissions and limitations under the License.
##
## SPDX-License-Identifier: Apache-2.0
##

set -e  # Exit immediately if any command fails

# Detect architecture if not already set
if [ -z "$architecture" ]; then
  case $(uname -m) in
    x86_64)
      architecture="amd64"
      ;;
    aarch64|arm64)
      architecture="arm64"
      ;;
    arm*)
      architecture="arm64"
      ;;
    *)
      echo "Unsupported architecture: $(uname -m)"
      exit 1
      ;;
  esac
fi

export TEST_PATH=./tests
export GOSS_PATH=$TEST_PATH/goss-linux-${architecture}
export GOSS_OPTS="$GOSS_OPTS --format junit"
export GOSS_FILES_STRATEGY=cp

DOCKER_IMAGE=$1
DOCKER_FILE="${2:-$PWD/Dockerfile}"

# Write dev.json genesis to a temp file for use in docker tests.
# --network=dev is deprecated; pass the genesis directly via --genesis-file instead.
GENESIS_FILE=$(mktemp /tmp/besu-dev-genesis-XXXXXX.json)
trap 'rm -f "$GENESIS_FILE" 2>/dev/null || true' EXIT
cat > "$GENESIS_FILE" <<'GENESIS'
{
  "config": {
    "chainId": 1337,
    "londonBlock": 0,
    "contractSizeLimit": 2147483647,
    "ethash": {
      "fixeddifficulty": 100
    }
  },
  "nonce": "0x42",
  "baseFeePerGas":"0x0",
  "timestamp": "0x0",
  "extraData": "0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa",
  "gasLimit": "0x1fffffffffffff",
  "difficulty": "0x10000",
  "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
  "coinbase": "0x0000000000000000000000000000000000000000",
  "alloc": {
    "fe3b557e8fb62b89f4916b721be55ceb828dbd73": { "balance": "0xad78ebc5ac6200000" },
    "627306090abaB3A6e1400e9345bC60c78a8BEf57": { "balance": "90000000000000000000000" },
    "f17f52151EbEF6C7334FAD080c5704D77216b732": { "balance": "90000000000000000000000" }
  }
}
GENESIS
GENESIS_MOUNT="-v $GENESIS_FILE:/opt/besu/genesis/dev.json"
GENESIS_ARGS="--genesis-file=/opt/besu/genesis/dev.json --network-id=2018"

# Test for normal startup with ports opened
# we test that things listen on the right interface/port, not what interface the advertise
# hence we don't set p2p-host=0.0.0.0 because this sets what its advertising to devp2p; the important piece is that it
# defaults to listening on all interfaces
echo "Running test 01: normal startup with ports opened"
GOSS_FILES_PATH=$TEST_PATH/01 \
bash $TEST_PATH/dgoss run --sysctl net.ipv6.conf.all.disable_ipv6=1 $GENESIS_MOUNT $DOCKER_IMAGE \
$GENESIS_ARGS \
--rpc-http-enabled \
--rpc-ws-enabled \
--graphql-http-enabled \
> ./reports/01.xml

# Test for directory permissions
echo "Running test 02: directory permissions"
GOSS_FILES_PATH=$TEST_PATH/02 \
bash $TEST_PATH/dgoss run --sysctl net.ipv6.conf.all.disable_ipv6=1 $GENESIS_MOUNT -v besu-data:/var/lib/besu $DOCKER_IMAGE --data-path=/var/lib/besu \
$GENESIS_ARGS \
> ./reports/02.xml

# Test that Besu container started and entered main loop
echo "Running test 03: Besu container started and entered main loop"
GOSS_FILES_PATH=$TEST_PATH/03 \
bash $TEST_PATH/dgoss run --sysctl net.ipv6.conf.all.disable_ipv6=1 $GENESIS_MOUNT $DOCKER_IMAGE \
$GENESIS_ARGS \
--rpc-http-enabled \
> ./reports/03.xml

# Test that Besu version matches expected version (only when EXPECTED_VERSION is explicitly set)
if [ -n "$EXPECTED_VERSION" ]; then
  echo "Running test 04: Besu version is correct"
  GOSS_FILES_PATH=$TEST_PATH/04 \
  bash $TEST_PATH/dgoss run --sysctl net.ipv6.conf.all.disable_ipv6=1 \
  $GENESIS_MOUNT \
  -e EXPECTED_VERSION=$EXPECTED_VERSION \
  $DOCKER_IMAGE \
  $GENESIS_ARGS \
  > ./reports/04.xml
else
  echo "Skipping test 04: EXPECTED_VERSION not set"
fi

echo "All tests passed successfully"
