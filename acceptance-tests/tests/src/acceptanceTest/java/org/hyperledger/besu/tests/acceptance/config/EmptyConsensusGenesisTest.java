/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.tests.acceptance.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.math.BigInteger;
import java.util.Optional;

import org.junit.jupiter.api.Test;

/**
 * Verifies that a genesis file declaring no consensus mechanism (empty {@code "config": {}}) does
 * not crash node startup. The node must fall back to Proof-of-Stake (Merge) instead of throwing
 * {@code IllegalArgumentException("Unknown consensus mechanism defined")}, and the JSON-RPC
 * endpoint must be reachable end-to-end. This exercises the full {@code
 * BesuController.fromGenesisFile} → {@code MergeBesuControllerBuilder.build()} → {@code
 * createEthProtocolManager} chain, including the TTD=0 path through {@code
 * TransitionBestPeerComparator}.
 */
public class EmptyConsensusGenesisTest extends AcceptanceTestBase {

  private static final String EMPTY_CONSENSUS_GENESIS =
      """
      {
        "config": {},
        "nonce": "0x0",
        "timestamp": "0x0",
        "gasLimit": "0x1388",
        "difficulty": "0x400",
        "alloc": {}
      }
      """;

  @Test
  public void shouldStartAndFallBackToPoSWhenNoConsensusMechanismInGenesis() throws Exception {
    final BesuNode node =
        besu.createNode(
            "empty-consensus",
            builder ->
                builder
                    .devMode(false)
                    .genesisConfigProvider(__ -> Optional.of(EMPTY_CONSENSUS_GENESIS)));

    cluster.startConsoleCapture();

    // cluster.start() (not runNodeStart()) is required: only it applies the genesisConfigProvider.
    cluster.start(node);

    // JSON-RPC must be reachable; eth_blockNumber must return cleanly (genesis block 0).
    assertThat(node.execute(ethTransactions.blockNumber())).isEqualTo(BigInteger.ZERO);

    // The fallback MUST be logged, otherwise the user has no signal the genesis was incomplete.
    assertThat(cluster.getConsoleContents()).contains("No consensus mechanism detected");
  }
}
