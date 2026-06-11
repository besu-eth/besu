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
package org.hyperledger.besu.tests.acceptance.bft;

import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.ImmutableSnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BftSyncAcceptanceTest extends ParameterizedBftTestBase {

  private static final int TARGET_BLOCK_HEIGHT = 70;
  private static final int SYNC_TIMEOUT_SECONDS = 100;

  static Stream<Arguments> syncModeTestParameters() {
    return Stream.of(SyncMode.FULL, SyncMode.SNAP)
        .flatMap(
            syncMode ->
                factoryFunctions()
                    .map(args -> Arguments.of(args.get()[0], args.get()[1], syncMode)));
  }

  @ParameterizedTest(name = "{index}: {0} with {2} sync")
  @MethodSource("syncModeTestParameters")
  public void shouldSyncValidatorNode(
      final String testName,
      final BftAcceptanceTestParameterization nodeFactory,
      final SyncMode syncMode)
      throws Exception {
    setUp(testName, nodeFactory);

    // Prefix node names with testName+syncMode so parallel parameterized cases
    // (ibft2/FULL, qbft/FULL, ibft2/SNAP) hash to distinct fixed ports and don't collide.
    final String prefix = testName + "-" + syncMode.name() + "-";

    // Create validator network with 4 validators
    final BesuNode validator1 = nodeFactory.createBonsaiNodeFixedPort(besu, prefix + "validator1");
    final BesuNode validator2 = nodeFactory.createBonsaiNodeFixedPort(besu, prefix + "validator2");
    final BesuNode validator3 = nodeFactory.createBonsaiNodeFixedPort(besu, prefix + "validator3");
    final BesuNode validator4 = nodeFactory.createBonsaiNodeFixedPort(besu, prefix + "validator4");

    // Enable snap server on validators 1-3 so they can serve world state data
    // This is required for SNAP sync mode to work
    final SnapSyncConfiguration snapServerEnabledConfig =
        ImmutableSnapSyncConfiguration.builder().isSnapServerEnabled(true).build();
    final SynchronizerConfiguration fullSyncWithSnapServer =
        SynchronizerConfiguration.builder()
            .syncMode(SyncMode.FULL)
            .syncMinimumPeerCount(1)
            .snapSyncConfiguration(snapServerEnabledConfig)
            .build();

    validator1.setSynchronizerConfiguration(fullSyncWithSnapServer);
    validator2.setSynchronizerConfiguration(fullSyncWithSnapServer);
    validator3.setSynchronizerConfiguration(fullSyncWithSnapServer);

    // Configure validator4 with the test's specified sync mode (FULL/SNAP)
    final SynchronizerConfiguration syncConfig =
        SynchronizerConfiguration.builder().syncMode(syncMode).syncMinimumPeerCount(1).build();
    validator4.setSynchronizerConfiguration(syncConfig);

    // Start first three validators
    cluster.start(validator1, validator2, validator3);

    validator1.verify(blockchain.minimumHeight(TARGET_BLOCK_HEIGHT, SYNC_TIMEOUT_SECONDS));
    // Add validator4 to cluster and start
    cluster.addNode(validator4);

    validator4.verify(blockchain.minimumHeight(TARGET_BLOCK_HEIGHT, 140));
  }
}
