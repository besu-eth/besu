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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairResult;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class BonsaiWorldStateRepairLoggingTest {
  private static final String GENESIS =
      "/org/hyperledger/besu/ethereum/mainnet/parallelization/genesis-it.json";

  @Test
  void unexpectedFailureLogsCauseAndTargetIdentifiers() {
    final ExecutionContextTestFixture context = newContext();
    final BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) context.getStateArchive();
    final var header = context.getBlockchain().getChainHeadHeader();
    final RuntimeException failure = new IllegalStateException("fetch failed");
    archive.setMissingTrieNodeFetcher(
        (request, budget) -> {
          throw failure;
        });
    final BonsaiWorldStateKeyValueStorage storage = archive.getWorldStateKeyValueStorage();
    final var rootHash = header.getStateRoot();
    final var updater = storage.updater();
    updater.removeTrieNode(Bytes.EMPTY);
    updater.commitComposedOnly();
    storage.clearCrossBlockCache();
    archive.getWorldStateCacheManager().reset();

    final List<LogEvent> events =
        captureLogs(
            () ->
                assertThat(
                        archive.repairWorldState(
                            new WorldStateRepairRequest(header.getHash(), rootHash),
                            WorldStateRepairBudget.defaults()))
                    .isEqualTo(WorldStateRepairResult.FAILED));

    assertThat(events)
        .anySatisfy(
            event -> {
              assertThat(event.getLevel()).isEqualTo(Level.ERROR);
              assertThat(event.getMessage().getFormattedMessage())
                  .contains(header.getHash().toString(), rootHash.toString());
              assertThat(event.getThrown()).isSameAs(failure);
            });
  }

  @Test
  void expectedRefusalDoesNotLogAtError() {
    final ExecutionContextTestFixture context = newContext();
    final BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) context.getStateArchive();
    final var header = context.getBlockchain().getChainHeadHeader();
    final var unknownBlockHash = Hash.fromHexString("0x" + "12".repeat(32));

    final List<LogEvent> events =
        captureLogs(
            () ->
                assertThat(
                        archive.repairWorldState(
                            new WorldStateRepairRequest(unknownBlockHash, header.getStateRoot()),
                            WorldStateRepairBudget.defaults()))
                    .isEqualTo(WorldStateRepairResult.UNAVAILABLE));

    assertThat(events).noneSatisfy(event -> assertThat(event.getLevel()).isEqualTo(Level.ERROR));
  }

  private static ExecutionContextTestFixture newContext() {
    return ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS))
        .dataStorageFormat(DataStorageFormat.BONSAI)
        .dataStorageConfiguration(
            ImmutableDataStorageConfiguration.builder()
                .dataStorageFormat(DataStorageFormat.BONSAI)
                .extraStorageConfiguration(ImmutableExtraStorageConfiguration.builder().build())
                .build())
        .build();
  }

  @SuppressWarnings("BannedMethod")
  private static List<LogEvent> captureLogs(final Runnable action) {
    final Logger logger = (Logger) LogManager.getLogger(BonsaiWorldStateRepair.class);
    final List<LogEvent> events = new CopyOnWriteArrayList<>();
    final AbstractAppender appender =
        new AbstractAppender("repair-test-capture", null, null, false, Property.EMPTY_ARRAY) {
          @Override
          public void append(final LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    try {
      action.run();
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
    return events;
  }
}
