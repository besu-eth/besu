/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;

class SynchronizationServiceImplTest {

  @Test
  void shouldAbortTrieDisableAndWarnWhenHashesAreMissing() {
    final Synchronizer synchronizer = mock(Synchronizer.class);
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
    final SyncState syncState = mock(SyncState.class);

    final PathBasedWorldStateProvider worldStateProvider = mock(PathBasedWorldStateProvider.class);
    final PathBasedWorldStateKeyValueStorage worldStateStorage =
        mock(PathBasedWorldStateKeyValueStorage.class);

    // Start with a config where trie is ENABLED (false)
    final WorldStateConfig worldStateConfig = WorldStateConfig.createStatefulConfigWithTrie();

    when(worldStateProvider.getWorldStateSharedSpec()).thenReturn(worldStateConfig);
    when(worldStateProvider.getWorldStateKeyValueStorage()).thenReturn(worldStateStorage);

    // Simulate missing root hash
    when(worldStateStorage.getWorldStateRootHash()).thenReturn(Optional.empty());
    when(worldStateStorage.getWorldStateBlockHash()).thenReturn(Optional.of(Hash.ZERO));

    final TestAppender appender = new TestAppender("TestAppender");
    appender.start();

    final Logger logger = (Logger) LogManager.getLogger(SynchronizationServiceImpl.class);
    logger.addAppender(appender);

    try {
      new SynchronizationServiceImpl(
              synchronizer, protocolContext, protocolSchedule, syncState, worldStateProvider)
          .disableWorldStateTrie();
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }

    // 1. Verify the specific warning message is logged
    assertThat(appender.getMessages())
        .anyMatch(message -> message.contains("World state trie migration aborted"));

    // 2. CRITICAL: Verify the trie was NOT disabled (it should still be false)
    assertThat(worldStateConfig.isTrieDisabled()).isFalse();
  }

  private static final class TestAppender extends AbstractAppender {
    private final List<String> messages = new ArrayList<>();

    private TestAppender(final String name) {
      super(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(final LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
    }

    private List<String> getMessages() {
      return messages;
    }
  }
}
