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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BonsaiWorldStateWitnessStorageTest {

  private final BonsaiWorldStateKeyValueStorage head =
      new BonsaiWorldStateKeyValueStorage(
          new InMemoryKeyValueStorageProvider(),
          new NoOpMetricsSystem(),
          DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);

  @AfterEach
  void tearDown() throws Exception {
    head.close();
  }

  @Test
  void shouldCloseWitnessWorldStateReleasesParentLayers() throws Exception {
    final int headSubscribers = head.subscribers.getSubscriberCount();
    final BonsaiWorldStateLayerStorage parentLayer = new BonsaiWorldStateLayerStorage(head);
    final BonsaiWorldStateWitnessStorage witnessStorage =
        new BonsaiWorldStateWitnessStorage(new NoOpMetricsSystem(), parentLayer);
    final BonsaiCodeCache codeCache = new BonsaiCodeCache();

    new BonsaiWorldState(
            witnessStorage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(
                witnessStorage, EvmConfiguration.DEFAULT, codeCache),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            WorldStateConfig.createStatefulConfigWithTrie(),
            codeCache)
        .close();
    parentLayer.close();

    assertThat(witnessStorage.isClosed.get()).isTrue();
    assertThat(parentLayer.isClosed.get()).isTrue();
    assertThat(head.subscribers.getSubscriberCount()).isEqualTo(headSubscribers);
  }

  @Test
  void readsCodeTheWayTheParentStoresIt() throws Exception {
    // PUSH1 0x5b; JUMPDEST; PUSH2 0x5b5b; JUMPDEST
    final Bytes code = Bytes.fromHexString("0x605b5b615b5b5b");
    final Hash codeHash = Hash.hash(code);
    head.updater().putCode(Hash.EMPTY, codeHash, code).commit();

    try (BonsaiWorldStateLayerStorage parentLayer = new BonsaiWorldStateLayerStorage(head);
        BonsaiWorldStateWitnessStorage witnessStorage =
            new BonsaiWorldStateWitnessStorage(new NoOpMetricsSystem(), parentLayer)) {
      final Code read = witnessStorage.getCode(codeHash, Hash.EMPTY).orElseThrow();

      assertThat(read.getBytes()).isEqualTo(code);
      assertThat(read.getJumpDestBitMask()).containsExactly(0b1000100L);
      assertThat(witnessStorage.getCodeBytes(codeHash, Hash.EMPTY)).contains(code);
    }
  }
}
