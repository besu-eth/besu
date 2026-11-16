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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class PendingChainTest {
  private final BlockDataGenerator gen = new BlockDataGenerator();
  private final KeyValueStorage headers = new InMemoryKeyValueStorage();
  private final KeyValueStorage blocks = new InMemoryKeyValueStorage();
  private final KeyValueStorage chainStorage = new InMemoryKeyValueStorage();
  private PendingChain chain;

  @BeforeEach
  void setUp() {
    chain = open();
  }

  private PendingChain open() {
    return new PendingChain(headers, blocks, chainStorage, new MainnetBlockHeaderFunctions());
  }

  private List<BlockHeader> headers(final long from, final long to) {
    return LongStream.rangeClosed(from, to).mapToObj(gen::header).toList();
  }

  @Test
  void isEmptyAndHasNoLinkedTopInitially() {
    assertThat(chain.isEmpty()).isTrue();
    assertThat(chain.linkedTop()).isEmpty();
    assertThat(chain.header(5L)).isEmpty();
  }

  @Test
  void storedHeadersAreFoundByNumberAndHash() {
    final List<BlockHeader> stored = headers(10, 14);
    chain.store(stored);

    assertThat(chain.isEmpty()).isFalse();
    for (final BlockHeader header : stored) {
      assertThat(chain.header(header.getNumber())).contains(header);
      assertThat(chain.header(header.getHash())).contains(header);
    }
    assertThat(chain.header(15L)).isEmpty();
  }

  @Test
  void overwritingANumberDropsTheReplacedHash() {
    final BlockHeader first = gen.header(10);
    final BlockHeader second = gen.header(10);
    assertThat(second.getHash()).isNotEqualTo(first.getHash());

    chain.store(List.of(first));
    chain.store(List.of(second));

    assertThat(chain.header(10L)).contains(second);
    assertThat(chain.header(second.getHash())).contains(second);
    assertThat(chain.header(first.getHash())).isEmpty();
  }

  @Test
  void overwritingWithinOneBatchKeepsTheLastHeader() {
    final BlockHeader first = gen.header(10);
    final BlockHeader second = gen.header(10);

    chain.store(List.of(first, second));

    assertThat(chain.header(10L)).contains(second);
    assertThat(chain.header(first.getHash())).isEmpty();
  }

  @Test
  void removeAboveDeletesHeadersAndLowersLinkedTop() {
    final List<BlockHeader> stored = headers(10, 20);
    chain.store(stored);
    chain.setLinkedTop(20);

    chain.removeAbove(15);

    assertThat(chain.header(15L)).contains(stored.get(5));
    assertThat(chain.header(16L)).isEmpty();
    assertThat(chain.header(stored.get(10).getHash())).isEmpty();
    assertThat(chain.linkedTop()).isEqualTo(OptionalLong.of(15));
  }

  @Test
  void removeAboveKeepsALowerLinkedTop() {
    chain.store(headers(10, 20));
    chain.setLinkedTop(12);

    chain.removeAbove(15);

    assertThat(chain.linkedTop()).isEqualTo(OptionalLong.of(12));
  }

  @Test
  void removeAboveEverythingEmptiesTheChain() {
    chain.store(headers(10, 12));

    chain.removeAbove(9);

    assertThat(chain.isEmpty()).isTrue();
    assertThat(chain.header(10L)).isEmpty();
  }

  @Test
  void removeUpToDeletesTheLowestHeaders() {
    final List<BlockHeader> stored = headers(10, 20);
    chain.store(stored);

    chain.removeUpTo(14);

    assertThat(chain.header(14L)).isEmpty();
    assertThat(chain.header(stored.get(4).getHash())).isEmpty();
    assertThat(chain.header(15L)).contains(stored.get(5));
    assertThat(chain.isEmpty()).isFalse();

    chain.removeUpTo(20);

    assertThat(chain.isEmpty()).isTrue();
    assertThat(chain.header(20L)).isEmpty();
  }

  @Test
  void storingAfterRemovalExtendsTheRangeCorrectly() {
    chain.store(headers(10, 12));
    chain.removeUpTo(12);
    final List<BlockHeader> later = headers(20, 21);
    chain.store(later);

    chain.removeUpTo(20);

    assertThat(chain.header(20L)).isEmpty();
    assertThat(chain.header(21L)).contains(later.get(1));
  }

  @Test
  void contentAndLinkedTopSurviveReopening() {
    final List<BlockHeader> stored = headers(10, 14);
    chain.store(stored);
    chain.setLinkedTop(14);

    final PendingChain reopened = open();

    assertThat(reopened.linkedTop()).isEqualTo(OptionalLong.of(14));
    assertThat(reopened.header(12L)).contains(stored.get(2));
    assertThat(reopened.header(stored.get(4).getHash())).contains(stored.get(4));
    assertThat(reopened.isEmpty()).isFalse();
  }

  @Test
  void oldLayoutDataIsClearedOnOpen() {
    final KeyValueStorage oldHeaders = new InMemoryKeyValueStorage();
    final KeyValueStorage oldBlocks = new InMemoryKeyValueStorage();
    final KeyValueStorage oldChain = new InMemoryKeyValueStorage();
    final var tx = oldHeaders.startTransaction();
    tx.put(new byte[] {1}, new byte[] {2});
    tx.commit();
    final var blockTx = oldBlocks.startTransaction();
    blockTx.put(new byte[] {3}, new byte[] {4});
    blockTx.commit();
    final var chainTx = oldChain.startTransaction();
    chainTx.put(new byte[] {5}, new byte[] {6});
    chainTx.commit();

    final PendingChain opened =
        new PendingChain(oldHeaders, oldBlocks, oldChain, new MainnetBlockHeaderFunctions());

    assertThat(oldHeaders.containsKey(new byte[] {1})).isFalse();
    assertThat(oldBlocks.containsKey(new byte[] {3})).isFalse();
    assertThat(oldChain.containsKey(new byte[] {5})).isFalse();
    assertThat(opened.isEmpty()).isTrue();
    assertThat(opened.linkedTop()).isEmpty();
  }

  @Test
  void clearRemovesEverything() {
    final BlockHeader header = gen.header(10);
    chain.store(List.of(header));
    chain.setLinkedTop(10);

    chain.clear();

    assertThat(chain.isEmpty()).isTrue();
    assertThat(chain.linkedTop()).isEmpty();
    assertThat(chain.header(header.getHash())).isEmpty();
    assertThat(open().isEmpty()).isTrue();
  }
}
