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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hyperledger.besu.ethereum.eth.sync.backwardsync.ChainForTestCreator.prepareChain;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class InMemoryBackwardChainTest {

  public static final int HEIGHT = 20_000;
  public static final int ELEMENTS = 20;
  private List<Block> blocks;

  GenericKeyValueStorageFacade<Hash, BlockHeader> headersStorage;
  GenericKeyValueStorageFacade<Hash, Block> blocksStorage;
  GenericKeyValueStorageFacade<Hash, Hash> chainStorage;
  GenericKeyValueStorageFacade<String, BlockHeader> sessionDataStorage;

  @BeforeEach
  public void prepareData() {
    headersStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new BlocksHeadersConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    blocksStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new BlocksConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    chainStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new HashConvertor(),
            new InMemoryKeyValueStorage());
    sessionDataStorage =
        new GenericKeyValueStorageFacade<>(
            key -> key.getBytes(StandardCharsets.UTF_8),
            new BlocksHeadersConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());

    blocks = prepareChain(ELEMENTS, HEIGHT);
  }

  @Test
  public void shouldReturnFirstHeaderCorrectly() {
    BackwardChain backwardChain = createChainFromBlock(blocks.get(blocks.size() - 1));
    backwardChain.prependAncestorsHeader(blocks.get(blocks.size() - 2).getHeader());
    backwardChain.prependAncestorsHeader(blocks.get(blocks.size() - 3).getHeader());
    backwardChain.prependAncestorsHeader(blocks.get(blocks.size() - 4).getHeader());
    BlockHeader firstHeader = backwardChain.getFirstAncestorHeader().orElseThrow();
    assertThat(firstHeader).isEqualTo(blocks.get(blocks.size() - 4).getHeader());
  }

  @NotNull
  private BackwardChain createChainFromBlock(final Block pivot) {
    final BackwardChain backwardChain =
        new BackwardChain(headersStorage, blocksStorage, chainStorage, sessionDataStorage);
    backwardChain.appendTrustedBlock(pivot);
    return backwardChain;
  }

  @Test
  public void shouldSaveHeadersWhenHeightAndHashMatches() {
    BackwardChain backwardChain = createChainFromBlock(blocks.get(blocks.size() - 1));
    backwardChain.prependAncestorsHeader(blocks.get(blocks.size() - 2).getHeader());
    backwardChain.prependAncestorsHeader(blocks.get(blocks.size() - 3).getHeader());
    backwardChain.prependAncestorsHeader(blocks.get(blocks.size() - 4).getHeader());
    BlockHeader firstHeader = backwardChain.getFirstAncestorHeader().orElseThrow();
    assertThat(firstHeader).isEqualTo(blocks.get(blocks.size() - 4).getHeader());
  }

  @Test
  public void shouldDropFromTheEnd() {

    BackwardChain backwardChain = createChainFromBlock(blocks.get(blocks.size() - 1));
    backwardChain.prependAncestorsHeader(blocks.get(blocks.size() - 2).getHeader());
    backwardChain.prependAncestorsHeader(blocks.get(blocks.size() - 3).getHeader());

    BlockHeader firstHeader = backwardChain.getFirstAncestorHeader().orElseThrow();
    assertThat(firstHeader).isEqualTo(blocks.get(blocks.size() - 3).getHeader());

    backwardChain.dropFirstHeader();

    firstHeader = backwardChain.getFirstAncestorHeader().orElseThrow();
    assertThat(firstHeader).isEqualTo(blocks.get(blocks.size() - 2).getHeader());

    backwardChain.dropFirstHeader();

    firstHeader = backwardChain.getFirstAncestorHeader().orElseThrow();
    assertThat(firstHeader).isEqualTo(blocks.get(blocks.size() - 1).getHeader());
  }

  @Test
  public void shouldCreateChainFromScheduleAndFunctions() {
    final StorageProvider provider = new InMemoryKeyValueStorageProvider();
    BlockHeaderFunctions functions = new MainnetBlockHeaderFunctions();

    final BackwardChain chain = BackwardChain.from(provider, functions);
    assertThat(chain).isNotNull();

    chain.clear();
  }

  @Test
  public void shouldAddHeaderToQueue() {
    BackwardChain backwardChain = createChainFromBlock(blocks.get(3));
    Optional<Hash> firstHash = backwardChain.getFirstHashToAppend();
    assertThat(firstHash).isNotPresent();
    backwardChain.addNewHash(blocks.get(7).getHash());
    backwardChain.addNewHash(blocks.get(9).getHash());
    backwardChain.addNewHash(blocks.get(9).getHash());
    backwardChain.addNewHash(blocks.get(11).getHash());

    firstHash = backwardChain.getFirstHashToAppend();
    assertThat(firstHash).isPresent();
    assertThat(firstHash.orElseThrow()).isEqualTo(blocks.get(7).getHash());
    backwardChain.removeFromHashToAppend(firstHash.get());
    firstHash = backwardChain.getFirstHashToAppend();
    assertThat(firstHash).isPresent();
    assertThat(firstHash.orElseThrow()).isEqualTo(blocks.get(9).getHash());
    backwardChain.removeFromHashToAppend(firstHash.get());
    firstHash = backwardChain.getFirstHashToAppend();
    assertThat(firstHash).isPresent();
    assertThat(firstHash.orElseThrow()).isEqualTo(blocks.get(11).getHash());
    backwardChain.removeFromHashToAppend(firstHash.get());
  }

  @Test
  public void shouldChangeFirstAncestorIfPivotIsToFar() {
    BackwardChain backwardChain = createChainFromBlock(blocks.get(3));
    backwardChain.appendTrustedBlock(blocks.get(4));

    Optional<BlockHeader> firstAncestorHeader = backwardChain.getFirstAncestorHeader();
    assertThat(firstAncestorHeader).isPresent();
    assertThat(firstAncestorHeader.orElseThrow()).isEqualTo(blocks.get(3).getHeader());
    Optional<Block> pivot = backwardChain.getPivot();
    assertThat(pivot).isPresent();
    assertThat(pivot.orElseThrow()).isEqualTo(blocks.get(4));

    backwardChain.appendTrustedBlock(blocks.get(7));

    firstAncestorHeader = backwardChain.getFirstAncestorHeader();

    assertThat(firstAncestorHeader).isPresent();
    assertThat(firstAncestorHeader.orElseThrow()).isEqualTo(blocks.get(7).getHeader());
    pivot = backwardChain.getPivot();
    assertThat(pivot).isPresent();
    assertThat(pivot.orElseThrow()).isEqualTo(blocks.get(7));
  }

  @Test
  public void shouldDropOldestTrustedBalsBeyondTheCap() {
    final BlockAccessList bal = new BlockDataGenerator().blockAccessList(5);
    final BackwardChain chain =
        new BackwardChain(headersStorage, blocksStorage, chainStorage, sessionDataStorage, 3);

    for (int i = 0; i < 10; i++) {
      chain.appendTrustedBlock(blocks.get(i), Optional.of(bal));
    }

    for (int i = 0; i < 10; i++) {
      assertThat(chain.getTrustedBlockAccessList(blocks.get(i).getHash()).isPresent())
          .isEqualTo(i >= 7);
    }
  }

  @Test
  public void clearShouldDropTrustedBals() {
    final BlockAccessList bal = new BlockDataGenerator().blockAccessList(2);
    final BackwardChain chain = createChainFromBlock(blocks.get(0));
    chain.appendTrustedBlock(blocks.get(1), Optional.of(bal));
    assertThat(chain.getTrustedBlockAccessList(blocks.get(1).getHash())).contains(bal);

    chain.clear();

    assertThat(chain.getTrustedBlockAccessList(blocks.get(1).getHash())).isEmpty();
  }
}
