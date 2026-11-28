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
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.BonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Reproduces the EIP-8252 deep-roll out-of-memory finding and locks in the streaming fix for {@code
 * PathBasedWorldStateProvider.rollFullWorldStateToBlockHash}.
 *
 * <p>Raising the default Bonsai trie-log retention to the EIP-8252 reorg retention window (262,144
 * blocks) means a single reconstruction roll can span that whole window. The old roll decoded every
 * trie log between the start state and the target into two {@code ArrayList}s and only applied them
 * once both lists were complete, so peak memory was O(roll depth) trie-log layers - about 49 GiB at
 * the full window for a realistic ~58 KiB/log - which an {@code eth_call}/{@code eth_getBalance} at
 * {@code head - 262144} or a deep {@code forkchoiceUpdated} could trigger on a default node,
 * killing it (the build ships {@code -XX:+ExitOnOutOfMemoryError} with no {@code -Xmx}).
 *
 * <p>{@link #deepRollHoldsAtMostOneTrieLogLayerRegardlessOfDepth()} asserts the fix: the number of
 * trie-log layers resident at once stays bounded (O(1)) and does not scale with roll depth. {@link
 * #deepRollReconstructsTheIdenticalStateRootInBothDirections()} asserts correctness: streaming the
 * logs in reproduces the exact, byte-for-byte canonical state root of a short chain, in both the
 * rollback and the roll-forward direction.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class Eip8252DeepRollMemoryReproTest {

  // A realistic trie log is on the order of tens of KiB; the finding measured ~58 KiB. The peak
  // layer COUNT below, multiplied by this, is the peak heap the roll holds for trie logs.
  private static final long REALISTIC_TRIE_LOG_BYTES = 58 * 1024L;

  private final BlockHeaderTestFixture blockBuilder = new BlockHeaderTestFixture();

  /**
   * Drives a genuine deep rollback through {@code rollFullWorldStateToBlockHash} and asserts that
   * at most one trie-log layer is ever resident, at both a shallow and a deep roll, so peak memory
   * does not scale with roll depth. Before the fix this peak equalled the roll depth.
   */
  @Test
  void deepRollHoldsAtMostOneTrieLogLayerRegardlessOfDepth() {
    final int shallowDepth = 500;
    final int deepDepth = 5_000;

    final int peakShallow = measurePeakConcurrentTrieLogLayers(shallowDepth);
    final int peakDeep = measurePeakConcurrentTrieLogLayers(deepDepth);

    // Bounded: a single layer is decoded, applied and released before the next is decoded.
    assertThat(peakShallow).isEqualTo(1);
    assertThat(peakDeep).isEqualTo(1);
    // Does NOT scale with depth: the deep roll (10x deeper) holds no more than the shallow one.
    assertThat(peakDeep).isLessThanOrEqualTo(peakShallow);
    // Sanity on the heap figures this translates to: O(1) layers, not O(depth).
    assertThat(peakDeep * REALISTIC_TRIE_LOG_BYTES)
        .isLessThan(deepDepth * REALISTIC_TRIE_LOG_BYTES);
  }

  /**
   * Runs the deep rollback for {@code depth} blocks and returns the maximum number of trie-log
   * layers held concurrently - i.e. decoded by {@code getTrieLogLayer} but not yet consumed by a
   * {@code rollBack}/{@code rollForward}. The roll is driven end to end through the real {@link
   * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider}
   * reconstruction path; only the trie-log store and the mutated world state are stubbed so the
   * concurrency can be counted deterministically, without relying on GC.
   */
  private int measurePeakConcurrentTrieLogLayers(final int depth) {
    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        (BonsaiWorldStateKeyValueStorage)
            new InMemoryKeyValueStorageProvider()
                .createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);

    final Blockchain blockchain = mock(Blockchain.class);
    final BonsaiWorldStateCacheManager cacheManager = mock(BonsaiWorldStateCacheManager.class);
    final TrieLogManager trieLogManager = mock(TrieLogManager.class);

    // A chain genesis(0) .. head(depth). head state must roll back `depth` layers to reach genesis.
    final Map<Hash, BlockHeader> headers = new HashMap<>();
    final BlockHeader genesis = blockBuilder.number(0).parentHash(Hash.ZERO).buildHeader();
    headers.put(genesis.getHash(), genesis);
    BlockHeader previous = genesis;
    for (int number = 1; number <= depth; number++) {
      final BlockHeader header =
          blockBuilder.number(number).parentHash(previous.getHash()).buildHeader();
      headers.put(header.getHash(), header);
      previous = header;
    }
    final BlockHeader head = previous;

    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenAnswer(invocation -> Optional.ofNullable(headers.get(invocation.getArgument(0))));
    when(blockchain.getChainHeadHeader()).thenReturn(head);

    // The state to roll: a mock world state positioned at head, with an accumulator we instrument.
    final AtomicInteger liveLayers = new AtomicInteger();
    final AtomicInteger peakLayers = new AtomicInteger();

    final PathBasedWorldStateUpdateAccumulator<?> accumulator =
        mock(PathBasedWorldStateUpdateAccumulator.class);
    // Each rollBack/rollForward consumes exactly the one layer it was handed.
    doAnswer(invocation -> liveLayers.decrementAndGet()).when(accumulator).rollBack(any());
    doAnswer(invocation -> liveLayers.decrementAndGet()).when(accumulator).rollForward(any());

    final BonsaiWorldState headState = mock(BonsaiWorldState.class);
    when(headState.blockHash()).thenReturn(head.getHash());
    // doReturn avoids a wildcard-capture mismatch on the generic updater() return type.
    doReturn(accumulator).when(headState).updater();
    when(headState.freezeStorage()).thenReturn(headState);
    when(headState.getWorldStateStorage()).thenReturn(worldStateStorage);

    // getTrieLogLayer decodes a fresh layer from storage each call (no cache). Count it as resident
    // from the moment it is handed out until its rollBack/rollForward consumes it.
    final TrieLog fakeLayer = mock(TrieLog.class);
    when(trieLogManager.getMaxLayersToLoad())
        .thenReturn((long) Math.max(depth, 262_144)); // never gate this roll on the depth guard
    when(trieLogManager.getTrieLogLayer(any(Hash.class)))
        .thenAnswer(
            invocation -> {
              final int live = liveLayers.incrementAndGet();
              peakLayers.updateAndGet(current -> Math.max(current, live));
              return Optional.of(fakeLayer);
            });

    final BonsaiWorldStateProvider archive =
        new BonsaiWorldStateProvider(
            cacheManager,
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG.getExtraStorageConfiguration(),
            trieLogManager,
            worldStateStorage,
            blockchain,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            EvmConfiguration.DEFAULT,
            new BonsaiCodeCache());

    when(cacheManager.getWorldState(genesis.getHash())).thenReturn(Optional.empty());
    when(cacheManager.getNearestWorldState(genesis)).thenReturn(Optional.of(headState));

    assertThat(archive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(genesis))).isPresent();

    // Every decoded layer was consumed: no leak, and the count is balanced.
    assertThat(liveLayers.get()).isZero();
    return peakLayers.get();
  }

  /**
   * Builds a short chain, serves its real trie logs, and reconstructs an older block's world state
   * from head (rollback) and then head's world state from the older block (roll-forward), both
   * through {@code rollFullWorldStateToBlockHash}. Asserts the reconstructed root equals the
   * canonical root computed the straightforward way - by executing the chain forward block by
   * block. The reconstruction itself re-verifies this: {@code persist} throws {@code
   * StateRootMismatchException} if the rolled root does not match the target header's state root.
   */
  @Test
  void deepRollReconstructsTheIdenticalStateRootInBothDirections() {
    final Address addressA = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final Address addressB = Address.fromHexString("0x2222222222222222222222222222222222222222");

    // Deterministic per-block mutations, applied identically when computing the reference roots and
    // when building the real chain, so both produce the same canonical state roots.
    final Consumer<WorldUpdater> block1 =
        updater -> {
          final MutableAccount a = updater.createAccount(addressA, 1, Wei.of(1_000L));
          a.setCode(Bytes.of(1, 2, 3));
          a.setStorageValue(UInt256.ONE, UInt256.valueOf(42));
          updater.commit();
        };
    final Consumer<WorldUpdater> block2 =
        updater -> {
          final MutableAccount a = updater.getAccount(addressA);
          a.setStorageValue(UInt256.ONE, UInt256.valueOf(43));
          a.setStorageValue(UInt256.valueOf(2), UInt256.valueOf(7));
          updater.commit();
        };
    final Consumer<WorldUpdater> block3 =
        updater -> {
          updater.createAccount(addressB, 1, Wei.of(5_000L));
          updater.getAccount(addressA).incrementBalance(Wei.of(10L));
          updater.commit();
        };

    // Reference: execute the chain forward on a throwaway world state to learn the canonical roots.
    final BonsaiWorldState reference =
        (BonsaiWorldState)
            createBonsaiInMemoryWorldStateArchive(mock(Blockchain.class)).getWorldState();
    reference.persist(null);
    final Hash root0 = reference.rootHash();
    block1.accept(reference.updater());
    reference.persist(null);
    final Hash root1 = reference.rootHash();
    block2.accept(reference.updater());
    reference.persist(null);
    final Hash root2 = reference.rootHash();
    block3.accept(reference.updater());
    reference.persist(null);
    final Hash root3 = reference.rootHash();

    assertThat(root0).isNotEqualTo(root3); // the chain actually changed state

    // Real chain: persist each block with a header carrying its canonical root, which both verifies
    // the root and writes the real trie log keyed by the block hash.
    final Blockchain blockchain = mock(Blockchain.class);
    final Map<Hash, BlockHeader> headers = new HashMap<>();
    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenAnswer(invocation -> Optional.ofNullable(headers.get(invocation.getArgument(0))));

    final BonsaiWorldStateProvider archive = createBonsaiInMemoryWorldStateArchive(blockchain);
    final BonsaiWorldState head = (BonsaiWorldState) archive.getWorldState();

    final BlockHeader header0 =
        blockBuilder.number(0).parentHash(Hash.ZERO).stateRoot(root0).buildHeader();
    headers.put(header0.getHash(), header0);
    head.persist(header0);

    block1.accept(head.updater());
    final BlockHeader header1 =
        blockBuilder.number(1).parentHash(header0.getHash()).stateRoot(root1).buildHeader();
    headers.put(header1.getHash(), header1);
    head.persist(header1);

    block2.accept(head.updater());
    final BlockHeader header2 =
        blockBuilder.number(2).parentHash(header1.getHash()).stateRoot(root2).buildHeader();
    headers.put(header2.getHash(), header2);
    head.persist(header2);

    block3.accept(head.updater());
    final BlockHeader header3 =
        blockBuilder.number(3).parentHash(header2.getHash()).stateRoot(root3).buildHeader();
    headers.put(header3.getHash(), header3);
    head.persist(header3);

    when(blockchain.getChainHeadHeader()).thenReturn(header3);

    // Rollback: reconstruct genesis state from head by rolling back three real trie logs.
    final Optional<Hash> rolledBackRoot =
        archive.getWorldState(withBlockHeaderAndUpdateNodeHead(header0)).map(ws -> ws.rootHash());
    assertThat(rolledBackRoot).contains(root0);

    // Roll-forward: reconstruct head state from genesis by rolling the same three logs forward.
    final Optional<Hash> rolledForwardRoot =
        archive.getWorldState(withBlockHeaderAndUpdateNodeHead(header3)).map(ws -> ws.rootHash());
    assertThat(rolledForwardRoot).contains(root3);
  }
}
