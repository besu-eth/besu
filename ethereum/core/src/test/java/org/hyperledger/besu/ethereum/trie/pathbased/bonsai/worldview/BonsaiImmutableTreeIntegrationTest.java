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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitter;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.common.StateRootMismatchException;
import org.hyperledger.besu.ethereum.trie.immutabletree.ImmutableTreeCache;
import org.hyperledger.besu.ethereum.trie.immutabletree.ImmutableTreeMerkleTrie;
import org.hyperledger.besu.ethereum.trie.immutabletree.TreeHandle;
import org.hyperledger.besu.ethereum.trie.immutabletree.TreeKind;
import org.hyperledger.besu.ethereum.trie.immutabletree.TreeRole;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs the same blocks through a Bonsai node computing state roots on the immutable tree cache and
 * one computing them on classic tries, and checks that roots, stored trie nodes and cache bindings
 * agree across head persists, payload validation, reorgs and pruning.
 */
class BonsaiImmutableTreeIntegrationTest {

  private static final List<Address> ADDRESSES =
      IntStream.range(1, 90)
          .mapToObj(i -> Address.fromHexString(String.format("0x%040x", i * 7919L)))
          .toList();

  private MutableBlockchain blockchain;
  private BlockHeader chainHead;
  private Participant cached;
  private Participant reference;

  private record Participant(
      BonsaiWorldStateKeyValueStorage storage, BonsaiWorldStateProvider provider) {

    BonsaiWorldState head() {
      return (BonsaiWorldState) provider.getWorldState();
    }

    ImmutableTreeCache cache() {
      return provider.getImmutableTreeCache().orElseThrow();
    }

    BonsaiWorldState frozenAt(final BlockHeader header) {
      return (BonsaiWorldState)
          provider
              .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(header))
              .orElseThrow();
    }
  }

  @BeforeEach
  void setUp() {
    final Block genesis =
        new Block(
            new BlockHeaderTestFixture().number(0).stateRoot(Hash.EMPTY_TRIE_HASH).buildHeader(),
            BlockBody.empty());
    blockchain = InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesis);
    chainHead = genesis.getHeader();
    cached = participant(cacheConfig(true, 512, 64 << 20));
    reference = participant(cacheConfig(false, 512, 64 << 20));
  }

  private static ExtraStorageConfiguration cacheConfig(
      final boolean enabled, final int pruneAfterBlocks, final long maxBytes) {
    return ImmutableExtraStorageConfiguration.builder()
        .unstable(
            ImmutableExtraStorageConfiguration.Unstable.builder()
                .bonsaiImmutableTreeCacheEnabled(enabled)
                .bonsaiImmutableTreeCachePruneAfterBlocks(pruneAfterBlocks)
                .bonsaiImmutableTreeCacheMaxCapacity(maxBytes)
                .build())
        .build();
  }

  private Participant participant(final ExtraStorageConfiguration configuration) {
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    return participant(storage, configuration);
  }

  private Participant participant(
      final BonsaiWorldStateKeyValueStorage storage,
      final ExtraStorageConfiguration configuration) {
    final BonsaiWorldStateProvider provider =
        new BonsaiWorldStateProvider(
            storage,
            blockchain,
            configuration,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            new BonsaiCodeCache());
    return new Participant(storage, provider);
  }

  /** Applies the pseudo-random changes of block {@code seed}: the same for every world state. */
  private static void applyChanges(final MutableWorldState worldState, final long seed) {
    final Random random = new Random(seed);
    final WorldUpdater updater = worldState.updater();
    // Deleting and recreating an account within one block is left out: rolling such a block's
    // trie log forward fails in Bonsai whatever computes the roots
    final Set<Address> touched = new HashSet<>();
    final Set<Address> deleted = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      final Address address = ADDRESSES.get(random.nextInt(ADDRESSES.size()));
      final int action = random.nextInt(100);
      if (deleted.contains(address)) {
        continue;
      }
      if (action < 5) {
        if (!touched.contains(address)) {
          updater.deleteAccount(address);
          deleted.add(address);
        }
        continue;
      }
      touched.add(address);
      final MutableAccount account = updater.getOrCreate(address);
      if (action < 10) {
        account.clearStorage();
      } else if (action < 50) {
        for (int slot = 0; slot < 1 + random.nextInt(8); slot++) {
          account.setStorageValue(
              UInt256.valueOf(random.nextInt(60)),
              random.nextInt(5) == 0
                  ? UInt256.ZERO
                  : UInt256.valueOf(random.nextLong() & Long.MAX_VALUE));
        }
      } else if (action < 60) {
        account.setCode(Bytes.of(random.nextInt(256), 0x60, 0x00, random.nextInt(256)));
      }
      account.setBalance(Wei.of(1 + random.nextInt(1_000_000)));
      account.incrementNonce();
    }
    updater.commit();
  }

  /** The root the classic participant computes for the changes pending in {@code worldState}. */
  private static Hash computeRoot(final BonsaiWorldState worldState) {
    return new DefaultStateRootCommitter().compute(worldState, null, worldState.updater()).root();
  }

  private static BlockHeader childHeader(
      final BlockHeader parent, final Hash stateRoot, final long seed) {
    return new BlockHeaderTestFixture()
        .parentHash(parent.getHash())
        .number(parent.getNumber() + 1)
        .stateRoot(stateRoot)
        .extraData(Bytes.ofUnsignedLong(seed))
        .buildHeader();
  }

  /** Imports a block on both heads; the cached head verifies the reference root on persist. */
  private BlockHeader importBlock(final long seed) {
    applyChanges(reference.head(), seed);
    applyChanges(cached.head(), seed);
    final Hash root = computeRoot(reference.head());
    final BlockHeader header = childHeader(chainHead, root, seed);
    reference.head().persist(header);
    cached.head().persist(header);
    blockchain.appendBlock(new Block(header, BlockBody.empty()), List.of());
    chainHead = header;
    assertThat(cached.head().rootHash()).isEqualTo(root);
    assertThat(cached.cache().boundState(TreeRole.HEAD).map(TreeHandle::rootHash))
        .contains(Bytes32.wrap(root.getBytes()));
    return header;
  }

  @Test
  void stateRootTriesAreImmutableTrees() {
    assertThat(cached.head().createAccountStateTrie()).isInstanceOf(ImmutableTreeMerkleTrie.class);
    assertThat(cached.head().createStorageTrie(Hash.ZERO, Hash.EMPTY_TRIE_HASH))
        .isInstanceOf(ImmutableTreeMerkleTrie.class);
    assertThat(reference.head().createAccountStateTrie())
        .isInstanceOf(ParallelStoredMerklePatriciaTrie.class);
    assertThat(reference.provider().getImmutableTreeCache()).isEmpty();
  }

  @Test
  void headChainMatchesClassicTriesAndStoresTheSameTrie() {
    for (long seed = 1; seed <= 40; seed++) {
      importBlock(seed);
      if (seed % 10 == 0) {
        assertDatabasesAgree(chainHead.getStateRoot());
      }
    }
    final ImmutableTreeCache.Stats stats = cached.cache().stats();
    // every block after the first started from the root the previous one registered
    assertThat(stats.cachedOpens()).isGreaterThanOrEqualTo(39);
    assertThat(stats.headStorageBindings()).isPositive();
    assertThat(stats.head()).isEqualTo(Bytes32.wrap(chainHead.getStateRoot().getBytes()));
  }

  @Test
  void aggressivePruningKeepsRootsAndStorageCorrect() {
    // a budget nothing fits in: every prune unloads all it can and drops every other root
    cached = participant(cacheConfig(true, 1, 1));
    long unloaded = 0;
    for (long seed = 100; seed < 130; seed++) {
      importBlock(seed);
      unloaded += cached.cache().prune().unloadedSubtrees();
      assertThat(cached.cache().stats().storageRoots()).isZero();
    }
    assertThat(unloaded).isPositive();
    assertThat(cached.cache().stats().lastPrune().windowBlocks()).isZero();
    assertDatabasesAgree(chainHead.getStateRoot());
  }

  @Test
  void newPayloadThenForkchoiceBindsBothRolesToOneTree() {
    for (long seed = 200; seed < 205; seed++) {
      importBlock(seed);
    }
    final Bytes32 headRoot = Bytes32.wrap(chainHead.getStateRoot().getBytes());
    final long seed = 205;

    // payload validation on a frozen copy of the head
    applyChanges(reference.head(), seed);
    final Hash root = computeRoot(reference.head());
    final BlockHeader header = childHeader(chainHead, root, seed);
    try (BonsaiWorldState payload = cached.frozenAt(chainHead)) {
      applyChanges(payload, seed);
      payload.persist(header);
    }
    final TreeHandle newPayload = cached.cache().boundState(TreeRole.NEW_PAYLOAD).orElseThrow();
    assertThat(newPayload.rootHash()).isEqualTo(Bytes32.wrap(root.getBytes()));
    assertThat(cached.cache().boundState(TreeRole.HEAD).orElseThrow().rootHash())
        .isEqualTo(headRoot);
    assertThat(cached.cache().stats().newPayloadStorageBindings()).isPositive();

    // forkchoice: the head computes the same root and binds the tree the payload registered
    reference.head().persist(header);
    applyChanges(cached.head(), seed);
    cached.head().persist(header);
    blockchain.appendBlock(new Block(header, BlockBody.empty()), List.of());
    chainHead = header;
    assertThat(cached.cache().boundState(TreeRole.HEAD)).containsSame(newPayload);
    assertDatabasesAgree(root);
  }

  @Test
  void frozenRootRecomputeRegistersAFork() {
    importBlock(300);
    try (BonsaiWorldState frozen = cached.frozenAt(chainHead)) {
      applyChanges(frozen, 301);
      final Hash root = frozen.rootHash();
      applyChanges(reference.head(), 301);
      assertThat(root).isEqualTo(computeRoot(reference.head()));
      // block building candidates and simulations are registered, not bound
      assertThat(cached.cache().lookup(TreeKind.STATE, Bytes32.wrap(root.getBytes()))).isPresent();
      assertThat(cached.cache().boundState(TreeRole.NEW_PAYLOAD)).isEmpty();
    }
  }

  @Test
  void restartedNodeStartsTheCacheAtItsHeadBlock() {
    for (long seed = 800; seed < 805; seed++) {
      importBlock(seed);
    }
    final Participant restarted = participant(cached.storage(), cacheConfig(true, 512, 64 << 20));
    assertThat(restarted.cache().currentBlock()).isEqualTo(chainHead.getNumber());
    // nothing it loads while computing the next block is taken for stale by the first prune
    cached = restarted;
    importBlock(805);
    assertThat(restarted.cache().prune().unloadedSubtrees()).isZero();
    assertDatabasesAgree(chainHead.getStateRoot());
  }

  @Test
  void reorgRollsTheHeadThroughTrieLogsWithCorrectRoots() {
    BlockHeader forkPoint = null;
    for (long seed = 400; seed < 410; seed++) {
      final BlockHeader header = importBlock(seed);
      if (seed == 405) {
        forkPoint = header;
      }
    }
    // a sibling chain from the fork point, validated on frozen world states
    BlockHeader sibling = forkPoint;
    for (long seed = 500; seed < 503; seed++) {
      final Hash root;
      try (BonsaiWorldState frozenReference = reference.frozenAt(sibling)) {
        applyChanges(frozenReference, seed);
        root = computeRoot(frozenReference);
        final BlockHeader header = childHeader(sibling, root, seed);
        frozenReference.persist(header);
        try (BonsaiWorldState frozenCached = cached.frozenAt(sibling)) {
          applyChanges(frozenCached, seed);
          frozenCached.persist(header);
        }
        blockchain.storeBlock(new Block(header, BlockBody.empty()), List.of());
        sibling = header;
      }
    }
    // forkchoice to the sibling chain: both heads roll back and forward through trie logs
    final BlockHeader newHead = sibling;
    for (final Participant participant : List.of(reference, cached)) {
      assertThat(
              participant
                  .provider()
                  .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(newHead)))
          .isPresent();
    }
    assertThat(blockchain.rewindToBlock(newHead.getHash())).isTrue();
    chainHead = newHead;
    assertThat(cached.head().rootHash()).isEqualTo(newHead.getStateRoot());
    assertThat(cached.cache().boundState(TreeRole.HEAD).map(TreeHandle::rootHash))
        .contains(Bytes32.wrap(newHead.getStateRoot().getBytes()));
    assertDatabasesAgree(newHead.getStateRoot());

    for (long seed = 600; seed < 605; seed++) {
      importBlock(seed);
    }
    assertDatabasesAgree(chainHead.getStateRoot());
  }

  @Test
  void rootMismatchRegistersNothing() {
    importBlock(700);
    final Bytes32 headRoot = Bytes32.wrap(chainHead.getStateRoot().getBytes());
    applyChanges(cached.head(), 701);
    final BlockHeader wrong = childHeader(chainHead, Hash.hash(Bytes.of(1)), 701);
    Hash computed = null;
    try {
      cached.head().persist(wrong);
    } catch (final StateRootMismatchException e) {
      computed = e.getActualRoot();
    }
    assertThat(computed).isNotNull();
    assertThat(cached.cache().lookup(TreeKind.STATE, Bytes32.wrap(computed.getBytes()))).isEmpty();
    assertThat(cached.cache().boundState(TreeRole.HEAD).orElseThrow().rootHash())
        .isEqualTo(headRoot);
    // the head is untouched and keeps importing
    importBlock(702);
    assertDatabasesAgree(chainHead.getStateRoot());
  }

  // ---------------------------------------------------------------------------------------------

  /** Reads the whole state through classic tries from both databases and compares it. */
  private void assertDatabasesAgree(final Hash root) {
    final Map<Bytes32, Bytes> expected = accounts(reference.storage(), root);
    final Map<Bytes32, Bytes> actual = accounts(cached.storage(), root);
    assertThat(actual).isEqualTo(expected);
    expected.forEach(
        (accountHash, account) -> {
          final Hash storageRoot =
              PmtStateTrieAccountValue.readFrom(RLP.input(account)).getStorageRoot();
          assertThat(storage(cached.storage(), accountHash, storageRoot))
              .describedAs("storage of %s", accountHash)
              .isEqualTo(storage(reference.storage(), accountHash, storageRoot));
        });
  }

  private static Map<Bytes32, Bytes> accounts(
      final BonsaiWorldStateKeyValueStorage storage, final Hash root) {
    return new StoredMerklePatriciaTrie<Bytes, Bytes>(
            storage::getAccountStateTrieNode,
            Bytes32.wrap(root.getBytes()),
            Function.identity(),
            Function.identity())
        .entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE);
  }

  private static Map<Bytes32, Bytes> storage(
      final BonsaiWorldStateKeyValueStorage storage,
      final Bytes32 accountHash,
      final Hash storageRoot) {
    return new StoredMerklePatriciaTrie<Bytes, Bytes>(
            (location, hash) ->
                storage.getAccountStorageTrieNode(Hash.wrap(accountHash), location, hash),
            Bytes32.wrap(storageRoot.getBytes()),
            Function.identity(),
            Function.identity())
        .entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE);
  }
}
