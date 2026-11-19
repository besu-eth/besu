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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitter;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveReadTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeWriter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * End-to-end integration test for the bonsai archive trie-node write and proof-read pipeline.
 *
 * <p>Covers the full chain: ArchiveTrieNodeStrategy → ArchiveNodeHistoryStore →
 * ArchiveReadTrieNodeStrategy (as would be used by BonsaiArchiveReadWorldStateStorageCoordinator).
 * Most tests drive the strategy directly; {@link #worldStatePersistsMatchArchivedTries} persists
 * through a real {@link BonsaiWorldState}.
 */
class BonsaiArchiveStateProofIntegrationTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveHistoryReader historyReader;
  private ArchiveTrieNodeStrategy archiveStrategy;
  private final List<BlockHeader> chain = new ArrayList<>();

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    final ArchiveNodeHistoryStore historyStore = new ArchiveNodeHistoryStore(storage);
    final BonsaiTrieNodeStrategy baseStrategy = new BonsaiTrieNodeStrategy();
    historyReader = new ArchiveHistoryReader(historyStore);
    final ArchiveTrieNodeWriter capture =
        new ArchiveTrieNodeWriter(
            historyStore,
            Executors.newFixedThreadPool(2),
            DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
            DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL,
            storage::startBatchTransaction);
    archiveStrategy =
        new ArchiveTrieNodeStrategy(baseStrategy, capture, historyStore, storage, b -> true);
  }

  private BlockHeader header(final long block) {
    while (chain.size() <= block) {
      final Hash parentHash = chain.isEmpty() ? Hash.EMPTY : chain.getLast().getHash();
      chain.add(
          new BlockHeaderTestFixture()
              .number(chain.size())
              .parentHash(parentHash)
              .stateRoot(
                  Hash.hash(
                      Bytes.concatenate(Bytes.ofUnsignedInt(chain.size()), parentHash.getBytes())))
              .buildHeader());
    }
    return chain.get((int) block);
  }

  private SegmentedKeyValueStorageTransaction startBlock(final long block) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.onBlockPersist(storage, tx, header(block));
    return tx;
  }

  // Advances the stored world block hash and root exactly as a real block commit would.
  private void commitBlock(final long block, final SegmentedKeyValueStorageTransaction tx) {
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_HASH_KEY,
        header(block).getHash().getBytes().toArrayUnsafe());
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_ROOT_HASH_KEY,
        header(block).getStateRoot().getBytes().toArrayUnsafe());
    archiveStrategy.onBeforeCommit(storage, tx);
    tx.commit();
  }

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  /** Branch node whose slot 0 holds a 33-byte hash ref derived from {@code seed}. */
  private static Bytes branchNode(final int seed) {
    final byte[] childRef = new byte[33];
    childRef[0] = (byte) 0xa0; // RLP string, 32 bytes
    for (int i = 1; i < 33; i++) {
      childRef[i] = (byte) (i + seed);
    }
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeRaw(Bytes.wrap(childRef));
    for (int i = 1; i < 16; i++) {
      out.writeNull();
    }
    out.writeBytes(Bytes.EMPTY);
    out.endList();
    return out.encoded();
  }

  /**
   * Branch node with all 16 child slots populated, where only slot 0 varies with {@code seed} — the
   * shape a real trie branch takes, and the case diff encoding is designed for: one changed child
   * ref out of sixteen.
   */
  private static Bytes fullBranchNode(final int seed) {
    return fullBranchNode(seed, 1);
  }

  /** As {@link #fullBranchNode(int)}, with slot 1 also set from a seed. */
  private static Bytes fullBranchNode(final int slot0, final int slot1) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeRaw(childRef(slot0));
    out.writeRaw(childRef(slot1));
    for (int i = 2; i < 16; i++) {
      out.writeRaw(childRef(i));
    }
    out.writeBytes(Bytes.EMPTY);
    out.endList();
    return out.encoded();
  }

  /** A 33-byte raw-RLP hash ref derived from {@code seed}. */
  private static Bytes childRef(final int seed) {
    final byte[] ref = new byte[33];
    ref[0] = (byte) 0xa0; // RLP string, 32 bytes
    for (int i = 1; i < 33; i++) {
      ref[i] = (byte) (i + seed);
    }
    return Bytes.wrap(ref);
  }

  /** Leaf node {@code [path, value]} — the short-node shape of the codec's diff path. */
  private static Bytes leafNode(final Bytes path, final Bytes value) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(path);
    out.writeBytes(value);
    out.endList();
    return out.encoded();
  }

  // Loads the stored node at key, as the state-root trie does before replacing it.
  private void loadStored(final Bytes key) {
    final Hash headRoot =
        storage
            .get(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY)
            .map(b -> Hash.wrap(Bytes32.wrap(b)))
            .orElse(Hash.ZERO);
    storage
        .get(TRIE_BRANCH_STORAGE, key.toArrayUnsafe())
        .map(Bytes::wrap)
        .ifPresent(
            node ->
                archiveStrategy
                    .wrapNodeLoader(headRoot, (l, h) -> Optional.of(node), l -> key)
                    .getNode(key, hash(node)));
  }

  /** Archives {@code node} at {@code location} as block {@code block}. */
  private void writeAccountBlock(final long block, final Bytes location, final Bytes node) {
    loadStored(location);
    final SegmentedKeyValueStorageTransaction tx = startBlock(block);
    archiveStrategy.putTrieNode(storage, tx, location, hash(node), node);
    commitBlock(block, tx);
  }

  private Optional<Bytes> readAccountNode(
      final long block, final Bytes location, final Bytes32 nodeHash) {
    return new ArchiveReadTrieNodeStrategy(block, historyReader)
        .getTrieNode(storage, location, nodeHash);
  }

  private Optional<Bytes> readStorageNode(
      final Hash accountHash, final long block, final Bytes location, final Bytes32 nodeHash) {
    return new ArchiveReadTrieNodeStrategy(block, historyReader)
        .getTrieNode(storage, TrieNodeKey.of(accountHash, location), nodeHash);
  }

  @Test
  void archivedNodeIsRetrievableViaProofLoader() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = branchNode(0);

    writeAccountBlock(0L, location, node);

    assertThat(readAccountNode(0L, location, hash(node))).contains(node);
  }

  @Test
  void archivePathServesHistoricalNodeWhenLiveStateAdvanced() {
    final Bytes location = Bytes.of(0x0a);
    final Bytes nodeAtBlock0 = branchNode(0);
    final Bytes nodeAtBlock1 = branchNode(1);

    writeAccountBlock(0L, location, nodeAtBlock0);
    writeAccountBlock(1L, location, nodeAtBlock1);

    assertThat(readAccountNode(0L, location, hash(nodeAtBlock0))).contains(nodeAtBlock0);
    assertThat(readAccountNode(1L, location, hash(nodeAtBlock1))).contains(nodeAtBlock1);
  }

  @Test
  void progressCoversBlockAfterArchive() {
    final Bytes location = Bytes.of(0x00);
    final Bytes node = branchNode(0);

    writeAccountBlock(0L, location, node);

    assertThat(new ArchiveNodeHistoryStore(storage).getArchivedBlock())
        .map(ArchiveNodeHistoryStore.ArchivedBlock::number)
        .contains(0L);
  }

  @Test
  void proofLoaderReturnsEmptyForUnarchivedBlock() {
    // Nothing written to storage
    final Bytes location = Bytes.of(0x0f);
    final Bytes phantomNode = Bytes.fromHexString("0x9999");
    assertThat(readAccountNode(5L, location, hash(phantomNode))).isEmpty();
  }

  @Test
  void archivesAndRetrievesStorageTrieNode() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"));
    final Bytes location = Bytes.of(0x01);
    final Bytes node = branchNode(0);

    final SegmentedKeyValueStorageTransaction tx = startBlock(0L);
    archiveStrategy.putTrieNode(
        storage, tx, TrieNodeKey.of(accountHash, location), hash(node), node);
    commitBlock(0L, tx);

    assertThat(readStorageNode(accountHash, 0L, location, hash(node))).contains(node);
  }

  @Test
  void accountTrieLoaderIgnoresStorageTrieEntries() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0x1234567812345678123456781234567812345678123456781234567812345678"));
    final Bytes storageLocation = Bytes.of(0x02);
    final Bytes storageNode = branchNode(0);

    // Write only to storage-trie archive
    final SegmentedKeyValueStorageTransaction tx = startBlock(0L);
    archiveStrategy.putTrieNode(
        storage, tx, TrieNodeKey.of(accountHash, storageLocation), hash(storageNode), storageNode);
    commitBlock(0L, tx);

    // Account-trie reader must not return anything for the same location
    assertThat(readAccountNode(0L, storageLocation, hash(storageNode))).isEmpty();
  }

  @Test
  void everyVersionAcrossACheckpointWindowIsProvable() {
    final Bytes location = Bytes.of(0x0b);
    final int blocks = DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL * 2 + 3;
    for (int block = 0; block < blocks; block++) {
      writeAccountBlock(block, location, branchNode(block));
    }

    // Walking past two checkpoint rollovers, every historical version still proves out by hash.
    for (int block = 0; block < blocks; block++) {
      final Bytes expected = branchNode(block);
      assertThat(readAccountNode(block, location, hash(expected)))
          .as("block %d", block)
          .contains(expected);
    }
  }

  @Test
  void diffEncodedHistoryIsSmallerThanFullNodePerBlock() {
    final Bytes location = Bytes.of(0x0c);
    final int blocks = DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
    long archivedBytes = 0;
    long fullNodeBytes = 0;
    for (int block = 0; block < blocks; block++) {
      final Bytes node = fullBranchNode(block);
      writeAccountBlock(block, location, node);
      fullNodeBytes += node.size();
      archivedBytes +=
          storage
              .get(
                  TRIE_BRANCH_STORAGE_ARCHIVE,
                  ArchiveNodeKey.historyKey(ArchiveNodeKey.account(location), block)
                      .toArrayUnsafe())
              .orElseThrow()
              .length;
    }
    // A single-child-slot change encodes in a handful of bytes instead of a whole branch node.
    assertThat(archivedBytes).isLessThan(fullNodeBytes / 2);
  }

  @Test
  void shortNodeDiffsReconstructThroughTheProofLoader() {
    final Bytes location = Bytes.of(0x0d);
    final Bytes path = Bytes.of(0x20, 0x0a, 0x0b);
    final Bytes leafV0 = leafNode(path, Bytes.of(0x01));
    final Bytes leafV1 = leafNode(path, Bytes.of(0x02)); // value-only change
    final Bytes leafV2 = leafNode(Bytes.of(0x20, 0x0a, 0x0c), Bytes.of(0x02)); // path-only change

    writeAccountBlock(0L, location, leafV0);
    writeAccountBlock(1L, location, leafV1);
    writeAccountBlock(2L, location, leafV2);

    assertThat(readAccountNode(0L, location, hash(leafV0))).contains(leafV0);
    assertThat(readAccountNode(1L, location, hash(leafV1))).contains(leafV1);
    assertThat(readAccountNode(2L, location, hash(leafV2))).contains(leafV2);
  }

  @Test
  void deletedNodeIsAbsentFromLaterProofsButPresentInEarlierOnes() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = branchNode(0);
    writeAccountBlock(0L, location, node);

    final SegmentedKeyValueStorageTransaction tx = startBlock(1L);
    archiveStrategy.removeTrieNode(storage, tx, location);
    commitBlock(1L, tx);

    assertThat(readAccountNode(0L, location, hash(node))).contains(node);
    assertThat(readAccountNode(1L, location, hash(node))).isEmpty();
  }

  @Test
  void storageTrieDiffsAreIsolatedPerAccount() {
    final Hash accountA =
        Hash.wrap(
            Bytes32.fromHexString(
                "0x1111111111111111111111111111111111111111111111111111111111111111"));
    final Hash accountB =
        Hash.wrap(
            Bytes32.fromHexString(
                "0x2222222222222222222222222222222222222222222222222222222222222222"));
    final Bytes location = Bytes.of(0x03);
    final Bytes aV0 = branchNode(10);
    final Bytes aV1 = branchNode(11);
    final Bytes bV0 = branchNode(20);

    for (long block = 0; block <= 1; block++) {
      loadStored(TrieNodeKey.of(accountA, location));
      final SegmentedKeyValueStorageTransaction tx = startBlock(block);
      final Bytes aNode = block == 0 ? aV0 : aV1;
      loadStored(TrieNodeKey.of(accountA, location));
      archiveStrategy.putTrieNode(
          storage, tx, TrieNodeKey.of(accountA, location), hash(aNode), aNode);
      if (block == 0) {
        archiveStrategy.putTrieNode(
            storage, tx, TrieNodeKey.of(accountB, location), hash(bV0), bV0);
      }
      commitBlock(block, tx);
    }

    // A's diff at block 1 must not disturb B, whose last write was at block 0.
    assertThat(readStorageNode(accountA, 1L, location, hash(aV1))).contains(aV1);
    assertThat(readStorageNode(accountB, 1L, location, hash(bV0))).contains(bV0);
  }

  @Test
  void nodeLeftByUncapturedWriteIsNotADiffBase() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes stale = fullBranchNode(1, 50);
    final Bytes written = fullBranchNode(3, 50);
    writeAccountBlock(0L, location, fullBranchNode(1, 1));
    writeAccountBlock(1L, Bytes.of(0x0f), fullBranchNode(9, 9));

    // An abandoned sibling of block 2 leaves a node at the location, unreachable at block 1.
    final SegmentedKeyValueStorageTransaction sibling = storage.startTransaction();
    archiveStrategy.putTrieNode(storage, sibling, location, hash(stale), stale);
    commitBlock(1L, sibling);

    // Block 2's state-root trie never loads the unreachable node.
    final SegmentedKeyValueStorageTransaction tx = startBlock(2L);
    archiveStrategy.putTrieNode(storage, tx, location, hash(written), written);
    commitBlock(2L, tx);

    assertThat(readAccountNode(2L, location, hash(written))).contains(written);
  }

  // BAL blocks compute the state root on a separate world state over the persisted parent.
  @ParameterizedTest(name = "state root computed on a separate world state: {0}")
  @ValueSource(booleans = {false, true})
  void worldStatePersistsMatchArchivedTries(final boolean separateStateRootWorldState) {
    final Address account = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final Function<Bytes, Bytes> storageKey =
        location -> TrieNodeKey.of(account.addressHash(), location);
    final BonsaiWorldStateProvider provider =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
            mock(Blockchain.class));
    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        provider.getWorldStateKeyValueStorage();
    final SegmentedKeyValueStorage liveStorage = worldStateStorage.getComposedWorldStateStorage();
    final ArchiveNodeHistoryStore liveHistory = new ArchiveNodeHistoryStore(liveStorage);

    try (ArchiveTrieNodeStrategy strategy =
        new ArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(),
            new ArchiveTrieNodeWriter(
                liveHistory,
                Executors.newFixedThreadPool(2),
                DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
                DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL,
                liveStorage::startBatchTransaction),
            liveHistory,
            liveStorage,
            b -> true)) {
      worldStateStorage.setTrieNodeStrategy(strategy);
      final BonsaiWorldState worldState = (BonsaiWorldState) provider.getWorldState();

      // Enough slots that the storage root is a branch, which diffs smaller than FULL.
      final WorldUpdater genesis = worldState.updater();
      final MutableAccount created = genesis.createAccount(account, 1, Wei.of(1L));
      for (int slot = 0; slot < 32; slot++) {
        created.setStorageValue(UInt256.valueOf(slot), UInt256.valueOf(slot + 1L));
      }
      genesis.commit();
      final BlockHeader block0 = persist(worldState, 0L, Hash.EMPTY);
      final Hash storageRoot0 = ((BonsaiAccount) worldState.get(account)).getStorageRoot();

      final WorldUpdater updater = worldState.updater();
      updater.getAccount(account).setStorageValue(UInt256.ZERO, UInt256.valueOf(100));
      updater.commit();
      final StateRootCommitter committer =
          separateStateRootWorldState
              ? onSeparateWorldState(provider, worldStateStorage)
              : new DefaultStateRootCommitter();
      final BlockHeader block1 = persist(worldState, 1L, block0.getHash(), committer);
      final Hash storageRoot1 = ((BonsaiAccount) worldState.get(account)).getStorageRoot();

      assertArchivedTrie(liveHistory, 0L, block0.getStateRoot(), Function.identity());
      assertArchivedTrie(liveHistory, 0L, storageRoot0, storageKey);
      assertArchivedTrie(liveHistory, 1L, block1.getStateRoot(), Function.identity());
      assertArchivedTrie(liveHistory, 1L, storageRoot1, storageKey);
      // Block 1 diffs the storage root against the node the state-root trie loaded, not FULL.
      final Bytes storageRootKey = ArchiveNodeKey.of(storageKey.apply(Bytes.EMPTY));
      assertThat(liveHistory.getLatestCounters(List.of(storageRootKey)))
          .containsEntry(storageRootKey, 1);
    }
  }

  private static BlockHeader persist(
      final BonsaiWorldState worldState, final long number, final Hash parentHash) {
    return persist(worldState, number, parentHash, new DefaultStateRootCommitter());
  }

  private static BlockHeader persist(
      final BonsaiWorldState worldState,
      final long number,
      final Hash parentHash,
      final StateRootCommitter committer) {
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(number)
            .parentHash(parentHash)
            .stateRoot(worldState.frontierRootHash())
            .buildHeader();
    worldState.persist(header, committer);
    return header;
  }

  private static StateRootCommitter onSeparateWorldState(
      final BonsaiWorldStateProvider provider, final BonsaiWorldStateKeyValueStorage storage) {
    final BonsaiWorldState parent =
        new BonsaiWorldState(
            provider,
            new BonsaiWorldStateLayerStorage(storage),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache());
    return (worldState, header, updater) ->
        new DefaultStateRootCommitter().compute(parent, header, updater);
  }

  // Walks the trie at block from the archive alone; every node must match its parent's hash.
  private static void assertArchivedTrie(
      final ArchiveNodeHistoryStore history,
      final long block,
      final Hash root,
      final Function<Bytes, Bytes> keyOf) {
    final ArchiveHistoryReader reader = new ArchiveHistoryReader(history);
    new StoredMerklePatriciaTrie<Bytes, Bytes>(
            (location, hash) -> {
              final Optional<Bytes> node =
                  reader.nodeAt(ArchiveNodeKey.of(keyOf.apply(location)), block);
              assertThat(node).map(n -> Hash.hash(n).getBytes()).contains(hash);
              return node;
            },
            Bytes32.wrap(root.getBytes()),
            Function.identity(),
            Function.identity())
        .entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE);
  }
}
