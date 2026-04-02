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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.referencetests.BonsaiReferenceTestWorldStateStorage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiPreImageProxy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.bouncycastle.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BonsaiArchiverTests {

  // Number of blocks in the chain. This is different to the number of blocks
  // we have successfully archived state for
  static final long SHORT_TEST_CHAIN_HEIGHT = 150;
  static final long LONG_TEST_CHAIN_HEIGHT =
      2000; // We want block 2000 to be returned so set to 2001

  // Address used for account and storage changes
  final Address address = Address.fromHexString("0x95cD8499051f7FE6a2F53749eC1e9F4a81cafa13");

  // Cache blocks that are generated during the test
  static LoadingCache<Long, Optional<Block>> blockNumberCache;

  static LoadingCache<Hash, Optional<Block>> blockHashCache =
      CacheBuilder.newBuilder()
          .maximumSize(SHORT_TEST_CHAIN_HEIGHT)
          .build(
              new CacheLoader<>() {
                @Override
                public Optional<Block> load(final Hash blockHash) {
                  Optional<Block> foundBlock;
                  for (long i = 0; i <= SHORT_TEST_CHAIN_HEIGHT; i++) {
                    if ((foundBlock = blockNumberCache.getUnchecked(i)).isPresent()
                        && foundBlock.get().getHash().equals(blockHash)) {
                      return foundBlock;
                    }
                  }
                  return Optional.empty();
                }
              });

  static long currentBlockHeight = 0;

  private BonsaiWorldStateKeyValueStorage worldStateStorage;
  private Blockchain blockchain;
  private TrieLogManager trieLogManager;
  private final Consumer<Runnable> executeAsync = Runnable::run;
  private BonsaiWorldState bonsaiWorldState;

  @SuppressWarnings("BannedMethod")
  @BeforeEach
  public void setup() {
    Configurator.setLevel(LogManager.getLogger(BonsaiArchiverTests.class).getName(), Level.TRACE);
    worldStateStorage = Mockito.mock(BonsaiWorldStateKeyValueStorage.class);
    blockchain = Mockito.mock(Blockchain.class);
    trieLogManager = Mockito.mock(TrieLogManager.class);
    bonsaiWorldState = Mockito.mock(BonsaiWorldState.class);
  }

  private static long getCurrentBlockHeight() {
    return currentBlockHeight;
  }

  // Utility for generating blocks on the fly, up to a certain chain height at which point no block
  // is returned.
  // This is recursive and generates all parent blocks for the requested block number on the fly,
  // but combined with a cache loader each block number will only be generated once
  private static Optional<Block> getGeneratedBlock(final long blockNumber, final long chainLength) {
    if (blockNumber >= chainLength) {
      return Optional.empty();
    }
    // Fake block
    final BlockHeader header =
        new BlockHeader(
            blockNumber == 0
                ? Hash.EMPTY
                : blockNumberCache.getUnchecked(blockNumber - 1).get().getHash(),
            Hash.EMPTY_TRIE_HASH,
            Address.ZERO,
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY_TRIE_HASH,
            LogsBloomFilter.builder().build(),
            Difficulty.ONE,
            blockNumber,
            0,
            0,
            0,
            Bytes.of(0x00),
            Wei.ZERO,
            Bytes32.wrap(Hash.EMPTY.getBytes()),
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new MainnetBlockHeaderFunctions());
    return Optional.of(new Block(header, BlockBody.empty()));
  }

  @Test
  public void archiveInitialisesAndSetsPendingBlockCount() {

    blockNumberCache =
        CacheBuilder.newBuilder()
            .maximumSize(LONG_TEST_CHAIN_HEIGHT)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Block> load(final Long blockNumber) {
                    return getGeneratedBlock(blockNumber, LONG_TEST_CHAIN_HEIGHT);
                  }
                });

    when(worldStateStorage.getFlatDbMode()).thenReturn(FlatDbMode.ARCHIVE);

    // If we had previously archived up to block 100...
    final AtomicLong archivedBlocks = new AtomicLong(100L);

    // Mock the DB setter so it updates what the getter returns
    doAnswer(
            invocation -> {
              long thisValue = invocation.getArgument(0, Long.class);
              archivedBlocks.set(thisValue);
              return null;
            })
        .when(worldStateStorage)
        .setLatestArchivedBlock(any(Long.class));

    // Mock the DB getter
    doAnswer(
            invocation -> {
              return Optional.of(archivedBlocks.get());
            })
        .when(worldStateStorage)
        .getLatestArchivedBlock();

    when(blockchain.getChainHeadBlockNumber()).thenReturn(2000L);

    // When any block is asked for during the test, generate it on the fly and return it
    // unless it is > block num 2000
    when(blockchain.getBlockByNumber(anyLong()))
        .then(
            requestedBlockNumber ->
                blockNumberCache.getUnchecked(requestedBlockNumber.getArgument(0, Long.class)));

    BonsaiArchiver archiver =
        new BonsaiArchiver(
            worldStateStorage, blockchain, executeAsync, trieLogManager, new NoOpMetricsSystem());
    archiver.getPendingBlocksCount();
    archiver.initialize();

    // Check that blocks 101 to 1990 (10 before chain head 2000) have been caught up
    assertThat(archiver.getPendingBlocksCount()).isEqualTo(1900);
  }

  @Test
  public void archiverMoves1AccountStateChangeToArchiveSegment() {
    // Set up the block cache
    blockNumberCache =
        CacheBuilder.newBuilder()
            .maximumSize(SHORT_TEST_CHAIN_HEIGHT)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Block> load(final Long blockNumber) {
                    return getGeneratedBlock(blockNumber, SHORT_TEST_CHAIN_HEIGHT);
                  }
                });

    when(worldStateStorage.getFlatDbMode()).thenReturn(FlatDbMode.ARCHIVE);

    // If we had previously archived up to block 100...
    final AtomicLong archivedBlocks = new AtomicLong(100L);

    // Mock the DB setter so it updates what the getter returns
    doAnswer(
            invocation -> {
              long thisValue = invocation.getArgument(0, Long.class);
              archivedBlocks.set(thisValue);
              return null;
            })
        .when(worldStateStorage)
        .setLatestArchivedBlock(any(Long.class));

    // Mock the DB getter
    doAnswer(
            invocation -> {
              return Optional.of(archivedBlocks.get());
            })
        .when(worldStateStorage)
        .getLatestArchivedBlock();

    // Mock the number of changes the archive action carries out for each relevant block
    when(worldStateStorage.archivePreviousAccountState(any(), any()))
        .then(
            request -> {
              Object objHeader = request.getArgument(0, Optional.class).get();
              if (objHeader instanceof BlockHeader blockHeader) {
                if (blockHeader.getNumber() == 101) {
                  // Mock 1 state change when block 102 is being processed, because state changes in
                  // block 101 can be archived NB: the trie log in this test for block 102 isn't
                  // archived because no further changes to that account are made
                  return 1;
                }
                return 0;
              }
              return 0;
            });

    // When any block is asked for by the archiver during the test, generate it on the fly, cache
    // it, and return it unless it exceeds the max block for the test
    when(blockchain.getBlockByNumber(anyLong()))
        .then(
            requestedBlockNumber ->
                blockNumberCache.getUnchecked(requestedBlockNumber.getArgument(0, Long.class)));

    when(blockchain.getBlockHeader(any()))
        .then(
            requestedBlockHash ->
                blockHashCache
                    .getUnchecked(requestedBlockHash.getArgument(0, Hash.class))
                    .map(Block::getHeader));

    // Generate some trie logs to return for a specific block

    // Simulate an account change in block 101. This state will be archived because block 102
    // updates the same account (see below)
    TrieLogLayer block101TrieLogs = new TrieLogLayer();
    PmtStateTrieAccountValue oldValue =
        new PmtStateTrieAccountValue(12, Wei.fromHexString("0x123"), Hash.EMPTY, Hash.EMPTY);
    PmtStateTrieAccountValue newValue =
        new PmtStateTrieAccountValue(13, Wei.fromHexString("0x234"), Hash.EMPTY, Hash.EMPTY);
    block101TrieLogs.addAccountChange(address, oldValue, newValue);

    // Simulate another change to the same account, this time in block 102. This change won't be
    // archived during the test because it is the current state of the account.
    TrieLogLayer block102TrieLogs = new TrieLogLayer();
    oldValue = new PmtStateTrieAccountValue(13, Wei.fromHexString("0x234"), Hash.EMPTY, Hash.EMPTY);
    newValue = new PmtStateTrieAccountValue(14, Wei.fromHexString("0x345"), Hash.EMPTY, Hash.EMPTY);
    block102TrieLogs.addAccountChange(address, oldValue, newValue);
    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0xf0b2ba5849ad812479a44bb1efd97f1f3fdab945ff53a81a4dea55b4db1a972e")))
        .thenReturn(Optional.of(block101TrieLogs));
    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0x0d22db864d4effa62b640de645bffd44fb5d130578fbea4399f9abf8d7ac7789")))
        .thenReturn(Optional.of(block102TrieLogs));

    // Initialize the archiver
    BonsaiArchiver archiver =
        new BonsaiArchiver(
            worldStateStorage, blockchain, executeAsync, trieLogManager, new NoOpMetricsSystem());
    archiver.initialize();

    currentBlockHeight = 100L;
    when(blockchain.getChainHeadBlockNumber())
        .then(requestedBlockNumber -> getCurrentBlockHeight());

    // No new blocks yet, no pending blocks to archive
    assertThat(archiver.getPendingBlocksCount()).isEqualTo(0);

    // Process the next 50 blocks. Only 1 account state change should happen during this processing
    // since there are only trie logs for blocks 101 and 102
    for (long nextBlock = 101; nextBlock < 150; nextBlock++) {
      currentBlockHeight = nextBlock;
      if (nextBlock == 113) {
        int accountsMoved = archiver.moveBlockStateToArchive();
        assertThat(accountsMoved).isEqualTo(1);
      } else {
        int accountsMoved = archiver.moveBlockStateToArchive();
        assertThat(accountsMoved).isEqualTo(0);
      }
    }
  }

  @Test
  public void archiverMoves2StorageChangesToArchiveSegment() {
    // Set up the block cache
    blockNumberCache =
        CacheBuilder.newBuilder()
            .maximumSize(SHORT_TEST_CHAIN_HEIGHT)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Block> load(final Long blockNumber) {
                    return getGeneratedBlock(blockNumber, SHORT_TEST_CHAIN_HEIGHT);
                  }
                });

    when(worldStateStorage.getFlatDbMode()).thenReturn(FlatDbMode.ARCHIVE);

    // If we had previously archived up to block 100...
    final AtomicLong archivedBlocks = new AtomicLong(100L);

    // Mock the DB setter so it updates what the getter returns
    doAnswer(
            invocation -> {
              long thisValue = invocation.getArgument(0, Long.class);
              archivedBlocks.set(thisValue);
              return null;
            })
        .when(worldStateStorage)
        .setLatestArchivedBlock(any(Long.class));

    // Mock the DB getter
    doAnswer(
            invocation -> {
              return Optional.of(archivedBlocks.get());
            })
        .when(worldStateStorage)
        .getLatestArchivedBlock();

    // Mock the number of changes the archive action carries out for each relevant block
    when(worldStateStorage.archivePreviousStorageState(any(), any()))
        .then(
            request -> {
              Object objHeader = request.getArgument(0, Optional.class).get();
              if (objHeader instanceof BlockHeader blockHeader) {
                if (blockHeader.getNumber() == 101 || blockHeader.getNumber() == 102) {
                  // Mock 1 state change when block 102 is being processed, because state changes in
                  // block 101 can be archived (and likewise for block 103). NB: the trie log in
                  // this test for block 103 isn't archived because no further changes to that
                  // storage
                  // are made
                  return 1;
                }
                return 0;
              }
              return 0;
            });

    // When any block is asked for by the archiver, generate it on the fly, cache it, and
    // return it unless it
    when(blockchain.getBlockByNumber(anyLong()))
        .then(
            requestedBlockNumber ->
                blockNumberCache.getUnchecked(requestedBlockNumber.getArgument(0, Long.class)));

    when(blockchain.getBlockHeader(any()))
        .then(
            requestedBlockHash ->
                blockHashCache
                    .getUnchecked(requestedBlockHash.getArgument(0, Hash.class))
                    .map(Block::getHeader));

    // Generate some trie logs to return for a specific block

    // Simulate a storage change in block 101. This state will be archived because block 102 updates
    // the same storage (see below)
    TrieLogLayer block101TrieLogs = new TrieLogLayer();
    UInt256 oldValue = UInt256.ZERO;
    UInt256 newValue = UInt256.ONE;
    UInt256 slot = UInt256.ONE;
    StorageSlotKey storageSlotKey = new StorageSlotKey(slot);
    block101TrieLogs.addStorageChange(address, storageSlotKey, oldValue, newValue);

    // Simulate a storage change in block 102. This state will also be archived because block 102
    // updates the same storage (see below)
    TrieLogLayer block102TrieLogs = new TrieLogLayer();
    oldValue = UInt256.ONE;
    newValue = UInt256.valueOf(2L);
    slot = UInt256.ONE;
    storageSlotKey = new StorageSlotKey(slot);
    block102TrieLogs.addStorageChange(address, storageSlotKey, oldValue, newValue);

    // Simulate a storage change in block 103. This state will not be archived because it refers to
    // a different slot
    TrieLogLayer block103TrieLogs = new TrieLogLayer();
    oldValue = UInt256.ZERO;
    newValue = UInt256.ONE;
    slot = UInt256.valueOf(2L);
    storageSlotKey = new StorageSlotKey(slot);
    block103TrieLogs.addStorageChange(address, storageSlotKey, oldValue, newValue);

    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0xf0b2ba5849ad812479a44bb1efd97f1f3fdab945ff53a81a4dea55b4db1a972e")))
        .thenReturn(Optional.of(block101TrieLogs));
    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0x0d22db864d4effa62b640de645bffd44fb5d130578fbea4399f9abf8d7ac7789")))
        .thenReturn(Optional.of(block102TrieLogs));
    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0x96440b533326c26f4611e4c0b123ce732aa7a68e3b275f4a5a2ea9bc4b089c73")))
        .thenReturn(Optional.of(block103TrieLogs));

    // Initialize the archiver
    BonsaiArchiver archiver =
        new BonsaiArchiver(
            worldStateStorage, blockchain, executeAsync, trieLogManager, new NoOpMetricsSystem());
    archiver.initialize();

    currentBlockHeight = 100L;
    when(blockchain.getChainHeadBlockNumber())
        .then(requestedBlockNumber -> getCurrentBlockHeight());

    // No new blocks yet, no pending blocks to archive
    assertThat(archiver.getPendingBlocksCount()).isEqualTo(0);

    when(blockchain.getChainHeadBlockNumber())
        .then(requestedBlockNumber -> getCurrentBlockHeight());

    int totalStorageMoved = 0;
    // Process the next 50 blocks. 2 storage changes should be archived during this time should
    // happen during this processing since there are only trie logs for blocks 101 and 102
    for (long nextBlock = 101; nextBlock < 150; nextBlock++) {
      currentBlockHeight = nextBlock;
      int storageMoved = archiver.moveBlockStateToArchive();
      totalStorageMoved += storageMoved;
      if (nextBlock == 113 || nextBlock == 114) {
        assertThat(storageMoved).isEqualTo(1);
      } else {
        assertThat(storageMoved).isEqualTo(0);
      }
    }

    assertThat(totalStorageMoved).isEqualTo(2);
  }

  @Test
  public void archiverMoves1AccountAnd2StorageChangesToArchiveSegment() {
    // Set up the block cache
    blockNumberCache =
        CacheBuilder.newBuilder()
            .maximumSize(SHORT_TEST_CHAIN_HEIGHT)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Block> load(final Long blockNumber) {
                    return getGeneratedBlock(blockNumber, SHORT_TEST_CHAIN_HEIGHT);
                  }
                });

    when(worldStateStorage.getFlatDbMode()).thenReturn(FlatDbMode.ARCHIVE);

    // If we had previously archived up to block 100...
    final AtomicLong archivedBlocks = new AtomicLong(100L);

    // Mock the DB setter so it updates what the getter returns
    doAnswer(
            invocation -> {
              long thisValue = invocation.getArgument(0, Long.class);
              archivedBlocks.set(thisValue);
              return null;
            })
        .when(worldStateStorage)
        .setLatestArchivedBlock(any(Long.class));

    // Mock the DB getter
    doAnswer(
            invocation -> {
              return Optional.of(archivedBlocks.get());
            })
        .when(worldStateStorage)
        .getLatestArchivedBlock();

    // Mock the number of changes the archive action carries out for each relevant block
    when(worldStateStorage.archivePreviousStorageState(any(), any()))
        .then(
            request -> {
              Object objHeader = request.getArgument(0, Optional.class).get();
              if (objHeader instanceof BlockHeader blockHeader) {
                if (blockHeader.getNumber() == 101 || blockHeader.getNumber() == 102) {
                  // Mock 1 storage change when block 102 is being processed, because state changes
                  // in block 101 can be archived (and likewise for block 103). NB: the trie log in
                  // this test for block 103 isn't archived because no further changes to that
                  // storage are made
                  return 1;
                }
              }
              return 0;
            });

    // Mock the number of changes the archive action carries out for each relevant block
    when(worldStateStorage.archivePreviousAccountState(any(), any()))
        .then(
            request -> {
              Object objHeader = request.getArgument(0, Optional.class).get();
              if (objHeader instanceof BlockHeader blockHeader) {
                if (blockHeader.getNumber() == 101) {
                  // Mock 1 state change when block 102 is being processed, because state changes in
                  // block 101 can be archived
                  return 1;
                }
              }
              return 0;
            });

    // When any block is asked for by the archiver, generate it on the fly, cache it, and
    // return it unless it
    when(blockchain.getBlockByNumber(anyLong()))
        .then(
            requestedBlockNumber ->
                blockNumberCache.getUnchecked(requestedBlockNumber.getArgument(0, Long.class)));

    when(blockchain.getBlockHeader(any()))
        .then(
            requestedBlockHash ->
                blockHashCache
                    .getUnchecked(requestedBlockHash.getArgument(0, Hash.class))
                    .map(Block::getHeader));

    // Generate some trie logs to return for a specific block

    Address address = Address.fromHexString("0x95cD8499051f7FE6a2F53749eC1e9F4a81cafa13");

    // Simulate a storage change AND an account change in block 101. This state and storage will be
    // archived because block 102 updates both again (see below)
    TrieLogLayer block101TrieLogs = new TrieLogLayer();
    UInt256 oldStorageValue = UInt256.ZERO;
    UInt256 newStorageValue = UInt256.ONE;
    UInt256 slot = UInt256.ONE;
    StorageSlotKey storageSlotKey = new StorageSlotKey(slot);
    block101TrieLogs.addStorageChange(address, storageSlotKey, oldStorageValue, newStorageValue);
    PmtStateTrieAccountValue oldAccountValue =
        new PmtStateTrieAccountValue(12, Wei.fromHexString("0x123"), Hash.EMPTY, Hash.EMPTY);
    PmtStateTrieAccountValue newAccountValue =
        new PmtStateTrieAccountValue(13, Wei.fromHexString("0x234"), Hash.EMPTY, Hash.EMPTY);
    block101TrieLogs.addAccountChange(address, oldAccountValue, newAccountValue);

    // Simulate a storage AND account change in block 102.
    TrieLogLayer block102TrieLogs = new TrieLogLayer();
    oldStorageValue = UInt256.ONE;
    newStorageValue = UInt256.valueOf(2L);
    slot = UInt256.ONE;
    storageSlotKey = new StorageSlotKey(slot);
    block102TrieLogs.addStorageChange(address, storageSlotKey, oldStorageValue, newStorageValue);
    oldAccountValue =
        new PmtStateTrieAccountValue(13, Wei.fromHexString("0x234"), Hash.EMPTY, Hash.EMPTY);
    newAccountValue =
        new PmtStateTrieAccountValue(14, Wei.fromHexString("0x345"), Hash.EMPTY, Hash.EMPTY);
    block102TrieLogs.addAccountChange(address, oldAccountValue, newAccountValue);

    // Simulate a storage change in block 103. This state will not be archived because it refers to
    // a different slot
    TrieLogLayer block103TrieLogs = new TrieLogLayer();
    oldStorageValue = UInt256.ZERO;
    newStorageValue = UInt256.ONE;
    slot = UInt256.valueOf(2L);
    storageSlotKey = new StorageSlotKey(slot);
    block103TrieLogs.addStorageChange(address, storageSlotKey, oldStorageValue, newStorageValue);

    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0xf0b2ba5849ad812479a44bb1efd97f1f3fdab945ff53a81a4dea55b4db1a972e")))
        .thenReturn(Optional.of(block101TrieLogs));
    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0x0d22db864d4effa62b640de645bffd44fb5d130578fbea4399f9abf8d7ac7789")))
        .thenReturn(Optional.of(block102TrieLogs));
    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0x96440b533326c26f4611e4c0b123ce732aa7a68e3b275f4a5a2ea9bc4b089c73")))
        .thenReturn(Optional.of(block103TrieLogs));

    // Initialize the archiver
    BonsaiArchiver archiver =
        new BonsaiArchiver(
            worldStateStorage, blockchain, executeAsync, trieLogManager, new NoOpMetricsSystem());
    archiver.initialize();

    currentBlockHeight = 100L;
    when(blockchain.getChainHeadBlockNumber())
        .then(requestedBlockNumber -> getCurrentBlockHeight());

    // No new blocks yet, no pending blocks to archive
    assertThat(archiver.getPendingBlocksCount()).isEqualTo(0);

    when(blockchain.getChainHeadBlockNumber())
        .then(requestedBlockNumber -> getCurrentBlockHeight());

    int totalStorageMoved = 0;
    // Process the next 50 blocks. 2 storage changes should be archived during this time should
    // happen during this processing since there are only trie logs for blocks 101 and 102
    for (long nextBlock = 101; nextBlock < 150; nextBlock++) {
      currentBlockHeight = nextBlock;
      int storageAndAccountsMoved = archiver.moveBlockStateToArchive();
      if (nextBlock == 113) {
        assertThat(storageAndAccountsMoved).isEqualTo(2);
      } else if (nextBlock == 114) {
        assertThat(storageAndAccountsMoved).isEqualTo(1);
      } else {
        assertThat(storageAndAccountsMoved).isEqualTo(0);
      }
      totalStorageMoved += storageAndAccountsMoved;
    }

    assertThat(totalStorageMoved).isEqualTo(3);
  }

  @Test
  public void archiveInMemoryDBArchivesAccountStateCorrectly() {
    final BonsaiPreImageProxy preImageProxy =
        new BonsaiPreImageProxy.BonsaiReferenceTestPreImageProxy();

    final BonsaiWorldStateKeyValueStorage bonsaiWorldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG);

    final BonsaiReferenceTestWorldStateStorage testWorldStateStorage =
        new BonsaiReferenceTestWorldStateStorage(bonsaiWorldStateKeyValueStorage, preImageProxy);

    // Simulate post-migration state: archive migration has completed, mode is ARCHIVE
    bonsaiWorldStateKeyValueStorage.upgradeToArchiveFlatDbMode();
    assertThat(testWorldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE);

    // Assume we've archived up to block 150L i.e. we're up to date with the chain head
    // (SHORT_TEST_CHAIN_HEIGHT)
    testWorldStateStorage.setLatestArchivedBlock(150L);

    // Set up the block cache
    blockNumberCache =
        CacheBuilder.newBuilder()
            .maximumSize(SHORT_TEST_CHAIN_HEIGHT)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Block> load(final Long blockNumber) {
                    return getGeneratedBlock(blockNumber, SHORT_TEST_CHAIN_HEIGHT);
                  }
                });

    // When any block is asked for by the archiver, generate it on the fly, cache it, and
    // return it unless it
    when(blockchain.getBlockByNumber(anyLong()))
        .then(
            requestedBlockNumber ->
                blockNumberCache.getUnchecked(requestedBlockNumber.getArgument(0, Long.class)));

    when(blockchain.getBlockHeader(any()))
        .then(
            requestedBlockHash ->
                blockHashCache
                    .getUnchecked(requestedBlockHash.getArgument(0, Hash.class))
                    .map(Block::getHeader));

    // Generate some trie logs to return for a specific block

    // For state to be moved from the archive DB segment to the archive freezer DB segment, we need
    // the archive DB segment to have the account in already
    SegmentedKeyValueStorageTransaction tx =
        testWorldStateStorage.getComposedWorldStateStorage().startTransaction();
    final BonsaiAccount block150Account =
        new BonsaiAccount(
            bonsaiWorldState,
            address,
            address.addressHash(),
            12,
            Wei.fromHexString("0x123"),
            Hash.EMPTY,
            Hash.EMPTY,
            false,
            new CodeCache());
    final BonsaiAccount block151Account =
        new BonsaiAccount(
            bonsaiWorldState,
            address,
            address.addressHash(),
            13,
            Wei.fromHexString("0x234"),
            Hash.EMPTY,
            Hash.EMPTY,
            false,
            new CodeCache());
    final BonsaiAccount block152Account =
        new BonsaiAccount(
            bonsaiWorldState,
            address,
            address.addressHash(),
            14,
            Wei.fromHexString("0x345"),
            Hash.EMPTY,
            Hash.EMPTY,
            false,
            new CodeCache());
    // The key for a bonsai-archive flat DB account entry is suffixed with the block number where
    // that state change took place, hence the "0x0000000000000096" suffix to the address hash below
    BytesValueRLPOutput out = new BytesValueRLPOutput();
    block150Account.writeTo(out);
    tx.put(
        KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE,
        Arrays.concatenate(
            address.addressHash().getBytes().toArrayUnsafe(),
            Bytes.fromHexString("0x0000000000000096").toArrayUnsafe()),
        out.encoded().toArrayUnsafe());
    out = new BytesValueRLPOutput();
    block151Account.writeTo(out);
    tx.put(
        KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE,
        Arrays.concatenate(
            address.addressHash().getBytes().toArrayUnsafe(),
            Bytes.fromHexString("0x0000000000000097").toArrayUnsafe()),
        out.encoded().toArrayUnsafe());
    out = new BytesValueRLPOutput();
    block152Account.writeTo(out);
    tx.put(
        KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE,
        Arrays.concatenate(
            address.addressHash().getBytes().toArrayUnsafe(),
            Bytes.fromHexString("0x0000000000000098").toArrayUnsafe()),
        out.encoded().toArrayUnsafe());
    tx.commit();

    // Simulate an account change in block 151. This state will be archived because block 152
    // updates the same account (see below)
    TrieLogLayer block151TrieLogs = new TrieLogLayer();
    PmtStateTrieAccountValue oldValue =
        new PmtStateTrieAccountValue(12, Wei.fromHexString("0x123"), Hash.EMPTY, Hash.EMPTY);
    PmtStateTrieAccountValue newValue =
        new PmtStateTrieAccountValue(13, Wei.fromHexString("0x234"), Hash.EMPTY, Hash.EMPTY);
    block151TrieLogs.addAccountChange(address, oldValue, newValue);

    // Simulate another change to the same account, this time in block 152. This change won't be
    // archived during the test because it is the current state of the account.
    TrieLogLayer block152TrieLogs = new TrieLogLayer();
    oldValue = new PmtStateTrieAccountValue(13, Wei.fromHexString("0x234"), Hash.EMPTY, Hash.EMPTY);
    newValue = new PmtStateTrieAccountValue(14, Wei.fromHexString("0x345"), Hash.EMPTY, Hash.EMPTY);
    block152TrieLogs.addAccountChange(address, oldValue, newValue);
    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0x62f948556539c8af8f44dd080bc2366fc361eac68e5623313a42323e48cb3f8e"))) // Block 151
        .thenReturn(Optional.of(block151TrieLogs));
    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0x8d6a523f547ee224ba533b34034a3056838f2dab3daf0ffbf75713daf18bf885"))) // Block 152
        .thenReturn(Optional.of(block152TrieLogs));

    // Initialize the archiver
    BonsaiArchiver archiver =
        new BonsaiArchiver(
            testWorldStateStorage,
            blockchain,
            executeAsync,
            trieLogManager,
            new NoOpMetricsSystem());
    archiver.initialize();

    // Chain height is 150, we've archived state up to block 150
    currentBlockHeight = SHORT_TEST_CHAIN_HEIGHT;
    when(blockchain.getChainHeadBlockNumber())
        .then(requestedBlockNumber -> getCurrentBlockHeight());
    assertThat(archiver.getPendingBlocksCount()).isEqualTo(0);

    // Process the next 50 blocks 151-200 and count the archive changes. We'll recreate the
    // block cache so we can generate blocks beyond 150
    blockNumberCache =
        CacheBuilder.newBuilder()
            .maximumSize(LONG_TEST_CHAIN_HEIGHT)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Block> load(final Long blockNumber) {
                    return getGeneratedBlock(blockNumber, LONG_TEST_CHAIN_HEIGHT);
                  }
                });

    blockHashCache =
        CacheBuilder.newBuilder()
            .maximumSize(LONG_TEST_CHAIN_HEIGHT)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Block> load(final Hash blockHash) {
                    Optional<Block> foundBlock;
                    for (long i = 0; i <= LONG_TEST_CHAIN_HEIGHT; i++) {
                      if ((foundBlock = blockNumberCache.getUnchecked(i)).isPresent()
                          && foundBlock.get().getHash().equals(blockHash)) {
                        return foundBlock;
                      }
                    }
                    return Optional.empty();
                  }
                });

    // By default we archive state for chainheight - 10 blocks, so importing up to block 210 whould
    // cause blocks up to 200 to be archived
    for (long nextBlock = 151; nextBlock <= 211; nextBlock++) {
      currentBlockHeight = nextBlock;
      archiver.onBlockAdded(
          BlockAddedEvent.createForStoredOnly(blockNumberCache.getUnchecked(nextBlock).get()));
    }

    // We should have marked up to block 200 as archived
    assertThat(testWorldStateStorage.getLatestArchivedBlock().get()).isEqualTo(200);

    // Only the latest/current state of the account should be in the archive DB segment
    assertThat(
            testWorldStateStorage.getComposedWorldStateStorage().stream(
                    KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE)
                .count())
        .isEqualTo(1);

    // Both the previous account states should be in the archive freezer segment, plus the special
    // key that records the latest archived block
    assertThat(
            testWorldStateStorage.getComposedWorldStateStorage().stream(
                    KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_FREEZER)
                .count())
        .isEqualTo(3);

    // Check the entries are in the correct segment
    assertThat(
            testWorldStateStorage
                .getComposedWorldStateStorage()
                .containsKey(
                    KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_FREEZER,
                    Arrays.concatenate(
                        address.addressHash().getBytes().toArrayUnsafe(),
                        Bytes.fromHexString("0x0000000000000096").toArrayUnsafe())))
        .isTrue();
    assertThat(
            testWorldStateStorage
                .getComposedWorldStateStorage()
                .containsKey(
                    KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_FREEZER,
                    Arrays.concatenate(
                        address.addressHash().getBytes().toArrayUnsafe(),
                        Bytes.fromHexString("0x0000000000000097").toArrayUnsafe())))
        .isTrue();
    assertThat(
            testWorldStateStorage
                .getComposedWorldStateStorage()
                .containsKey(
                    KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE,
                    Arrays.concatenate(
                        address.addressHash().getBytes().toArrayUnsafe(),
                        Bytes.fromHexString("0x0000000000000098").toArrayUnsafe())))
        .isTrue();
  }

  @Test
  public void archiverInMemoryDBArchivesStorageStateCorrectly() {
    final BonsaiPreImageProxy preImageProxy =
        new BonsaiPreImageProxy.BonsaiReferenceTestPreImageProxy();

    final BonsaiWorldStateKeyValueStorage bonsaiWorldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG);

    final BonsaiReferenceTestWorldStateStorage testWorldStateStorage =
        new BonsaiReferenceTestWorldStateStorage(bonsaiWorldStateKeyValueStorage, preImageProxy);

    // Simulate post-migration state: archive migration has completed, mode is ARCHIVE
    bonsaiWorldStateKeyValueStorage.upgradeToArchiveFlatDbMode();
    assertThat(testWorldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE);

    // Assume we've archived up to block 150L i.e. we're up to date with the chain head
    // (SHORT_TEST_CHAIN_HEIGHT)
    testWorldStateStorage.setLatestArchivedBlock(150L);

    // Set up the block cache
    blockNumberCache =
        CacheBuilder.newBuilder()
            .maximumSize(SHORT_TEST_CHAIN_HEIGHT)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Block> load(final Long blockNumber) {
                    return getGeneratedBlock(blockNumber, SHORT_TEST_CHAIN_HEIGHT);
                  }
                });

    // When any block is asked for by the archiver, generate it on the fly, cache it, and
    // return it unless it
    when(blockchain.getBlockByNumber(anyLong()))
        .then(
            requestedBlockNumber ->
                blockNumberCache.getUnchecked(requestedBlockNumber.getArgument(0, Long.class)));

    when(blockchain.getBlockHeader(any()))
        .then(
            requestedBlockHash ->
                blockHashCache
                    .getUnchecked(requestedBlockHash.getArgument(0, Hash.class))
                    .map(Block::getHeader));

    // Generate some trie logs to return for a specific block

    // For storage to be moved from the archive DB segment to the archive freezer DB segment, we
    // need the archive DB segment to have the storage in already
    SegmentedKeyValueStorageTransaction tx =
        testWorldStateStorage.getComposedWorldStateStorage().startTransaction();
    StorageSlotKey slotKey = new StorageSlotKey(UInt256.fromHexString("0x1"));
    // The key for a bonsai-archive flat DB storage entry is suffixed with the block number where
    // that state change took place, hence the "0x0000000000000096" suffix to the address hash below
    tx.put(
        KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE,
        Arrays.concatenate(
            address.addressHash().getBytes().toArrayUnsafe(),
            slotKey.getSlotHash().getBytes().toArrayUnsafe(),
            Bytes.fromHexString("0x0000000000000096").toArrayUnsafe()),
        Bytes.fromHexString("0x0123").toArrayUnsafe());
    tx.put(
        KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE,
        Arrays.concatenate(
            address.addressHash().getBytes().toArrayUnsafe(),
            slotKey.getSlotHash().getBytes().toArrayUnsafe(),
            Bytes.fromHexString("0x0000000000000097").toArrayUnsafe()),
        Bytes.fromHexString("0x0234").toArrayUnsafe());
    tx.put(
        KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE,
        Arrays.concatenate(
            address.addressHash().getBytes().toArrayUnsafe(),
            slotKey.getSlotHash().getBytes().toArrayUnsafe(),
            Bytes.fromHexString("0x0000000000000098").toArrayUnsafe()),
        Bytes.fromHexString("0x0345").toArrayUnsafe());
    tx.put(
        KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE,
        Arrays.concatenate(
            address.addressHash().getBytes().toArrayUnsafe(),
            slotKey.getSlotHash().getBytes().toArrayUnsafe(),
            Bytes.fromHexString("0x0000000000000099").toArrayUnsafe()),
        Bytes.fromHexString("0x0456").toArrayUnsafe());
    tx.commit();

    // Simulate a storage change in block 151. This state will be archived because block 152 updates
    // the same storage (see below)
    TrieLogLayer block151TrieLogs = new TrieLogLayer();
    UInt256 oldValue = UInt256.fromHexString("0x123");
    UInt256 newValue = UInt256.fromHexString("0x234");
    UInt256 slot = UInt256.ONE;
    StorageSlotKey storageSlotKey = new StorageSlotKey(slot);
    block151TrieLogs.addStorageChange(address, storageSlotKey, oldValue, newValue);

    // Simulate a storage change in block 152. This state will also be archived because block 152
    // updates the same storage (see below)
    TrieLogLayer block152TrieLogs = new TrieLogLayer();
    oldValue = UInt256.fromHexString("0x234");
    newValue = UInt256.fromHexString("0x345");
    slot = UInt256.ONE;
    storageSlotKey = new StorageSlotKey(slot);
    block152TrieLogs.addStorageChange(address, storageSlotKey, oldValue, newValue);

    // Simulate a storage change in block 153. This state will not be archived because it refers to
    // a different slot
    TrieLogLayer block153TrieLogs = new TrieLogLayer();
    oldValue = UInt256.fromHexString("0x345");
    newValue = UInt256.fromHexString("0x456");
    slot = UInt256.ONE;
    storageSlotKey = new StorageSlotKey(slot);
    block153TrieLogs.addStorageChange(address, storageSlotKey, oldValue, newValue);

    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0x62f948556539c8af8f44dd080bc2366fc361eac68e5623313a42323e48cb3f8e"))) // Block 151
        .thenReturn(Optional.of(block151TrieLogs));
    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0x8d6a523f547ee224ba533b34034a3056838f2dab3daf0ffbf75713daf18bf885"))) // Block 152
        .thenReturn(Optional.of(block152TrieLogs));
    when(trieLogManager.getTrieLogLayer(
            Hash.fromHexString(
                "0xffce5e5e58cc2737a50076e4dce8c7c715968b98a52942dc2072df4b6941d1ca"))) // Block 153
        .thenReturn(Optional.of(block153TrieLogs));

    // Initialize the archiver
    BonsaiArchiver archiver =
        new BonsaiArchiver(
            testWorldStateStorage,
            blockchain,
            executeAsync,
            trieLogManager,
            new NoOpMetricsSystem());
    archiver.initialize();

    // Chain height is 150, we've archived state up to block 150
    currentBlockHeight = SHORT_TEST_CHAIN_HEIGHT;
    when(blockchain.getChainHeadBlockNumber())
        .then(requestedBlockNumber -> getCurrentBlockHeight());
    assertThat(archiver.getPendingBlocksCount()).isEqualTo(0);

    // Process the next 50 blocks 150-200 and count the archive changes. We'll recreate the
    // block cache so we can generate blocks beyond 150
    blockNumberCache =
        CacheBuilder.newBuilder()
            .maximumSize(LONG_TEST_CHAIN_HEIGHT)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Block> load(final Long blockNumber) {
                    return getGeneratedBlock(blockNumber, LONG_TEST_CHAIN_HEIGHT);
                  }
                });

    blockHashCache =
        CacheBuilder.newBuilder()
            .maximumSize(LONG_TEST_CHAIN_HEIGHT)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Block> load(final Hash blockHash) {
                    Optional<Block> foundBlock;
                    for (long i = 0; i < LONG_TEST_CHAIN_HEIGHT; i++) {
                      if ((foundBlock = blockNumberCache.getUnchecked(i)).isPresent()
                          && foundBlock.get().getHash().equals(blockHash)) {
                        return foundBlock;
                      }
                    }
                    return Optional.empty();
                  }
                });

    // By default we archive state for chainheight - 10 blocks, so importing up to block 210 whould
    // cause blocks up to 200 to be archived
    for (long nextBlock = 151; nextBlock <= 211; nextBlock++) {
      currentBlockHeight = nextBlock;
      archiver.onBlockAdded(
          BlockAddedEvent.createForStoredOnly(blockNumberCache.getUnchecked(nextBlock).get()));
    }

    // We should have marked up to block 200 as archived
    assertThat(testWorldStateStorage.getLatestArchivedBlock().get()).isEqualTo(200);

    // Only the latest/current state of the account should be in the archive DB segment
    assertThat(
            testWorldStateStorage.getComposedWorldStateStorage().stream(
                    KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE)
                .count())
        .isEqualTo(1);

    // All 3 previous storage states should be in the storage archive freezer
    assertThat(
            testWorldStateStorage.getComposedWorldStateStorage().stream(
                    KeyValueSegmentIdentifier.ACCOUNT_STORAGE_FREEZER)
                .count())
        .isEqualTo(3);

    // Check the entries are in the correct segment
    assertThat(
            testWorldStateStorage
                .getComposedWorldStateStorage()
                .containsKey(
                    KeyValueSegmentIdentifier.ACCOUNT_STORAGE_FREEZER,
                    Arrays.concatenate(
                        address.addressHash().getBytes().toArrayUnsafe(),
                        slotKey.getSlotHash().getBytes().toArrayUnsafe(),
                        Bytes.fromHexString("0x0000000000000096").toArrayUnsafe())))
        .isTrue();
    assertThat(
            testWorldStateStorage
                .getComposedWorldStateStorage()
                .containsKey(
                    KeyValueSegmentIdentifier.ACCOUNT_STORAGE_FREEZER,
                    Arrays.concatenate(
                        address.addressHash().getBytes().toArrayUnsafe(),
                        slotKey.getSlotHash().getBytes().toArrayUnsafe(),
                        Bytes.fromHexString("0x0000000000000097").toArrayUnsafe())))
        .isTrue();
    assertThat(
            testWorldStateStorage
                .getComposedWorldStateStorage()
                .containsKey(
                    KeyValueSegmentIdentifier.ACCOUNT_STORAGE_FREEZER,
                    Arrays.concatenate(
                        address.addressHash().getBytes().toArrayUnsafe(),
                        slotKey.getSlotHash().getBytes().toArrayUnsafe(),
                        Bytes.fromHexString("0x0000000000000098").toArrayUnsafe())))
        .isTrue();
    assertThat(
            testWorldStateStorage
                .getComposedWorldStateStorage()
                .containsKey(
                    KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE,
                    Arrays.concatenate(
                        address.addressHash().getBytes().toArrayUnsafe(),
                        slotKey.getSlotHash().getBytes().toArrayUnsafe(),
                        Bytes.fromHexString("0x0000000000000099").toArrayUnsafe())))
        .isTrue();
  }
}
