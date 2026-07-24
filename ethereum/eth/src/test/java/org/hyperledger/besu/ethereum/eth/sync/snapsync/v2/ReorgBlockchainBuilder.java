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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.mockito.Mockito;

class ReorgBlockchainBuilder {

  private final BlockDataGenerator gen = new BlockDataGenerator(1);
  private final DefaultBlockchain blockchain;

  ReorgBlockchainBuilder() {
    final Block genesis = gen.genesisBlock();
    this.blockchain = createMutableBlockchain(genesis);
    append(genesis, Optional.empty(), gen.receipts(genesis));
  }

  private DefaultBlockchain createMutableBlockchain(final Block genesis) {
    return (DefaultBlockchain)
        DefaultBlockchain.createMutable(
            genesis,
            new KeyValueStoragePrefixedKeyBlockchainStorage(
                new InMemoryKeyValueStorage(),
                new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
                new MainnetBlockHeaderFunctions(),
                false),
            new org.hyperledger.besu.metrics.noop.NoOpMetricsSystem(),
            0);
  }

  public DefaultBlockchain blockchain() {
    return blockchain;
  }

  public BlockDataGenerator generator() {
    return gen;
  }

  /**
   * Synthetic fork-choice weights used to drive {@link
   * org.hyperledger.besu.ethereum.chain.DefaultBlockchain}'s heaviest-chain rule. Real difficulty
   * is 0 for the post-merge blocks and fork choice is attestation-based; the test blockchain has no
   * attestations, so difficulty is borrowed as a stand-in.
   */
  private static final Difficulty LOW = Difficulty.of(10L);

  private static final Difficulty HIGH = Difficulty.of(100L);

  record BlockWithBal(Block block, Optional<BlockAccessList> bal) {}

  /**
   * Builds a block that extends {@code parentHeader}, carries the given BAL (and a matching header
   * balHash), and has the requested difficulty.
   *
   * <p>The header is built with all post-EIP-7928 optional fields populated so that {@code balHash}
   * survives the RLP round-trip through blockchain storage (the header encoder only serializes
   * {@code balHash} when baseFee, withdrawalsRoot, blob fields, parentBeaconBlockRoot and
   * requestsHash are all present).
   */
  public BlockWithBal blockWithBal(
      final BlockHeader parentHeader,
      final BlockAccessList bal,
      final Difficulty difficulty,
      final long blockNumber) {
    final Hash balHash = BodyValidation.balHash(bal);
    final BlockHeader header =
        BlockHeaderBuilder.createDefault()
            .parentHash(parentHeader.getHash())
            .number(blockNumber)
            .difficulty(difficulty)
            .baseFee(Wei.ZERO)
            .withdrawalsRoot(Hash.EMPTY_TRIE_HASH)
            .parentBeaconBlockRoot(Bytes32.ZERO)
            .requestsHash(Hash.EMPTY)
            .balHash(balHash)
            .buildBlockHeader();
    final Block block = new Block(header, BlockBody.empty());
    return new BlockWithBal(block, Optional.of(bal));
  }

  public void append(final Block block, final Optional<BlockAccessList> bal) {
    blockchain.appendBlock(block, gen.receipts(block), bal);
  }

  void append(
      final Block block,
      final Optional<BlockAccessList> bal,
      final List<TransactionReceipt> receipts) {
    blockchain.appendBlock(block, receipts, bal);
  }

  /**
   * Builds a neutral block (common ancestor or a straight, non-competing chain) with the given BAL,
   * appends both, and returns the block.
   */
  public Block appendBlockWithBal(
      final BlockHeader parentHeader, final BlockAccessList bal, final long blockNumber) {
    return appendBlockWithBal(parentHeader, bal, LOW, blockNumber);
  }

  /** Appends a block destined to be orphaned by a canonical sibling. */
  public Block appendStale(
      final BlockHeader parentHeader, final BlockAccessList bal, final long blockNumber) {
    return appendBlockWithBal(parentHeader, bal, LOW, blockNumber);
  }

  /** Appends a block that wins the reorg against any stale sibling. */
  public Block appendCanonical(
      final BlockHeader parentHeader, final BlockAccessList bal, final long blockNumber) {
    return appendBlockWithBal(parentHeader, bal, HIGH, blockNumber);
  }

  /**
   * Builds a stale block whose header carries the BAL hash but deliberately does NOT store the BAL,
   * simulating a locally pruned orphaned BAL.
   */
  public Block appendStaleWithoutStoringBal(
      final BlockHeader parentHeader, final BlockAccessList bal, final long blockNumber) {
    return appendWithoutStoringBal(parentHeader, bal, LOW, blockNumber);
  }

  /**
   * Builds a canonical block whose header carries the BAL hash but deliberately does NOT store the
   * BAL, simulating a locally pruned canonical BAL.
   */
  public Block appendCanonicalWithoutStoringBal(
      final BlockHeader parentHeader, final BlockAccessList bal, final long blockNumber) {
    return appendWithoutStoringBal(parentHeader, bal, HIGH, blockNumber);
  }

  /**
   * Builds a canonical block whose header commits to {@code headerBal} while storing {@code
   * storedBal} instead, simulating locally corrupted BAL data that fails the balHash check.
   */
  public Block appendCanonicalWithMismatchedBal(
      final BlockHeader parentHeader,
      final BlockAccessList headerBal,
      final BlockAccessList storedBal,
      final long blockNumber) {
    final Block block = blockWithBal(parentHeader, headerBal, HIGH, blockNumber).block();
    append(block, Optional.of(storedBal));
    return block;
  }

  /** Appends {@code count} stale empty-BAL blocks starting after {@code parentHeader}. */
  public BlockHeader appendStaleChain(
      final BlockHeader parentHeader, final long startNumber, final long count) {
    return appendEmptyChain(parentHeader, startNumber, count, LOW);
  }

  /** Appends {@code count} canonical empty-BAL blocks starting after {@code parentHeader}. */
  public BlockHeader appendCanonicalChain(
      final BlockHeader parentHeader, final long startNumber, final long count) {
    return appendEmptyChain(parentHeader, startNumber, count, HIGH);
  }

  private Block appendBlockWithBal(
      final BlockHeader parentHeader,
      final BlockAccessList bal,
      final Difficulty difficulty,
      final long blockNumber) {
    final Block block = blockWithBal(parentHeader, bal, difficulty, blockNumber).block();
    append(block, Optional.of(bal));
    return block;
  }

  private Block appendWithoutStoringBal(
      final BlockHeader parentHeader,
      final BlockAccessList bal,
      final Difficulty difficulty,
      final long blockNumber) {
    final Block block = blockWithBal(parentHeader, bal, difficulty, blockNumber).block();
    append(block, Optional.empty());
    return block;
  }

  private BlockHeader appendEmptyChain(
      final BlockHeader parentHeader,
      final long startNumber,
      final long count,
      final Difficulty difficulty) {
    BlockHeader prev = parentHeader;
    for (long n = startNumber; n < startNumber + count; n++) {
      prev = appendBlockWithBal(prev, emptyBal(), difficulty, n).getHeader();
    }
    return prev;
  }

  /** Returns a schedule that reports BAL as enabled for every block. */
  static ProtocolSchedule balEnabledSchedule() {
    final ProtocolSpec spec = Mockito.mock(ProtocolSpec.class);
    Mockito.when(spec.isBlockAccessListEnabled()).thenReturn(true);
    final ProtocolSchedule schedule = Mockito.mock(ProtocolSchedule.class);
    Mockito.when(schedule.getByBlockHeader(Mockito.any())).thenReturn(spec);
    return schedule;
  }

  /** Returns a schedule that reports BAL as disabled for every block. */
  static ProtocolSchedule balDisabledSchedule() {
    final ProtocolSpec spec = Mockito.mock(ProtocolSpec.class);
    Mockito.when(spec.isBlockAccessListEnabled()).thenReturn(false);
    final ProtocolSchedule schedule = Mockito.mock(ProtocolSchedule.class);
    Mockito.when(schedule.getByBlockHeader(Mockito.any())).thenReturn(spec);
    return schedule;
  }

  // ---- BAL construction helpers ----

  BlockAccessList balWithBalances(final Map<Address, Wei> balances) {
    final List<BlockAccessList.AccountChanges> changes = new ArrayList<>();
    for (final Map.Entry<Address, Wei> entry : balances.entrySet()) {
      changes.add(
          new BlockAccessList.AccountChanges(
              entry.getKey(),
              List.of(),
              List.of(),
              List.of(new BlockAccessList.BalanceChange(0, entry.getValue())),
              List.of(),
              List.of()));
    }
    return finalizeBal(changes);
  }

  BlockAccessList balWithStorageChanges(
      final Address account, final Map<UInt256, UInt256> slotValues) {
    final List<BlockAccessList.SlotChanges> slotChanges = new ArrayList<>();
    for (final Map.Entry<UInt256, UInt256> entry : slotValues.entrySet()) {
      slotChanges.add(
          new BlockAccessList.SlotChanges(
              new StorageSlotKey(entry.getKey()),
              List.of(new BlockAccessList.StorageChange(0, entry.getValue()))));
    }
    return finalizeBal(
        List.of(
            new BlockAccessList.AccountChanges(
                account, slotChanges, List.of(), List.of(), List.of(), List.of())));
  }

  BlockAccessList balWithStorageReads(final Address address, final UInt256... slotKeys) {
    final List<BlockAccessList.SlotRead> reads = new ArrayList<>();
    for (final UInt256 slotKey : slotKeys) {
      reads.add(new BlockAccessList.SlotRead(new StorageSlotKey(slotKey)));
    }
    return finalizeBal(
        List.of(
            new BlockAccessList.AccountChanges(
                address, List.of(), reads, List.of(), List.of(), List.of())));
  }

  BlockAccessList balWithNonceChange(final Address address, final long newNonce) {
    return finalizeBal(
        List.of(
            new BlockAccessList.AccountChanges(
                address,
                List.of(),
                List.of(),
                List.of(),
                List.of(new BlockAccessList.NonceChange(0, newNonce)),
                List.of())));
  }

  BlockAccessList balWithCodeChange(final Address address, final Bytes newCode) {
    return finalizeBal(
        List.of(
            new BlockAccessList.AccountChanges(
                address,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(new BlockAccessList.CodeChange(0, newCode)))));
  }

  BlockAccessList merge(final BlockAccessList... bals) {
    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    for (final BlockAccessList bal : bals) {
      builder.mergeFrom(bal);
    }
    return finalizeBal(builder.build().accountChanges());
  }

  BlockAccessList balWithBalanceTouches(final Address... addresses) {
    final List<BlockAccessList.AccountChanges> changes = new ArrayList<>();
    for (final Address address : addresses) {
      changes.add(
          new BlockAccessList.AccountChanges(
              address,
              List.of(),
              List.of(),
              List.of(new BlockAccessList.BalanceChange(0, Wei.ONE)),
              List.of(),
              List.of()));
    }
    return finalizeBal(changes);
  }

  BlockAccessList emptyBal() {
    return finalizeBal(List.of());
  }

  private static BlockAccessList finalizeBal(final List<BlockAccessList.AccountChanges> changes) {
    final BlockAccessList noRlp = new BlockAccessList(changes);
    return new BlockAccessList(changes, noRlp.encode());
  }

  static Hash slotHash(final UInt256 slotKey) {
    return new StorageSlotKey(slotKey).getSlotHash();
  }

  BlockHeader header(final long number) {
    return blockchain.getBlockHeader(number).orElseThrow();
  }
}
