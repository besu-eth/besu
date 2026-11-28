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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockReadWriteSets.Cell;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockReadWriteSets.RawWrite;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockReadWriteSets.Read;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockReadWriteSets.ReadWriteSet;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockReadWriteSets.Write;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BlockReadWriteSetsTest {

  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000000c0de");
  private static final Address BENEFICIARY =
      Address.fromHexString("0x000000000000000000000000000000000000beef");
  private static final UInt256 KEY = UInt256.ONE;
  private static final Cell SLOT = new Cell(CONTRACT, new StorageSlotKey(KEY));
  private static final Cell OTHER_SLOT = new Cell(CONTRACT, new StorageSlotKey(UInt256.valueOf(2)));

  private static UInt256 v(final long value) {
    return UInt256.valueOf(value);
  }

  /**
   * A run that read the slot at {@code read} (parent {@code parent}) and left it at {@code write}.
   */
  private static ReadWriteSet readWrite(final long parent, final long read, final long write) {
    return readWriteSet(
        new Read[] {new Read(SLOT, v(read), v(parent))},
        read == write ? new Write[0] : new Write[] {new Write(SLOT, v(write))},
        Set.of(),
        false);
  }

  private static ReadWriteSet readWriteSet(
      final Read[] reads,
      final Write[] writes,
      final Set<Address> cleared,
      final boolean refreshed) {
    return new ReadWriteSet(null, reads, writes, cleared, refreshed, true);
  }

  @Test
  void aReaderStoredBeforeItsWriterIsInvalidatedByIt() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    readWriteSets.store(1, readWrite(10, 10, 11), true);

    readWriteSets.store(0, readWrite(10, 10, 11), true);

    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(1);
    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(-1);
  }

  @Test
  void aReaderStoredAfterItsWriterInvalidatesItself() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    readWriteSets.store(0, readWrite(10, 10, 11), true);

    readWriteSets.store(1, readWrite(10, 10, 11), true);

    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(1);
  }

  @Test
  void aReaderOfTheValueItsWriterLeavesStaysValid() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    readWriteSets.store(0, readWrite(10, 10, 11), true);

    readWriteSets.store(1, readWrite(10, 11, 12), true);

    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(-1);
  }

  @Test
  void executedWritesReplaceWhatTheReadWriteSetPredicted() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    // predicted to leave 11, which the reader read
    readWriteSets.store(0, readWrite(10, 10, 11), true);
    readWriteSets.store(1, readWrite(10, 11, 12), true);
    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(-1);

    readWriteSets.reportExecuted(0, List.of(new RawWrite(CONTRACT, KEY, v(50))), Set.of());
    readWriteSets.applyReported();

    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(1);
  }

  @Test
  void aTakenReadWriteSetKeepsItsPredictedWrites() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    readWriteSets.store(0, readWrite(10, 10, 11), true);
    readWriteSets.store(1, readWrite(10, 11, 12), true);

    readWriteSets.reportTaken(0);
    readWriteSets.applyReported();

    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(-1);

    // a later reader still sees what the taken transaction wrote
    readWriteSets.store(2, readWrite(10, 10, 11), true);
    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(2);
  }

  @Test
  void anExecutedPositionTakesNoReadWriteSetAndIsNotInvalidated() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    readWriteSets.reportExecuted(1, List.of(), Set.of());
    readWriteSets.applyReported();

    assertThat(readWriteSets.store(1, readWrite(10, 10, 11), false)).isFalse();

    readWriteSets.store(0, readWrite(10, 10, 11), true);
    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(-1);
  }

  @Test
  void onlyIfEmptyKeepsTheReadWriteSetStoredFirst() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(2);
    final ReadWriteSet refreshed =
        readWriteSet(new Read[] {new Read(SLOT, v(11), v(10))}, new Write[0], Set.of(), true);
    readWriteSets.store(1, refreshed, false);

    assertThat(readWriteSets.store(1, readWrite(10, 10, 11), true)).isFalse();
    assertThat(readWriteSets.latest(1)).isSameAs(refreshed);
  }

  @Test
  void seedsAreTheValuesTheWritesBeforeLeave() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    readWriteSets.store(0, readWrite(10, 10, 11), true);
    final ReadWriteSet reader = readWrite(10, 10, 11);
    readWriteSets.store(1, reader, true);

    final Map<Address, Map<StorageSlotKey, UInt256>> seeds = readWriteSets.seedsFor(1, reader);

    assertThat(seeds).containsOnlyKeys(CONTRACT);
    assertThat(seeds.get(CONTRACT)).containsExactly(Map.entry(SLOT.slot(), v(11)));
  }

  @Test
  void noSeedsWhenNothingMoved() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    readWriteSets.store(0, readWrite(10, 10, 11), true);
    final ReadWriteSet reader = readWrite(10, 11, 12);
    readWriteSets.store(1, reader, true);

    assertThat(readWriteSets.seedsFor(1, reader)).isNull();
  }

  @Test
  void aRefreshedReadWriteSetReadingASeedWhoseWriterIsGoneMoves() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    // refreshed on 11, which no transaction before it writes any more: the slot is at the parent's
    final ReadWriteSet refreshed =
        readWriteSet(new Read[] {new Read(SLOT, v(11), v(10))}, new Write[0], Set.of(), true);
    readWriteSets.store(1, refreshed, false);

    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(1);
    assertThat(readWriteSets.seedsFor(1, refreshed)).isEmpty();
  }

  @Test
  void aClearedAccountIsNotTrackedAndItsReadersAreNotRefreshed() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    readWriteSets.store(0, readWriteSet(new Read[0], new Write[0], Set.of(CONTRACT), false), true);
    final ReadWriteSet reader = readWrite(10, 10, 11);
    readWriteSets.store(1, reader, true);

    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(-1);
    assertThat(readWriteSets.seedsFor(1, reader)).isNull();
  }

  @Test
  void aSlotWrittenByTooManyTransactionsIsNoLongerTracked() {
    final int writers = BlockReadWriteSets.MAX_INDEXED_PER_SLOT + 1;
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(writers + 1);
    for (int i = 0; i < writers; i++) {
      readWriteSets.store(i, readWrite(10, 10 + i, 11 + i), true);
    }

    final ReadWriteSet reader = readWrite(10, 10, 11);
    readWriteSets.store(writers, reader, true);

    assertThat(readWriteSets.seedsFor(writers, reader)).isNull();
  }

  @Test
  void invalidatedPositionsAreTakenInBlockOrderAfterTheGivenOne() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(5);
    readWriteSets.store(1, readWrite(10, 10, 11), true);
    readWriteSets.store(2, readWrite(10, 10, 11), true);
    readWriteSets.store(
        3,
        readWriteSet(
            new Read[] {new Read(OTHER_SLOT, v(0), v(0)), new Read(SLOT, v(10), v(10))},
            new Write[0],
            Set.of(),
            false),
        true);
    readWriteSets.store(0, readWrite(10, 10, 11), true);

    // block processing is at 1: the next one, 2, is left to it
    assertThat(readWriteSets.takeInvalidated(2)).isEqualTo(3);
    assertThat(readWriteSets.takeInvalidated(2)).isEqualTo(-1);
  }

  @Test
  void aStoredRefreshReplacesThePredictionItsReadersRelyOn() {
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(3);
    readWriteSets.store(0, readWrite(10, 10, 11), true);
    readWriteSets.store(1, readWrite(10, 11, 12), true);
    readWriteSets.store(2, readWrite(10, 12, 13), true);
    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(-1);

    // position 1 refreshed onto another value: the reader after it relied on its old write
    readWriteSets.store(
        1,
        readWriteSet(
            new Read[] {new Read(SLOT, v(11), v(10))},
            new Write[] {new Write(SLOT, v(20))},
            Set.of(),
            true),
        false);

    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(2);
  }

  @Test
  void aBlockRunsAtMostItsRefreshes() {
    final int readers = BlockReadWriteSets.MAX_REFRESHES_PER_BLOCK + 1;
    final BlockReadWriteSets readWriteSets = new BlockReadWriteSets(readers + 1);
    readWriteSets.store(0, readWrite(10, 10, 11), true);
    for (int i = 1; i <= readers; i++) {
      readWriteSets.store(i, readWrite(10, 10, 10), true);
    }

    for (int i = 0; i < BlockReadWriteSets.MAX_REFRESHES_PER_BLOCK; i++) {
      final int position = readWriteSets.takeInvalidated(-1);
      assertThat(readWriteSets.seedsFor(position, readWriteSets.latest(position))).isNotNull();
    }

    assertThat(readWriteSets.takeInvalidated(-1)).isEqualTo(-1);
  }

  @Test
  void aRunReadsEverySlotItLoadedAndWritesTheOnesItChanged() {
    final BonsaiWorldStateUpdateAccumulator run = accumulator();
    putSlot(run, CONTRACT, 1, 10, 10);
    putSlot(run, CONTRACT, 2, 5, 7);
    putSlot(run, BENEFICIARY, 1, 0, 3);

    final ReadWriteSet readWriteSet = ReadWriteSet.of(context(run, null, false), BENEFICIARY, null);

    assertThat(readWriteSet.reads)
        .containsExactlyInAnyOrder(new Read(cell(1), v(10), v(10)), new Read(cell(2), v(5), v(5)));
    assertThat(readWriteSet.writes).containsExactly(new Write(cell(2), v(7)));
    assertThat(readWriteSet.refreshed).isFalse();
    assertThat(readWriteSet.refreshable).isTrue();
  }

  @Test
  void aRefreshStartsFromItsSeeds() {
    final BonsaiWorldStateUpdateAccumulator run = accumulator();
    putSlot(run, CONTRACT, 1, 10, 12);
    putSlot(run, CONTRACT, 2, 5, 6);

    final ReadWriteSet readWriteSet =
        ReadWriteSet.of(
            context(run, null, false),
            BENEFICIARY,
            Map.of(CONTRACT, Map.of(slotKey(1), v(12), slotKey(2), v(4))));

    assertThat(readWriteSet.reads)
        .containsExactlyInAnyOrder(new Read(cell(1), v(12), v(10)), new Read(cell(2), v(4), v(5)));
    // the run left slot 1 at its seed
    assertThat(readWriteSet.writes).containsExactly(new Write(cell(2), v(6)));
    assertThat(readWriteSet.refreshed).isTrue();
  }

  @Test
  void aChainedRunStartsFromItsPredecessorsValues() {
    final BonsaiWorldStateUpdateAccumulator predecessor = accumulator();
    putSlot(predecessor, CONTRACT, 1, 10, 11);

    final BonsaiWorldStateUpdateAccumulator run = accumulator();
    putSlot(run, CONTRACT, 1, 10, 12);
    putSlot(run, CONTRACT, 2, 5, 5);

    final ReadWriteSet readWriteSet =
        ReadWriteSet.of(context(run, predecessor, false), BENEFICIARY, null);

    assertThat(readWriteSet.reads)
        .containsExactlyInAnyOrder(new Read(cell(1), v(11), v(10)), new Read(cell(2), v(5), v(5)));
    assertThat(readWriteSet.writes).containsExactly(new Write(cell(1), v(12)));
  }

  @Test
  void aRunThatClearedStorageIsNotRefreshable() {
    final BonsaiWorldStateUpdateAccumulator run = accumulator();
    run.getStorageToClear().add(CONTRACT);

    final ReadWriteSet readWriteSet = ReadWriteSet.of(context(run, null, false), BENEFICIARY, null);

    assertThat(readWriteSet.cleared).containsExactly(CONTRACT);
    assertThat(readWriteSet.refreshable).isFalse();
  }

  @Test
  void anInvalidRunIsNotRefreshable() {
    final ReadWriteSet readWriteSet =
        ReadWriteSet.of(context(accumulator(), null, true), BENEFICIARY, null);

    assertThat(readWriteSet.refreshable).isFalse();
  }

  private static Cell cell(final long slot) {
    return new Cell(CONTRACT, slotKey(slot));
  }

  private static StorageSlotKey slotKey(final long slot) {
    return new StorageSlotKey(UInt256.valueOf(slot));
  }

  private static BonsaiWorldStateUpdateAccumulator accumulator() {
    return new BonsaiWorldStateUpdateAccumulator(
        mock(BonsaiWorldState.class),
        (__, ___) -> {},
        (__, ___) -> {},
        EvmConfiguration.DEFAULT,
        new BonsaiCodeCache());
  }

  private static void putSlot(
      final BonsaiWorldStateUpdateAccumulator accumulator,
      final Address address,
      final long slot,
      final long prior,
      final long updated) {
    accumulator
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(slotKey(slot), new BonsaiValue<>(v(prior), v(updated)));
  }

  private static ParallelizedTransactionContext context(
      final BonsaiWorldStateUpdateAccumulator run,
      final BonsaiWorldStateUpdateAccumulator predecessor,
      final boolean invalid) {
    final TransactionProcessingResult result = mock(TransactionProcessingResult.class);
    when(result.isInvalid()).thenReturn(invalid);
    return new ParallelizedTransactionContext(run, result, false, Wei.ZERO, predecessor);
  }
}
