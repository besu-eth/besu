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
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockFootprints.Cell;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockFootprints.Footprint;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockFootprints.RawWrite;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockFootprints.Read;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockFootprints.Write;
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

class BlockFootprintsTest {

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
  private static Footprint readWrite(final long parent, final long read, final long write) {
    return footprint(
        new Read[] {new Read(SLOT, v(read), v(parent))},
        read == write ? new Write[0] : new Write[] {new Write(SLOT, v(write))},
        Set.of(),
        false);
  }

  private static Footprint footprint(
      final Read[] reads,
      final Write[] writes,
      final Set<Address> cleared,
      final boolean refreshed) {
    return new Footprint(null, reads, writes, cleared, refreshed, true);
  }

  @Test
  void aReaderStoredBeforeItsWriterIsInvalidatedByIt() {
    final BlockFootprints footprints = new BlockFootprints(3);
    footprints.store(1, readWrite(10, 10, 11), true);

    footprints.store(0, readWrite(10, 10, 11), true);

    assertThat(footprints.takeInvalidated(-1)).isEqualTo(1);
    assertThat(footprints.takeInvalidated(-1)).isEqualTo(-1);
  }

  @Test
  void aReaderStoredAfterItsWriterInvalidatesItself() {
    final BlockFootprints footprints = new BlockFootprints(3);
    footprints.store(0, readWrite(10, 10, 11), true);

    footprints.store(1, readWrite(10, 10, 11), true);

    assertThat(footprints.takeInvalidated(-1)).isEqualTo(1);
  }

  @Test
  void aReaderOfTheValueItsWriterLeavesStaysValid() {
    final BlockFootprints footprints = new BlockFootprints(3);
    footprints.store(0, readWrite(10, 10, 11), true);

    footprints.store(1, readWrite(10, 11, 12), true);

    assertThat(footprints.takeInvalidated(-1)).isEqualTo(-1);
  }

  @Test
  void executedWritesReplaceWhatTheFootprintPredicted() {
    final BlockFootprints footprints = new BlockFootprints(3);
    // predicted to leave 11, which the reader read
    footprints.store(0, readWrite(10, 10, 11), true);
    footprints.store(1, readWrite(10, 11, 12), true);
    assertThat(footprints.takeInvalidated(-1)).isEqualTo(-1);

    footprints.reportExecuted(0, List.of(new RawWrite(CONTRACT, KEY, v(50))), Set.of());
    footprints.applyReported();

    assertThat(footprints.takeInvalidated(-1)).isEqualTo(1);
  }

  @Test
  void aTakenFootprintKeepsItsPredictedWrites() {
    final BlockFootprints footprints = new BlockFootprints(3);
    footprints.store(0, readWrite(10, 10, 11), true);
    footprints.store(1, readWrite(10, 11, 12), true);

    footprints.reportTaken(0);
    footprints.applyReported();

    assertThat(footprints.takeInvalidated(-1)).isEqualTo(-1);
    // a later reader still sees what the taken transaction wrote
    footprints.store(2, readWrite(10, 10, 11), true);
    assertThat(footprints.takeInvalidated(-1)).isEqualTo(2);
  }

  @Test
  void anExecutedPositionTakesNoFootprintAndIsNotInvalidated() {
    final BlockFootprints footprints = new BlockFootprints(3);
    footprints.reportExecuted(1, List.of(), Set.of());
    footprints.applyReported();

    assertThat(footprints.store(1, readWrite(10, 10, 11), false)).isFalse();
    footprints.store(0, readWrite(10, 10, 11), true);
    assertThat(footprints.takeInvalidated(-1)).isEqualTo(-1);
  }

  @Test
  void onlyIfEmptyKeepsTheFootprintStoredFirst() {
    final BlockFootprints footprints = new BlockFootprints(2);
    final Footprint refreshed =
        footprint(new Read[] {new Read(SLOT, v(11), v(10))}, new Write[0], Set.of(), true);
    footprints.store(1, refreshed, false);

    assertThat(footprints.store(1, readWrite(10, 10, 11), true)).isFalse();
    assertThat(footprints.latest(1)).isSameAs(refreshed);
  }

  @Test
  void seedsAreTheValuesTheWritesBeforeLeave() {
    final BlockFootprints footprints = new BlockFootprints(3);
    footprints.store(0, readWrite(10, 10, 11), true);
    final Footprint reader = readWrite(10, 10, 11);
    footprints.store(1, reader, true);

    final Map<Address, Map<StorageSlotKey, UInt256>> seeds = footprints.seedsFor(1, reader);

    assertThat(seeds).containsOnlyKeys(CONTRACT);
    assertThat(seeds.get(CONTRACT)).containsExactly(Map.entry(SLOT.slot(), v(11)));
  }

  @Test
  void noSeedsWhenNothingMoved() {
    final BlockFootprints footprints = new BlockFootprints(3);
    footprints.store(0, readWrite(10, 10, 11), true);
    final Footprint reader = readWrite(10, 11, 12);
    footprints.store(1, reader, true);

    assertThat(footprints.seedsFor(1, reader)).isNull();
  }

  @Test
  void aRefreshedFootprintReadingASeedWhoseWriterIsGoneMoves() {
    final BlockFootprints footprints = new BlockFootprints(3);
    // refreshed on 11, which no transaction before it writes any more: the slot is at the parent's
    final Footprint refreshed =
        footprint(new Read[] {new Read(SLOT, v(11), v(10))}, new Write[0], Set.of(), true);
    footprints.store(1, refreshed, false);

    assertThat(footprints.takeInvalidated(-1)).isEqualTo(1);
    assertThat(footprints.seedsFor(1, refreshed)).isEmpty();
  }

  @Test
  void aClearedAccountIsNotTrackedAndItsReadersAreNotRefreshed() {
    final BlockFootprints footprints = new BlockFootprints(3);
    footprints.store(0, footprint(new Read[0], new Write[0], Set.of(CONTRACT), false), true);
    final Footprint reader = readWrite(10, 10, 11);
    footprints.store(1, reader, true);

    assertThat(footprints.takeInvalidated(-1)).isEqualTo(-1);
    assertThat(footprints.seedsFor(1, reader)).isNull();
  }

  @Test
  void aSlotWrittenByTooManyTransactionsIsNoLongerTracked() {
    final int writers = BlockFootprints.MAX_INDEXED_PER_SLOT + 1;
    final BlockFootprints footprints = new BlockFootprints(writers + 1);
    for (int i = 0; i < writers; i++) {
      footprints.store(i, readWrite(10, 10 + i, 11 + i), true);
    }
    final Footprint reader = readWrite(10, 10, 11);
    footprints.store(writers, reader, true);

    assertThat(footprints.seedsFor(writers, reader)).isNull();
  }

  @Test
  void invalidatedPositionsAreTakenInBlockOrderAfterTheGivenOne() {
    final BlockFootprints footprints = new BlockFootprints(5);
    footprints.store(1, readWrite(10, 10, 11), true);
    footprints.store(2, readWrite(10, 10, 11), true);
    footprints.store(
        3,
        footprint(
            new Read[] {new Read(OTHER_SLOT, v(0), v(0)), new Read(SLOT, v(10), v(10))},
            new Write[0],
            Set.of(),
            false),
        true);
    footprints.store(0, readWrite(10, 10, 11), true);

    // block processing is at 1: the next one, 2, is left to it
    assertThat(footprints.takeInvalidated(2)).isEqualTo(3);
    assertThat(footprints.takeInvalidated(2)).isEqualTo(-1);
  }

  @Test
  void aStoredRefreshReplacesThePredictionItsReadersRelyOn() {
    final BlockFootprints footprints = new BlockFootprints(3);
    footprints.store(0, readWrite(10, 10, 11), true);
    footprints.store(1, readWrite(10, 11, 12), true);
    footprints.store(2, readWrite(10, 12, 13), true);
    assertThat(footprints.takeInvalidated(-1)).isEqualTo(-1);

    // position 1 refreshed onto another value: the reader after it relied on its old write
    footprints.store(
        1,
        footprint(
            new Read[] {new Read(SLOT, v(11), v(10))},
            new Write[] {new Write(SLOT, v(20))},
            Set.of(),
            true),
        false);

    assertThat(footprints.takeInvalidated(-1)).isEqualTo(2);
  }

  @Test
  void aBlockRunsAtMostItsRefreshes() {
    final int readers = BlockFootprints.MAX_REFRESHES_PER_BLOCK + 1;
    final BlockFootprints footprints = new BlockFootprints(readers + 1);
    footprints.store(0, readWrite(10, 10, 11), true);
    for (int i = 1; i <= readers; i++) {
      footprints.store(i, readWrite(10, 10, 10), true);
    }

    for (int i = 0; i < BlockFootprints.MAX_REFRESHES_PER_BLOCK; i++) {
      final int position = footprints.takeInvalidated(-1);
      assertThat(footprints.seedsFor(position, footprints.latest(position))).isNotNull();
    }

    assertThat(footprints.takeInvalidated(-1)).isEqualTo(-1);
  }

  @Test
  void aRunReadsEverySlotItLoadedAndWritesTheOnesItChanged() {
    final BonsaiWorldStateUpdateAccumulator run = accumulator();
    putSlot(run, CONTRACT, 1, 10, 10);
    putSlot(run, CONTRACT, 2, 5, 7);
    putSlot(run, BENEFICIARY, 1, 0, 3);

    final Footprint footprint = Footprint.of(context(run, null, false), BENEFICIARY, null);

    assertThat(footprint.reads)
        .containsExactlyInAnyOrder(new Read(cell(1), v(10), v(10)), new Read(cell(2), v(5), v(5)));
    assertThat(footprint.writes).containsExactly(new Write(cell(2), v(7)));
    assertThat(footprint.refreshed).isFalse();
    assertThat(footprint.refreshable).isTrue();
  }

  @Test
  void aRefreshStartsFromItsSeeds() {
    final BonsaiWorldStateUpdateAccumulator run = accumulator();
    putSlot(run, CONTRACT, 1, 10, 12);
    putSlot(run, CONTRACT, 2, 5, 6);

    final Footprint footprint =
        Footprint.of(
            context(run, null, false),
            BENEFICIARY,
            Map.of(CONTRACT, Map.of(slotKey(1), v(12), slotKey(2), v(4))));

    assertThat(footprint.reads)
        .containsExactlyInAnyOrder(new Read(cell(1), v(12), v(10)), new Read(cell(2), v(4), v(5)));
    // the run left slot 1 at its seed
    assertThat(footprint.writes).containsExactly(new Write(cell(2), v(6)));
    assertThat(footprint.refreshed).isTrue();
  }

  @Test
  void aChainedRunStartsFromItsPredecessorsValues() {
    final BonsaiWorldStateUpdateAccumulator predecessor = accumulator();
    putSlot(predecessor, CONTRACT, 1, 10, 11);
    final BonsaiWorldStateUpdateAccumulator run = accumulator();
    putSlot(run, CONTRACT, 1, 10, 12);
    putSlot(run, CONTRACT, 2, 5, 5);

    final Footprint footprint = Footprint.of(context(run, predecessor, false), BENEFICIARY, null);

    assertThat(footprint.reads)
        .containsExactlyInAnyOrder(new Read(cell(1), v(11), v(10)), new Read(cell(2), v(5), v(5)));
    assertThat(footprint.writes).containsExactly(new Write(cell(1), v(12)));
  }

  @Test
  void aRunThatClearedStorageIsNotRefreshable() {
    final BonsaiWorldStateUpdateAccumulator run = accumulator();
    run.getStorageToClear().add(CONTRACT);

    final Footprint footprint = Footprint.of(context(run, null, false), BENEFICIARY, null);

    assertThat(footprint.cleared).containsExactly(CONTRACT);
    assertThat(footprint.refreshable).isFalse();
  }

  @Test
  void anInvalidRunIsNotRefreshable() {
    final Footprint footprint = Footprint.of(context(accumulator(), null, true), BENEFICIARY, null);

    assertThat(footprint.refreshable).isFalse();
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
