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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.PREWARM_COINBASE;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.READS_COINBASE;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.READS_GAS_LIMIT;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.READS_PREVRANDAO;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.READS_TIMESTAMP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.Execution;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class PrewarmReuseTest {

  private static final Hash HEAD = Hash.hash(Bytes.of(1));
  private static final Address COINBASE = Address.fromHexString("0xc0");
  private static final Address SENDER = Address.fromHexString("0x5e");
  private static final Address CONTRACT = Address.fromHexString("0xc7");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.ONE);
  private static final long TIMESTAMP = 1_000L;
  private static final long GAS_LIMIT = 30_000_000L;

  private final BlockHeader block =
      new BlockHeaderTestFixture()
          .parentHash(HEAD)
          .timestamp(TIMESTAMP)
          .gasLimit(GAS_LIMIT)
          .coinbase(COINBASE)
          .buildHeader();
  private final PrewarmedTransactions prewarmed =
      new PrewarmedTransactions(new NoOpMetricsSystem());
  private final Transaction transaction = transaction(1);

  @Test
  void takesAResultWhoseReadsTheBlockLeftAlone() {
    record(transaction, execution(null, 0, view -> readsSlot(view).withPostBalance(Wei.of(7))));

    final Optional<TransactionProcessingResult> result = withBlockAccessList(1, accessList());

    assertThat(result).isPresent();
    final PartialBlockAccessView view = result.get().getPartialBlockAccessView().orElseThrow();
    assertThat(view.getTxIndex()).isEqualTo(2L);
    assertThat(view.accountChanges())
        .extracting(PartialBlockAccessView.AccountChanges::getAddress)
        .containsExactlyInAnyOrder(SENDER, CONTRACT, COINBASE);
    // the fee paid to the prewarm coinbase goes to the block's coinbase
    assertThat(account(view, COINBASE).getPostBalance()).contains(Wei.of(107));
    assertThat(prewarmed.reusedIn(block.getHash())).isTrue();
  }

  @Test
  void executesATransactionWhoseSlotTheBlockChangedBefore() {
    record(transaction, execution(null, 0, this::readsSlot));
    final BlockAccessList changedAtIndexOne = accessList(slotChange(1L, UInt256.valueOf(9)));

    assertThat(withBlockAccessList(1, changedAtIndexOne)).isEmpty();
    // a change by the transaction itself, at its own index, comes after what it read
    assertThat(withBlockAccessList(0, changedAtIndexOne)).isPresent();
  }

  @Test
  void takesAResultWhenTheSendersEarlierExecutionWroteWhatTheBlockHas() {
    final Execution earlier =
        execution(
            null,
            0,
            view ->
                view.getOrCreateAccountBuilder(CONTRACT)
                    .addStorageChange(SLOT, UInt256.ZERO, UInt256.valueOf(5)));
    record(transaction, execution(earlier, 0, this::readsSlot));

    // the block ran the sender's earlier transaction at index 1
    assertThat(
            withBlockAccessList(1, accessList(senderNonce(1L), slotChange(1L, UInt256.valueOf(5)))))
        .isPresent();
    assertThat(
            withBlockAccessList(1, accessList(senderNonce(1L), slotChange(1L, UInt256.valueOf(6)))))
        .isEmpty();
    // without the earlier transaction, the slot is still the parent's
    assertThat(withBlockAccessList(1, accessList(senderNonce(1L)))).isEmpty();
  }

  @Test
  void movesOnlyTheTransactionsOwnFeeToTheCoinbase() {
    final Execution earlier =
        execution(
            null,
            0,
            view -> view.getOrCreateAccountBuilder(PREWARM_COINBASE).withPostBalance(Wei.of(10)));
    record(
        transaction,
        execution(
            earlier,
            0,
            view -> view.getOrCreateAccountBuilder(PREWARM_COINBASE).withPostBalance(Wei.of(25))));

    final PartialBlockAccessView view =
        withBlockAccessList(1, accessList(senderNonce(1L)))
            .orElseThrow()
            .getPartialBlockAccessView()
            .orElseThrow();

    assertThat(account(view, COINBASE).getPostBalance()).contains(Wei.of(115));
  }

  @Test
  void executesATransactionThatTouchedTheCoinbase() {
    record(transaction, execution(null, 0, view -> view.getOrCreateAccountBuilder(COINBASE)));

    assertThat(withBlockAccessList(0, accessList())).isEmpty();
  }

  @Test
  void executesATransactionWhoseCodeReadsBlockValuesThatDiffer() {
    record(transaction, execution(null, READS_COINBASE, this::readsSlot));
    assertThat(withBlockAccessList(0, accessList())).isEmpty();

    record(transaction, execution(null, READS_PREVRANDAO, this::readsSlot));
    assertThat(withBlockAccessList(0, accessList())).isEmpty();

    record(
        transaction,
        new Execution(
            HEAD, TIMESTAMP, GAS_LIMIT + 1, READS_GAS_LIMIT, null, result(this::readsSlot)));
    assertThat(withBlockAccessList(0, accessList())).isEmpty();

    record(
        transaction,
        new Execution(HEAD, TIMESTAMP + 12, GAS_LIMIT, 0, null, result(this::readsSlot)));
    assertThat(withBlockAccessList(0, accessList())).isEmpty();

    // the timestamp and gas limit the execution assumed are the block's
    record(transaction, execution(null, READS_TIMESTAMP | READS_GAS_LIMIT, this::readsSlot));
    assertThat(withBlockAccessList(0, accessList())).isPresent();
  }

  @Test
  void executesWhenTheBlockTouchesThePrewarmCoinbase() {
    record(transaction, execution(null, 0, this::readsSlot));
    final BlockAccessList touchesPrewarmCoinbase =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    PREWARM_COINBASE,
                    List.of(),
                    List.of(),
                    List.of(new BlockAccessList.BalanceChange(1L, Wei.ONE)),
                    List.of(),
                    List.of())),
            Optional.empty());

    assertThat(withBlockAccessList(1, touchesPrewarmCoinbase)).isEmpty();
  }

  @Test
  void keepsTheExecutionsOfTheCurrentHeadOnly() {
    record(transaction, execution(null, 0, this::readsSlot));
    assertThat(prewarmed.forBlock(block)).isPresent();

    final BlockHeader otherParent =
        new BlockHeaderTestFixture().parentHash(Hash.hash(Bytes.of(2))).buildHeader();
    assertThat(prewarmed.forBlock(otherParent)).isEmpty();

    prewarmed.record(
        transaction(2).getHash(),
        new Execution(Hash.hash(Bytes.of(2)), TIMESTAMP, GAS_LIMIT, 0, null, result(v -> {})));
    assertThat(prewarmed.forBlock(block)).isEmpty();
  }

  @Test
  void processesABlockAgainWithoutResults() {
    record(transaction, execution(null, 0, this::readsSlot));
    assertThat(withBlockAccessList(0, accessList())).isPresent();
    assertThat(prewarmed.reusedIn(block.getHash())).isTrue();

    prewarmed.processWithoutReuse(block.getHash());

    assertThat(prewarmed.reusedIn(block.getHash())).isFalse();
    assertThat(prewarmed.forBlock(block)).isEmpty();
  }

  @Test
  void disabledKeepsNothing() {
    PrewarmedTransactions.DISABLED.record(
        transaction.getHash(), execution(null, 0, this::readsSlot));

    assertThat(PrewarmedTransactions.DISABLED.forBlock(block)).isEmpty();
  }

  @Test
  void findsBlockValueReadsOutsidePushData() {
    // COINBASE, PUSH1 0x42, PREVRANDAO
    assertThat(PrewarmedTransactions.blockValueReads(Bytes.fromHexString("0x41604244")))
        .isEqualTo(READS_COINBASE | READS_PREVRANDAO);
    // PUSH32 whose data holds every block value operation, then GASLIMIT
    assertThat(
            PrewarmedTransactions.blockValueReads(
                Bytes.concatenate(
                    Bytes.of(0x7f),
                    Bytes.fromHexString("0x41424445"),
                    Bytes.wrap(new byte[28]),
                    Bytes.of(0x45))))
        .isEqualTo(READS_GAS_LIMIT);
    assertThat(PrewarmedTransactions.blockValueReads(Bytes.fromHexString("0x42")))
        .isEqualTo(READS_TIMESTAMP);
    assertThat(PrewarmedTransactions.blockValueReads(Bytes.fromHexString("0x6041"))).isZero();
  }

  private Optional<TransactionProcessingResult> withBlockAccessList(
      final int location, final BlockAccessList blockAccessList) {
    return prewarmed
        .forBlock(block)
        .orElseThrow()
        .withBlockAccessList(
            transaction,
            location,
            BlockAccessListAccountLookup.of(blockAccessList),
            Wei.of(100),
            Wei.ZERO);
  }

  private void record(final Transaction transaction, final Execution execution) {
    prewarmed.record(transaction.getHash(), execution);
  }

  private Execution execution(
      final Execution previous,
      final int blockValueReads,
      final Consumer<PartialBlockAccessView.PartialBlockAccessViewBuilder> changes) {
    return new Execution(HEAD, TIMESTAMP, GAS_LIMIT, blockValueReads, previous, result(changes));
  }

  private static TransactionProcessingResult result(
      final Consumer<PartialBlockAccessView.PartialBlockAccessViewBuilder> changes) {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder view =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(1L);
    view.getOrCreateAccountBuilder(SENDER).withNonceChange(1L);
    changes.accept(view);
    return new TransactionProcessingResult(
        TransactionProcessingResult.Status.SUCCESSFUL,
        List.of(),
        21_000L,
        0L,
        21_000L,
        0L,
        Bytes.EMPTY,
        ValidationResult.<TransactionInvalidReason>valid(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(view.build()));
  }

  private PartialBlockAccessView.AccountChangesBuilder readsSlot(
      final PartialBlockAccessView.PartialBlockAccessViewBuilder view) {
    view.getOrCreateAccountBuilder(PREWARM_COINBASE).withPostBalance(Wei.of(7));
    return view.getOrCreateAccountBuilder(CONTRACT).addStorageRead(SLOT);
  }

  private static BlockAccessList.AccountChanges slotChange(
      final long txIndex, final UInt256 value) {
    return new BlockAccessList.AccountChanges(
        CONTRACT,
        List.of(
            new BlockAccessList.SlotChanges(
                SLOT, List.of(new BlockAccessList.StorageChange(txIndex, value)))),
        List.of(),
        List.of(),
        List.of(),
        List.of());
  }

  private static BlockAccessList.AccountChanges senderNonce(final long txIndex) {
    return new BlockAccessList.AccountChanges(
        SENDER,
        List.of(),
        List.of(),
        List.of(),
        List.of(new BlockAccessList.NonceChange(txIndex, 1L)),
        List.of());
  }

  private static BlockAccessList accessList(final BlockAccessList.AccountChanges... changes) {
    return new BlockAccessList(List.of(changes), Optional.empty());
  }

  private static PartialBlockAccessView.AccountChanges account(
      final PartialBlockAccessView view, final Address address) {
    return view.accountChanges().stream()
        .filter(changes -> changes.getAddress().equals(address))
        .findFirst()
        .orElseThrow();
  }

  private static Transaction transaction(final int seed) {
    final Transaction transaction = mock(Transaction.class);
    when(transaction.getHash()).thenReturn(Hash.hash(Bytes.of(seed, 0x7a)));
    return transaction;
  }
}
