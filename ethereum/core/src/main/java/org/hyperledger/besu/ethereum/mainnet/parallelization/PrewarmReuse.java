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

import static org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.PREWARM_COINBASE;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.READS_COINBASE;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.READS_GAS_LIMIT;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.READS_PREVRANDAO;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions.Execution;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.account.Account;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;

/**
 * Takes, for one block, the prewarm result of a transaction instead of executing it, when the
 * execution read the values the block has before the transaction.
 *
 * <p>A prewarm execution ran on the state of the block's parent after the sender's earlier pool
 * transactions. Its result stands for the block's execution when every account field and storage
 * slot it touched is, before the transaction in the block, as the execution saw it: changed neither
 * by the block nor by the sender's earlier prewarm executions, or changed by both to the same
 * value. The execution must also not have read a block value that differs: the coinbase and
 * prevRandao are not known before the block, and the timestamp and gas limit must be the block's.
 *
 * <p>The block's validation still checks the result: a block whose access list or state root does
 * not match after taking results is processed again without them.
 */
public final class PrewarmReuse {

  private final PrewarmedTransactions transactions;
  private final BlockHeader block;

  PrewarmReuse(final PrewarmedTransactions transactions, final BlockHeader block) {
    this.transactions = transactions;
    this.block = block;
  }

  /**
   * The prewarm result of a transaction of a block with an access list, rewritten for its position
   * and the block's coinbase, if the access list says the execution read the block's values.
   *
   * @param transaction the transaction
   * @param location its position in the block
   * @param accountLookup the block's access list
   * @param coinbaseBalance the coinbase's balance before the block
   * @param prewarmCoinbaseBalance the prewarm coinbase's balance before the block
   * @return the result to take instead of executing the transaction
   */
  public Optional<TransactionProcessingResult> withBlockAccessList(
      final Transaction transaction,
      final int location,
      final BlockAccessListAccountLookup accountLookup,
      final Wei coinbaseBalance,
      final Wei prewarmCoinbaseBalance) {
    if (accountLookup.getAccountChanges(PREWARM_COINBASE).isPresent()) {
      // the fees of the prewarm executions would not start from its balance
      return Optional.empty();
    }
    final long index = location + 1L;
    final BlockAccessListOverlay before = new BlockAccessListOverlay(accountLookup, index);
    return candidate(transaction)
        .filter(execution -> readsAsInBlock(execution, before))
        .flatMap(
            execution ->
                rewrite(
                    execution,
                    before.getBalance(block.getCoinbase()).orElse(coinbaseBalance),
                    prewarmCoinbaseBalance,
                    index))
        .map(this::reused);
  }

  /**
   * The prewarm result of a transaction of a block without an access list, if the block's state
   * before the transaction has the values the execution read; its changes are then applied to the
   * block's state.
   *
   * @param transaction the transaction
   * @param location its position in the block
   * @param accumulator the block's state, with the transactions before this one applied
   * @param clearEmptyAccounts whether empty accounts are deleted
   * @return the result to take instead of executing the transaction
   */
  public Optional<TransactionProcessingResult> withBlockState(
      final Transaction transaction,
      final int location,
      final PathBasedWorldStateUpdateAccumulator<?> accumulator,
      final boolean clearEmptyAccounts) {
    final Optional<TransactionProcessingResult> result =
        candidate(transaction)
            .filter(execution -> readsAsInState(execution, accumulator))
            .flatMap(
                execution ->
                    rewrite(
                        execution,
                        balance(accumulator.get(block.getCoinbase())),
                        prewarmCoinbaseBalance(accumulator),
                        location + 1L));
    result.ifPresent(
        reused -> {
          accumulator.importStateChangesFromPartialView(
              reused.getPartialBlockAccessView().orElseThrow(), clearEmptyAccounts);
          reused(reused);
        });
    return result;
  }

  /** What the prewarm executions saw: the parent's balance, whatever the block did to it. */
  private static Wei prewarmCoinbaseBalance(
      final PathBasedWorldStateUpdateAccumulator<?> accumulator) {
    final var entry = accumulator.getAccountsToUpdate().get(PREWARM_COINBASE);
    return entry != null ? balance(entry.getPrior()) : balance(accumulator.get(PREWARM_COINBASE));
  }

  private TransactionProcessingResult reused(final TransactionProcessingResult result) {
    transactions.reused(block);
    return result;
  }

  private Optional<Execution> candidate(final Transaction transaction) {
    return transactions
        .execution(transaction.getHash())
        .filter(execution -> execution.head().equals(block.getParentHash()))
        .filter(execution -> execution.timestamp() == block.getTimestamp())
        .filter(
            execution -> (execution.blockValueReads() & (READS_COINBASE | READS_PREVRANDAO)) == 0)
        .filter(
            execution ->
                (execution.blockValueReads() & READS_GAS_LIMIT) == 0
                    || execution.gasLimit() == block.getGasLimit())
        .filter(execution -> execution.result().getPartialBlockAccessView().isPresent());
  }

  private boolean readsAsInBlock(final Execution execution, final BlockAccessListOverlay before) {
    for (final PartialBlockAccessView.AccountChanges touched : view(execution).accountChanges()) {
      final Address address = touched.getAddress();
      if (address.equals(PREWARM_COINBASE)) {
        continue;
      }
      if (address.equals(block.getCoinbase())) {
        // the coinbase's balance changes with every transaction's fee
        return false;
      }
      final ChainAccount chain = ChainAccount.of(execution, address);
      final Hash blockCode = before.getCode(address).map(Hash::hash).orElse(null);
      if (!sameOrUnchanged(before.getBalance(address).orElse(null), chain.balance)
          || !sameOrUnchanged(before.getNonce(address).orElse(null), chain.nonce)
          || !sameOrUnchanged(blockCode, chain.codeHash)) {
        return false;
      }
      if (touched.getNewCode().isPresent() && changedBefore(before, address)) {
        // a creation also reads whether the account has storage
        return false;
      }
      for (final StorageSlotKey slot : touched.getStorageReads()) {
        if (!slotAsInBlock(execution, before, address, slot)) {
          return false;
        }
      }
      for (final PartialBlockAccessView.SlotChange write : touched.getStorageChanges()) {
        if (!slotAsInBlock(execution, before, address, write.slot())) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean slotAsInBlock(
      final Execution execution,
      final BlockAccessListOverlay before,
      final Address address,
      final StorageSlotKey slot) {
    final AtomicReference<UInt256> blockValue = new AtomicReference<>();
    before.applyToStorage(address, slot, blockValue::set);
    return sameOrUnchanged(blockValue.get(), chainSlot(execution, address, slot));
  }

  private static boolean changedBefore(final BlockAccessListOverlay before, final Address address) {
    if (before.getBalance(address).isPresent()
        || before.getNonce(address).isPresent()
        || before.getCode(address).isPresent()) {
      return true;
    }
    final long index = before.getMaxTxIndexExclusive();
    return before
        .getAccountLookup()
        .getAccountChanges(address)
        .map(
            changes ->
                changes.storageChanges().stream()
                    .flatMap(slot -> slot.changes().stream())
                    .anyMatch(change -> change.txIndex() < index))
        .orElse(false);
  }

  /**
   * Without the values the block had before, a location matches only if neither the block nor the
   * sender's earlier executions changed it, or both changed it to the same value.
   */
  private static boolean sameOrUnchanged(
      @Nullable final Object blockValue, @Nullable final Object chainValue) {
    return Objects.equals(blockValue, chainValue);
  }

  private boolean readsAsInState(
      final Execution execution, final PathBasedWorldStateUpdateAccumulator<?> accumulator) {
    for (final PartialBlockAccessView.AccountChanges touched : view(execution).accountChanges()) {
      final Address address = touched.getAddress();
      if (address.equals(PREWARM_COINBASE)) {
        continue;
      }
      if (address.equals(block.getCoinbase())
          || accumulator.getStorageToClear().contains(address)) {
        return false;
      }
      final ChainAccount chain = ChainAccount.of(execution, address);
      final var entry = accumulator.getAccountsToUpdate().get(address);
      // an account neither the block nor the sender's earlier executions changed is as the
      // execution saw it, without reading it
      if (entry != null || chain.changed()) {
        final Account current = entry != null ? entry.getUpdated() : accumulator.get(address);
        final Account parent = entry == null ? current : entry.getPrior();
        if (entry != null
            && (entry.getPrior() == null) != (entry.getUpdated() == null)
            && (!touched.getStorageReads().isEmpty() || !touched.getStorageChanges().isEmpty())) {
          // created or deleted in the block, so its storage is not the parent's
          return false;
        }
        if (!Objects.equals(
                chain.balance != null ? chain.balance : balance(parent), balance(current))
            || nonce(current) != (chain.nonce != null ? chain.nonce : nonce(parent))
            || !Objects.equals(
                chain.codeHash != null ? chain.codeHash : codeHash(parent), codeHash(current))) {
          return false;
        }
      }
      for (final StorageSlotKey slot : touched.getStorageReads()) {
        if (!slotAsInState(execution, accumulator, address, slot)) {
          return false;
        }
      }
      for (final PartialBlockAccessView.SlotChange write : touched.getStorageChanges()) {
        if (!slotAsInState(execution, accumulator, address, write.slot())) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean slotAsInState(
      final Execution execution,
      final PathBasedWorldStateUpdateAccumulator<?> accumulator,
      final Address address,
      final StorageSlotKey slot) {
    final var slots = accumulator.getStorageToUpdate().get(address);
    final var entry = slots == null ? null : slots.get(slot);
    final UInt256 chain = chainSlot(execution, address, slot);
    if (entry == null && chain == null) {
      // neither the block nor the sender's earlier executions wrote it
      return true;
    }
    final UInt256 current =
        entry != null
            ? orZero(entry.getUpdated())
            : accumulator.getStorageValueByStorageSlotKey(address, slot).orElse(UInt256.ZERO);
    return (chain != null ? chain : orZero(entry.getPrior())).equals(current);
  }

  /** The result with the block's index, and the fee paid to the block's coinbase. */
  private Optional<TransactionProcessingResult> rewrite(
      final Execution execution,
      final Wei coinbaseBalance,
      final Wei prewarmCoinbaseBalance,
      final long index) {
    final List<PartialBlockAccessView.AccountChanges> accounts = new ArrayList<>();
    for (final PartialBlockAccessView.AccountChanges touched : view(execution).accountChanges()) {
      if (!touched.getAddress().equals(PREWARM_COINBASE)) {
        accounts.add(copy(touched));
        continue;
      }
      if (touched.getNonceChange().isPresent()
          || touched.getNewCode().isPresent()
          || !touched.getStorageReads().isEmpty()
          || !touched.getStorageChanges().isEmpty()) {
        return Optional.empty();
      }
      // the prewarm coinbase also holds the fees of the sender's earlier executions
      final Wei chainBalance = ChainAccount.of(execution, PREWARM_COINBASE).balance;
      final Wei feesBefore = chainBalance != null ? chainBalance : prewarmCoinbaseBalance;
      accounts.add(
          new PartialBlockAccessView.AccountChanges(
              block.getCoinbase(),
              touched
                  .getPostBalance()
                  .map(after -> coinbaseBalance.add(after.subtract(feesBefore))),
              Optional.empty(),
              Optional.empty(),
              List.of(),
              List.of()));
    }
    accounts.sort(
        (left, right) ->
            Arrays.compareUnsigned(
                left.getAddress().getBytes().toArrayUnsafe(),
                right.getAddress().getBytes().toArrayUnsafe()));
    final TransactionProcessingResult result = execution.result();
    final TransactionProcessingResult rewritten =
        new TransactionProcessingResult(
            result.getStatus(),
            result.getLogs(),
            result.getEstimateGasUsedByTransaction(),
            result.getGasRemaining(),
            result.getGasSpent(),
            result.getStateGasUsed(),
            result.getOutput(),
            result.getValidationResult(),
            result.getRevertReason(),
            result.getExceptionalHaltReason(),
            Optional.of(new PartialBlockAccessView(accounts, index, false)));
    rewritten.setExecutionGasUsedForBlock(result.getExecutionGasUsedForBlock());
    return Optional.of(rewritten);
  }

  private static PartialBlockAccessView.AccountChanges copy(
      final PartialBlockAccessView.AccountChanges changes) {
    return new PartialBlockAccessView.AccountChanges(
        changes.getAddress(),
        changes.getPostBalance(),
        changes.getNonceChange(),
        changes.getNewCode(),
        changes.getStorageReads(),
        changes.getStorageChanges());
  }

  private static PartialBlockAccessView view(final Execution execution) {
    return execution.result().getPartialBlockAccessView().orElseThrow();
  }

  /** The latest value the sender's earlier executions wrote to a slot, if any. */
  @Nullable
  private static UInt256 chainSlot(
      final Execution execution, final Address address, final StorageSlotKey slot) {
    for (Execution earlier = execution.previous(); earlier != null; earlier = earlier.previous()) {
      for (final PartialBlockAccessView.AccountChanges seen : view(earlier).accountChanges()) {
        if (seen.getAddress().equals(address)) {
          for (final PartialBlockAccessView.SlotChange change : seen.getStorageChanges()) {
            if (change.slot().equals(slot)) {
              return orZero(change.newValue());
            }
          }
        }
      }
    }
    return null;
  }

  /** The latest account values the sender's earlier executions wrote, field by field. */
  private record ChainAccount(
      @Nullable Wei balance, @Nullable Long nonce, @Nullable Hash codeHash) {

    static ChainAccount of(final Execution execution, final Address address) {
      Wei balance = null;
      Long nonce = null;
      Hash codeHash = null;
      for (Execution earlier = execution.previous();
          earlier != null && (balance == null || nonce == null || codeHash == null);
          earlier = earlier.previous()) {
        for (final PartialBlockAccessView.AccountChanges seen : view(earlier).accountChanges()) {
          if (seen.getAddress().equals(address)) {
            if (balance == null && seen.getPostBalance().isPresent()) {
              balance = seen.getPostBalance().get();
            }
            if (nonce == null && seen.getNonceChange().isPresent()) {
              nonce = seen.getNonceChange().get();
            }
            if (codeHash == null && seen.getNewCode().isPresent()) {
              codeHash = Hash.hash(seen.getNewCode().get());
            }
          }
        }
      }
      return new ChainAccount(balance, nonce, codeHash);
    }

    boolean changed() {
      return balance != null || nonce != null || codeHash != null;
    }
  }

  private static UInt256 orZero(@Nullable final UInt256 value) {
    return value == null ? UInt256.ZERO : value;
  }

  private static Wei balance(@Nullable final Account account) {
    return account == null ? Wei.ZERO : account.getBalance();
  }

  private static long nonce(@Nullable final Account account) {
    return account == null ? 0L : account.getNonce();
  }

  private static Hash codeHash(@Nullable final Account account) {
    return account == null ? Hash.EMPTY : account.getCodeHash();
  }
}
