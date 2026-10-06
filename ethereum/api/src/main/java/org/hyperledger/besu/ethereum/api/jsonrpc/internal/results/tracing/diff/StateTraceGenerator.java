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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.diff;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.TracingUtils;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.tracing.TraceFrame;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * Generates state diffs (before/after account and storage changes) for Ethereum transactions.
 *
 * <p>This class is used in JSON-RPC tracing to show how accounts and storage values were modified
 * by a transaction. It produces a diff-only trace (changed values only).
 */
public class StateTraceGenerator {

  public StateTraceGenerator() {}

  /**
   * Generate a state diff (only differences, not the full pre-state) for a transaction trace.
   *
   * @param transactionTrace The transaction trace containing execution and world state data.
   * @return A stream of {@link StateDiffTrace} objects representing modified state.
   */
  public Stream<StateDiffTrace> generateStateDiff(final TransactionTrace transactionTrace) {
    final List<TraceFrame> traceFrames = transactionTrace.getTraceFrames();
    if (traceFrames.isEmpty()) {
      return Stream.empty();
    }

    final WorldUpdater transactionUpdater = getTransactionUpdater(traceFrames);
    final WorldUpdater parentUpdater = transactionUpdater.parentUpdater().get();

    final StateDiffTrace stateDiffResult = new StateDiffTrace();

    // In diff mode, include only accounts whose state changed.
    // Use the transaction updater to detect modifications and compare them to the parent state.
    // As we need only modified accounts, we can rely on the world updater's touched accounts.
    processDiffMode(transactionUpdater, parentUpdater, stateDiffResult);

    processDeletedAccounts(stateDiffResult, transactionUpdater, parentUpdater);
    return Stream.of(stateDiffResult);
  }

  private void processDiffMode(
      final WorldUpdater transactionUpdater,
      final WorldUpdater parentUpdater,
      final StateDiffTrace stateDiffResult) {
    transactionUpdater
        .getTouchedAccounts()
        .forEach(
            updatedAccount -> {
              final Account rootAccount = parentUpdater.get(updatedAccount.getAddress());
              // Compute the storage diff for this account.
              // We compare the post-transaction storage (from updatedAccount) against the
              // pre-transaction storage (from rootAccount).
              final Map<String, DiffNode> storageDiff = new TreeMap<>();
              ((MutableAccount) updatedAccount)
                  .getUpdatedStorage()
                  .forEach(
                      (key, newValue) -> {
                        if (rootAccount == null) {
                          // Case 1: The account did not exist before this transaction.
                          // In diff mode, include only non-zero storage writes.
                          if (!UInt256.ZERO.equals(newValue)) {
                            storageDiff.put(
                                key.toHexString(), new DiffNode(null, newValue.toHexString()));
                          }
                        } else {
                          // Case 2: The account existed pre-transaction.
                          // Include the slot only if the value actually changed.
                          if (!rootAccount.getStorageValue(key).equals(newValue)) {
                            final String originalValueHex =
                                rootAccount.getStorageValue(key).toHexString();
                            storageDiff.put(
                                key.toHexString(),
                                new DiffNode(originalValueHex, newValue.toHexString()));
                          }
                        }
                      });

              // Build the account-level diff (balance, code, codeHash, nonce, storage).
              final AccountDiff accountDiff =
                  createAccountDiff(rootAccount, updatedAccount, storageDiff);

              // Record this account only if something actually changed.
              if (accountDiff.hasDifference()) {
                stateDiffResult.put(
                    updatedAccount.getAddress().getBytes().toHexString(), accountDiff);
              }
            });
  }

  /**
   * Get the {@link WorldUpdater} at the transaction scope.
   *
   * <p>Traverses up the world updater chain to retrieve the correct layer.
   */
  private WorldUpdater getTransactionUpdater(final List<TraceFrame> traceFrames) {
    // Transaction updater sits two levels above the frame's updater
    return traceFrames.getFirst().getWorldUpdater().parentUpdater().get().parentUpdater().get();
  }

  /** Record accounts deleted during the transaction. */
  private void processDeletedAccounts(
      final StateDiffTrace stateDiff,
      final WorldUpdater transactionUpdater,
      final WorldUpdater previousUpdater) {

    transactionUpdater
        .getDeletedAccountAddresses()
        .forEach(
            accountAddress -> {
              final Account deletedAccount = previousUpdater.get(accountAddress);
              if (deletedAccount != null) {
                final AccountDiff accountDiff =
                    createAccountDiff(deletedAccount, null, Collections.emptyMap());
                stateDiff.put(accountAddress.getBytes().toHexString(), accountDiff);
              }
            });
  }

  private AccountDiff createAccountDiff(
      final Account from, final Account to, final Map<String, DiffNode> storageDiff) {
    return new AccountDiff(
        createDiffNode(from, to, StateTraceGenerator::balanceAsHex),
        createDiffNode(from, to, StateTraceGenerator::codeAsHex),
        createDiffNode(from, to, StateTraceGenerator::codeHashAsHex),
        createDiffNode(from, to, StateTraceGenerator::nonceAsHex),
        storageDiff);
  }

  /** Create a {@link DiffNode} by extracting a field from two account states. */
  private DiffNode createDiffNode(
      final Account from, final Account to, final Function<Account, String> func) {

    return new DiffNode(Optional.ofNullable(from).map(func), Optional.ofNullable(to).map(func));
  }

  private static String balanceAsHex(final Account account) {
    return TracingUtils.weiAsHex(account.getBalance());
  }

  private static String codeHashAsHex(final Account account) {
    return account.getCodeHash().getBytes().toHexString();
  }

  private static String codeAsHex(final Account account) {
    return account.getCode().toHexString();
  }

  private static String nonceAsHex(final Account account) {
    return "0x" + Long.toHexString(account.getNonce());
  }
}
