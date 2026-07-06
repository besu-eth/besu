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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.MutableAccount;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Resolves prior-block state from a {@link BlockAccessList} as of the end of transaction {@code
 * maxTxIndexExclusive - 1} (changes with {@code txIndex < maxTxIndexExclusive}).
 *
 * <p>Built per transaction from a shared {@link BlockAccessListAccountLookup}; lookups are address-
 * or slot-scoped with no database access.
 */
public final class BlockAccessListOverlay {

  private final BlockAccessListAccountLookup accountLookup;
  private final long maxTxIndexExclusive;

  public BlockAccessListOverlay(
      final BlockAccessListAccountLookup accountLookup, final long maxTxIndexExclusive) {
    this.accountLookup = accountLookup;
    this.maxTxIndexExclusive = maxTxIndexExclusive;
  }

  /**
   * Returns whether the BAL has balance, nonce, or code-hash changes for {@code address} before
   * {@code maxTxIndexExclusive}.
   */
  public boolean hasAccountHeaderChanges(final Address address) {
    return accountLookup
        .getAccountChanges(address)
        .map(this::hasAccountHeaderChanges)
        .orElse(false);
  }

  private boolean hasAccountHeaderChanges(final BlockAccessList.AccountChanges accountChanges) {
    return findLatestBeforeMax(
                accountChanges.balanceChanges(),
                maxTxIndexExclusive,
                BlockAccessList.BalanceChange::txIndex)
            .isPresent()
        || findLatestBeforeMax(
                accountChanges.nonceChanges(),
                maxTxIndexExclusive,
                BlockAccessList.NonceChange::txIndex)
            .isPresent()
        || findLatestBeforeMax(
                accountChanges.codeChanges(),
                maxTxIndexExclusive,
                BlockAccessList.CodeChange::txIndex)
            .isPresent();
  }

  public BlockAccessListAccountLookup getAccountLookup() {
    return accountLookup;
  }

  public long getMaxTxIndexExclusive() {
    return maxTxIndexExclusive;
  }

  public Optional<Wei> getBalance(final Address address) {
    return accountLookup
        .getAccountChanges(address)
        .flatMap(
            accountChanges ->
                findLatestBeforeMax(
                        accountChanges.balanceChanges(),
                        maxTxIndexExclusive,
                        BlockAccessList.BalanceChange::txIndex)
                    .map(BlockAccessList.BalanceChange::postBalance));
  }

  public Optional<Long> getNonce(final Address address) {
    return accountLookup
        .getAccountChanges(address)
        .flatMap(
            accountChanges ->
                findLatestBeforeMax(
                        accountChanges.nonceChanges(),
                        maxTxIndexExclusive,
                        BlockAccessList.NonceChange::txIndex)
                    .map(BlockAccessList.NonceChange::newNonce));
  }

  public Optional<Hash> getCodeHash(final Address address) {
    return accountLookup
        .getAccountChanges(address)
        .flatMap(
            accountChanges ->
                findLatestBeforeMax(
                        accountChanges.codeChanges(),
                        maxTxIndexExclusive,
                        BlockAccessList.CodeChange::txIndex)
                    .map(BlockAccessListOverlay::codeHashFromChange));
  }

  public Optional<Bytes> getCode(final Address address) {
    return accountLookup
        .getAccountChanges(address)
        .flatMap(
            accountChanges ->
                findLatestBeforeMax(
                        accountChanges.codeChanges(),
                        maxTxIndexExclusive,
                        BlockAccessList.CodeChange::txIndex)
                    .map(BlockAccessList.CodeChange::newCode));
  }

  /**
   * Applies balance, nonce and code hash from the BAL when prior changes exist. The {@code
   * accountSupplier} provides the account to mutate (existing copy or newly created).
   */
  public <A extends MutableAccount> Optional<A> applyToAccountState(
      final Address address, final Supplier<A> accountSupplier) {
    if (!hasAccountHeaderChanges(address)) {
      return Optional.empty();
    }

    final Optional<Wei> balance = getBalance(address);
    final Optional<Long> nonce = getNonce(address);
    final Optional<Bytes> code = getCode(address);

    final A account = accountSupplier.get();
    balance.ifPresent(account::setBalance);
    nonce.ifPresent(account::setNonce);
    code.ifPresent(account::setCode);
    return Optional.of(account);
  }

  public void applyToCode(final Address address, final Consumer<Bytes> codeApplier) {
    accountLookup
        .getAccountChanges(address)
        .flatMap(
            accountChanges ->
                findLatestBeforeMax(
                    accountChanges.codeChanges(),
                    maxTxIndexExclusive,
                    BlockAccessList.CodeChange::txIndex))
        .ifPresent(change -> codeApplier.accept(change.newCode()));
  }

  private static Hash codeHashFromChange(final BlockAccessList.CodeChange change) {
    final Bytes code = change.newCode();
    return code == null || code.isEmpty() ? Hash.EMPTY : Hash.hash(code);
  }

  public void applyToStorage(
      final Address address,
      final StorageSlotKey storageSlotKey,
      final Consumer<UInt256> valueApplier) {
    accountLookup
        .getSlotChanges(address, storageSlotKey)
        .flatMap(
            slotChanges ->
                findLatestBeforeMax(
                    slotChanges.changes(),
                    maxTxIndexExclusive,
                    BlockAccessList.StorageChange::txIndex))
        .ifPresent(
            change ->
                valueApplier.accept(change.newValue() != null ? change.newValue() : UInt256.ZERO));
  }

  /**
   * Returns the latest change with {@code txIndex < maxIndex}. Change lists are sorted by {@code
   * txIndex} ascending.
   */
  private static <T> Optional<T> findLatestBeforeMax(
      final List<T> changes, final long maxIndex, final ToLongFunction<T> txIndexGetter) {
    if (changes.isEmpty()) {
      return Optional.empty();
    }
    int lo = 0;
    int hi = changes.size() - 1;
    int latestIndex = -1;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      if (txIndexGetter.applyAsLong(changes.get(mid)) < maxIndex) {
        latestIndex = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return latestIndex < 0 ? Optional.empty() : Optional.of(changes.get(latestIndex));
  }
}
