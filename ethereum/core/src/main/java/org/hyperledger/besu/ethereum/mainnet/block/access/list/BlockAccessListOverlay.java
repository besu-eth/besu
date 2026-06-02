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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.evm.account.MutableAccount;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Resolves prior-block state from a {@link BlockAccessList} as of the end of transaction {@code
 * maxTxIndexExclusive - 1} (changes with {@code txIndex < maxTxIndexExclusive}).
 *
 * <p>Uses a shared {@link BlockAccessListAddressView} and looks up only the requested address or
 * storage slot — no full-BAL materialization and no database access.
 */
public final class BlockAccessListOverlay {

  private final BlockAccessListAddressView blockAccessListAddressView;
  private final long maxTxIndexExclusive;

  public BlockAccessListOverlay(
      final BlockAccessListAddressView blockAccessListAddressView, final long maxTxIndexExclusive) {
    this.blockAccessListAddressView = blockAccessListAddressView;
    this.maxTxIndexExclusive = maxTxIndexExclusive;
  }

  public boolean hasPriorAccountState(final Address address) {
    return blockAccessListAddressView
        .getAccountChanges(address)
        .map(this::hasResolvedAccountField)
        .orElse(false);
  }

  public boolean hasPriorStorageChange(final Address address, final StorageSlotKey storageSlotKey) {
    return hasPriorStorageChangeInternal(address, storageSlotKey);
  }

  public void applyToAccount(final Address address, final MutableAccount account) {
    blockAccessListAddressView
        .getAccountChanges(address)
        .ifPresent(
            accountChanges -> {
              findLatestBeforeMax(
                      accountChanges.balanceChanges(),
                      maxTxIndexExclusive,
                      BlockAccessList.BalanceChange::txIndex)
                  .ifPresent(change -> account.setBalance(change.postBalance()));
              findLatestBeforeMax(
                      accountChanges.nonceChanges(),
                      maxTxIndexExclusive,
                      BlockAccessList.NonceChange::txIndex)
                  .ifPresent(change -> account.setNonce(change.newNonce()));
            });
  }

  public void applyToCode(final Address address, final MutableAccount account) {
    applyToCode(address, account::setCode);
  }

  public void applyToCode(final Address address, final Consumer<Bytes> codeApplier) {
    blockAccessListAddressView
        .getAccountChanges(address)
        .flatMap(
            accountChanges ->
                findLatestBeforeMax(
                    accountChanges.codeChanges(),
                    maxTxIndexExclusive,
                    BlockAccessList.CodeChange::txIndex))
        .ifPresent(change -> codeApplier.accept(change.newCode()));
  }

  public void applyToStorage(
      final Address address,
      final StorageSlotKey storageSlotKey,
      final Consumer<UInt256> valueApplier) {
    blockAccessListAddressView
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

  public UInt256 getStorageValue(
      final Address address, final StorageSlotKey storageSlotKey, final UInt256 parentValue) {
    final UInt256[] resolved = {parentValue};
    applyToStorage(address, storageSlotKey, balValue -> resolved[0] = balValue);
    return resolved[0];
  }

  private boolean hasResolvedAccountField(final BlockAccessList.AccountChanges accountChanges) {
    return findLatestBeforeMax(
                accountChanges.balanceChanges(),
                maxTxIndexExclusive,
                BlockAccessList.BalanceChange::txIndex)
            .isPresent()
        || findLatestBeforeMax(
                accountChanges.nonceChanges(),
                maxTxIndexExclusive,
                BlockAccessList.NonceChange::txIndex)
            .isPresent();
  }

  private boolean hasPriorStorageChangeInternal(
      final Address address, final StorageSlotKey storageSlotKey) {
    return blockAccessListAddressView
        .getSlotChanges(address, storageSlotKey)
        .flatMap(
            slotChanges ->
                findLatestBeforeMax(
                    slotChanges.changes(),
                    maxTxIndexExclusive,
                    BlockAccessList.StorageChange::txIndex))
        .isPresent();
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
