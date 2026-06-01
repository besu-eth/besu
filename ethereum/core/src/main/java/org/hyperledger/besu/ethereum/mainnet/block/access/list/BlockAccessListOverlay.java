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
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.MutableAccount;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToLongFunction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Read-only overlay of {@link BlockAccessList} state as of the end of transaction {@code
 * maxTxIndexExclusive - 1} (i.e. all changes with {@code txIndex < maxTxIndexExclusive}).
 *
 * <p>Account and storage values are resolved once at construction so hot-path reads are map lookups
 * without re-scanning change lists.
 */
public final class BlockAccessListOverlay {

  private final long maxTxIndexExclusive;
  private final Map<Address, AccountOverlay> accountOverlays;

  public BlockAccessListOverlay(
      final BlockAccessList blockAccessList, final long maxTxIndexExclusive) {
    this.maxTxIndexExclusive = maxTxIndexExclusive;
    this.accountOverlays = buildAccountOverlays(blockAccessList, maxTxIndexExclusive);
  }

  public long maxTxIndexExclusive() {
    return maxTxIndexExclusive;
  }

  public boolean hasPriorAccountState(final Address address) {
    final AccountOverlay overlay = accountOverlays.get(address);
    return overlay != null && overlay.hasPriorAccountState();
  }

  public boolean hasPriorStorageChange(final Address address, final StorageSlotKey storageSlotKey) {
    final AccountOverlay overlay = accountOverlays.get(address);
    return overlay != null && overlay.hasStorageOverlay(storageSlotKey);
  }

  public void applyToAccount(final Address address, final MutableAccount account) {
    final AccountOverlay overlay = accountOverlays.get(address);
    if (overlay != null) {
      overlay.applyTo(account);
    }
  }

  public Optional<Bytes> getCode(final Address address) {
    final AccountOverlay overlay = accountOverlays.get(address);
    return overlay == null ? Optional.empty() : overlay.code();
  }

  public UInt256 getStorageValue(
      final Address address, final StorageSlotKey storageSlotKey, final UInt256 parentValue) {
    final AccountOverlay overlay = accountOverlays.get(address);
    if (overlay == null) {
      return parentValue;
    }
    return overlay.storageValue(storageSlotKey).orElse(parentValue);
  }

  private static Map<Address, AccountOverlay> buildAccountOverlays(
      final BlockAccessList blockAccessList, final long maxTxIndexExclusive) {
    final Map<Address, AccountOverlay> overlays = new HashMap<>();
    for (final BlockAccessList.AccountChanges accountChanges : blockAccessList.accountChanges()) {
      final Optional<Wei> balance =
          findLatestBeforeMax(
                  accountChanges.balanceChanges(),
                  maxTxIndexExclusive,
                  BlockAccessList.BalanceChange::txIndex)
              .map(BlockAccessList.BalanceChange::postBalance);
      final Optional<Long> nonce =
          findLatestBeforeMax(
                  accountChanges.nonceChanges(),
                  maxTxIndexExclusive,
                  BlockAccessList.NonceChange::txIndex)
              .map(BlockAccessList.NonceChange::newNonce);
      final Optional<Bytes> code =
          findLatestBeforeMax(
                  accountChanges.codeChanges(),
                  maxTxIndexExclusive,
                  BlockAccessList.CodeChange::txIndex)
              .map(BlockAccessList.CodeChange::newCode);
      final Map<StorageSlotKey, UInt256> storage =
          buildStorageOverlay(accountChanges.storageChanges(), maxTxIndexExclusive);
      if (balance.isPresent() || nonce.isPresent() || code.isPresent() || !storage.isEmpty()) {
        overlays.put(
            accountChanges.address(),
            new AccountOverlay(balance, nonce, code, Map.copyOf(storage)));
      }
    }
    return Map.copyOf(overlays);
  }

  private static Map<StorageSlotKey, UInt256> buildStorageOverlay(
      final List<BlockAccessList.SlotChanges> storageChanges, final long maxTxIndexExclusive) {
    if (storageChanges.isEmpty()) {
      return Map.of();
    }
    final Map<StorageSlotKey, UInt256> storage = new HashMap<>();
    for (final BlockAccessList.SlotChanges slotChanges : storageChanges) {
      findLatestBeforeMax(
              slotChanges.changes(), maxTxIndexExclusive, BlockAccessList.StorageChange::txIndex)
          .ifPresent(
              latest -> {
                final UInt256 value = latest.newValue() != null ? latest.newValue() : UInt256.ZERO;
                storage.put(slotChanges.slot(), value);
              });
    }
    return storage;
  }

  /**
   * Returns the latest change with {@code txIndex < maxIndex}. Lists are sorted by {@code txIndex}
   * ascending, so we scan from the end.
   */
  private static <T> Optional<T> findLatestBeforeMax(
      final List<T> changes, final long maxIndex, final ToLongFunction<T> txIndexGetter) {
    for (int i = changes.size() - 1; i >= 0; i--) {
      final T change = changes.get(i);
      if (txIndexGetter.applyAsLong(change) < maxIndex) {
        return Optional.of(change);
      }
    }
    return Optional.empty();
  }

  private record AccountOverlay(
      Optional<Wei> balance,
      Optional<Long> nonce,
      Optional<Bytes> code,
      Map<StorageSlotKey, UInt256> storageBySlot) {

    boolean hasPriorAccountState() {
      return balance.isPresent() || nonce.isPresent() || code.isPresent();
    }

    boolean hasStorageOverlay(final StorageSlotKey storageSlotKey) {
      return storageBySlot.containsKey(storageSlotKey);
    }

    Optional<UInt256> storageValue(final StorageSlotKey storageSlotKey) {
      return Optional.ofNullable(storageBySlot.get(storageSlotKey));
    }

    void applyTo(final MutableAccount account) {
      balance.ifPresent(account::setBalance);
      nonce.ifPresent(account::setNonce);
      code.ifPresent(account::setCode);
    }
  }
}
