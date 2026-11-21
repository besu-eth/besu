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
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup.PreBlockCodeReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.evm.account.MutableAccount;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
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

  /**
   * Returns the code from the latest code change before the max index.
   *
   * @param address the account
   * @param preBlockCode reads pre-block code to resolve a hash-form (EIP-8298) change, see {@link
   *     BlockAccessListAccountLookup#resolveAdoptedCode}
   * @return the code, or empty if the account has no such change
   * @throws IllegalStateException if a hash-form change cannot be resolved
   */
  public Optional<Bytes> getCode(final Address address, final PreBlockCodeReader preBlockCode) {
    return accountLookup
        .getAccountChanges(address)
        .flatMap(
            accountChanges ->
                findLatestBeforeMax(
                        accountChanges.codeChanges(),
                        maxTxIndexExclusive,
                        BlockAccessList.CodeChange::txIndex)
                    .map(change -> codeOf(change, adoptedCodeFrom(preBlockCode))));
  }

  /**
   * Applies balance, nonce and code hash from the BAL when prior changes exist. The {@code
   * accountSupplier} provides the account to mutate (existing copy or newly created).
   *
   * <p>A Bonsai account takes the code hash alone, its code is read through {@link #applyToCode}.
   * Any other account gets the code bytes, so a hash-form (EIP-8298) change must resolve from the
   * BAL itself.
   */
  public <A extends MutableAccount> Optional<A> applyToAccountState(
      final Address address, final Supplier<A> accountSupplier) {
    return accountLookup
        .getAccountChanges(address)
        .flatMap(
            accountChanges -> {
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
              final Optional<BlockAccessList.CodeChange> codeChange =
                  findLatestBeforeMax(
                      accountChanges.codeChanges(),
                      maxTxIndexExclusive,
                      BlockAccessList.CodeChange::txIndex);

              if (balance.isEmpty() && nonce.isEmpty() && codeChange.isEmpty()) {
                return Optional.empty();
              }

              final A account = accountSupplier.get();
              balance.ifPresent(account::setBalance);
              nonce.ifPresent(account::setNonce);
              codeChange.ifPresent(change -> applyCodeChange(account, change));
              return Optional.of(account);
            });
  }

  private void applyCodeChange(
      final MutableAccount account, final BlockAccessList.CodeChange change) {
    if (account instanceof BonsaiAccount pathBasedAccount && change.isAdoptedCode()) {
      pathBasedAccount.setCodeHash(change.newCodeHash().orElseThrow());
      return;
    }
    final Bytes code = codeOf(change, accountLookup::resolveWrittenCode);
    if (account instanceof BonsaiAccount pathBasedAccount) {
      pathBasedAccount.setCodeHash(code.isEmpty() ? Hash.EMPTY : Hash.hash(code));
      return;
    }
    account.setCode(code);
  }

  /**
   * Passes the code from the latest code change before the max index to {@code codeApplier}, if
   * there is one.
   *
   * @param address the account
   * @param preBlockCode reads pre-block code to resolve a hash-form (EIP-8298) change, see {@link
   *     BlockAccessListAccountLookup#resolveAdoptedCode}
   * @param codeApplier receives the code
   * @throws IllegalStateException if a hash-form change cannot be resolved
   */
  public void applyToCode(
      final Address address,
      final PreBlockCodeReader preBlockCode,
      final Consumer<Bytes> codeApplier) {
    accountLookup
        .getAccountChanges(address)
        .flatMap(
            accountChanges ->
                findLatestBeforeMax(
                    accountChanges.codeChanges(),
                    maxTxIndexExclusive,
                    BlockAccessList.CodeChange::txIndex))
        .ifPresent(change -> codeApplier.accept(codeOf(change, adoptedCodeFrom(preBlockCode))));
  }

  /**
   * A hash-form (EIP-8298) code change that does not resolve throws, so a transaction that reads it
   * fails its parallel run and is executed again sequentially.
   */
  private Bytes codeOf(
      final BlockAccessList.CodeChange change, final Function<Hash, Optional<Bytes>> adoptedCode) {
    if (change.isAdoptedCode()) {
      final Hash codeHash = change.newCodeHash().orElseThrow();
      return adoptedCode
          .apply(codeHash)
          .orElseThrow(
              () ->
                  new IllegalStateException(
                      "Code "
                          + codeHash
                          + " adopted at index "
                          + change.txIndex()
                          + " is neither in the block access list nor in the pre-block state"));
    }
    return change.newCode() != null ? change.newCode() : Bytes.EMPTY;
  }

  private Function<Hash, Optional<Bytes>> adoptedCodeFrom(final PreBlockCodeReader preBlockCode) {
    return codeHash -> accountLookup.resolveAdoptedCode(codeHash, preBlockCode);
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
