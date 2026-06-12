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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiBalWorldStateUpdater;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.CodeCache;

import java.util.List;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;

/**
 * A lazy-loading mutable account wrapper that resolves field values BAL-first.
 *
 * <p>Reading nonce, balance, code, or storage for an account modified in a prior transaction within
 * the same block costs O(1) BAL lookup with zero DB access. The parent account is loaded from the
 * database at most once, only when a field not present in the BAL is first accessed.
 *
 * <p>This class is constructed by {@link BonsaiBalWorldStateUpdater#getForMutation(Address)} and is
 * immediately wrapped by {@link org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount}, which
 * snapshots nonce, balance, and code at construction time. Therefore mutations applied by the EVM
 * (via {@code setNonce}, {@code setBalance}, etc.) are tracked by the wrapper, not here.
 */
public class BonsaiBalAccount extends BonsaiAccount {

  // True when the corresponding parent field (this.nonce / this.balance / this.code) has been
  // set — either from the BAL in the constructor or by a subsequent setter call. False means the
  // field is still the sentinel value and a DB fallback via parent.get() is required.
  private boolean hasNonce;
  private boolean hasBalance;
  private boolean hasCode;

  /** Lazily loaded from the DB at most once. {@code null} result is also cached. */
  private final Supplier<BonsaiAccount> parent;

  /**
   * Creates a new BAL overlay account. BAL field values are applied to the parent's fields via
   * setters in the constructor (O(1) index lookup); DB fallback for any field absent from the BAL
   * is deferred until first read.
   *
   * @param world the owning updater (used as the {@code PathBasedWorldView} context for storage
   *     delegation)
   * @param address the account address
   * @param balIndex O(1) index over the {@link BlockAccessList}
   * @param txIndex 1-based position of the current transaction; only BAL entries with {@code
   *     txIndex < this.txIndex} are visible
   * @param worldView the parent world view used for DB fallback reads
   * @param codeCache shared code cache
   */
  public BonsaiBalAccount(
      final BonsaiBalWorldStateUpdater world,
      final Address address,
      final BlockAccessListIndex balIndex,
      final long txIndex,
      final PathBasedWorldView worldView,
      final CodeCache codeCache) {
    super(
        world,
        address,
        address.addressHash(),
        0L,
        Wei.ZERO,
        Hash.EMPTY_TRIE_HASH,
        Hash.EMPTY,
        true,
        codeCache);

    final BlockAccessList.AccountChanges changes = balIndex.findAccount(address);

    final BlockAccessList.NonceChange nc =
        latestBefore(changes != null ? changes.nonceChanges() : null,
            BlockAccessList.NonceChange::txIndex, txIndex);
    if (nc != null) { setNonce(nc.newNonce()); }

    final BlockAccessList.BalanceChange bc =
        latestBefore(changes != null ? changes.balanceChanges() : null,
            BlockAccessList.BalanceChange::txIndex, txIndex);
    if (bc != null) { setBalance(bc.postBalance()); }

    final BlockAccessList.CodeChange cc =
        latestBefore(changes != null ? changes.codeChanges() : null,
            BlockAccessList.CodeChange::txIndex, txIndex);
    // setCode handles null/empty (clears code), hash computation, and codeCache
    if (cc != null) { setCode(cc.newCode()); }

    this.parent =
        Suppliers.memoize(
            () -> {
              final Account a = worldView.get(address);
              return a instanceof BonsaiAccount b ? b : null;
            });
  }

  @Override
  public void setNonce(final long value) {
    super.setNonce(value);
    hasNonce = true;
  }

  @Override
  public void setBalance(final Wei value) {
    super.setBalance(value);
    hasBalance = true;
  }

  @Override
  public void setCode(final Bytes code) {
    super.setCode(code);
    hasCode = true;
  }

  @Override
  public long getNonce() {
    if (hasNonce) return super.getNonce();
    final BonsaiAccount p = parent.get();
    return p != null ? p.getNonce() : 0L;
  }

  @Override
  public Wei getBalance() {
    if (hasBalance) return super.getBalance();
    final BonsaiAccount p = parent.get();
    return p != null ? p.getBalance() : Wei.ZERO;
  }

  @Override
  public Hash getCodeHash() {
    if (hasCode) return super.getCodeHash();
    final BonsaiAccount p = parent.get();
    return p != null ? p.getCodeHash() : Hash.EMPTY;
  }

  @Override
  public Bytes getCode() {
    return getOrCreateCachedCode().getBytes();
  }

  @Override
  public Code getOrCreateCachedCode() {
    // When hasCode is true, setCode() set this.code correctly; super returns it directly.
    // When false, the sentinel Hash.EMPTY causes PathBasedAccount to set this.code =
    // Code.EMPTY_CODE, so we must bypass it and delegate to the DB parent account.
    if (hasCode) return super.getOrCreateCachedCode();
    final BonsaiAccount p = parent.get();
    return p != null ? p.getOrCreateCachedCode() : Code.EMPTY_CODE;
  }

  @Override
  public boolean isStorageEmpty() {
    final BonsaiAccount p = parent.get();
    return p == null || p.isStorageEmpty();
  }

  /**
   * Returns the last entry in {@code changes} whose tx index is strictly less than {@code
   * txIndex}, or {@code null} if none qualifies. The list must be in ascending order.
   */
  private static <T> T latestBefore(
      final List<T> changes, final ToLongFunction<T> txIndexOf, final long txIndex) {
    if (changes == null) return null;
    for (int i = changes.size() - 1; i >= 0; i--) {
      final T entry = changes.get(i);
      if (txIndexOf.applyAsLong(entry) < txIndex) return entry;
    }
    return null;
  }
}
