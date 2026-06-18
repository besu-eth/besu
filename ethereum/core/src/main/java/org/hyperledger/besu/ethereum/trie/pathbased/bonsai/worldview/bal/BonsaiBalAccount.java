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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.bal;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAddressView;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
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
 * <p>Reading nonce, balance, or code for an account modified in a prior transaction within the same
 * block costs O(1) BAL lookup with zero DB access. The parent account is loaded from the database
 * at most once, only when a field not present in the BAL is first accessed.
 *
 * <p>Constructed by {@link BonsaiBalWorldStateUpdateAccumulator#loadAccount} and wrapped by {@link
 * org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount}, which snapshots nonce, balance, and
 * code at construction time.
 */
public class BonsaiBalAccount extends BonsaiAccount {

  private boolean hasNonce;
  private boolean hasBalance;
  private boolean hasCode;

  private final Supplier<BonsaiAccount> parent;

  public BonsaiBalAccount(
      final BonsaiBalWorldStateUpdateAccumulator world,
      final Address address,
      final Supplier<Account> parentLoader,
      final BlockAccessListAddressView balView,
      final long maxTxIndexExclusive,
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

    final BlockAccessList.AccountChanges changes =
        balView.getAccountChanges(address).orElse(null);

    final BlockAccessList.NonceChange nonceChange =
        latestBefore(
            changes != null ? changes.nonceChanges() : null,
            BlockAccessList.NonceChange::txIndex,
            maxTxIndexExclusive);
    if (nonceChange != null) {
      setNonce(nonceChange.newNonce());
    }

    final BlockAccessList.BalanceChange balanceChange =
        latestBefore(
            changes != null ? changes.balanceChanges() : null,
            BlockAccessList.BalanceChange::txIndex,
            maxTxIndexExclusive);
    if (balanceChange != null) {
      setBalance(balanceChange.postBalance());
    }

    final BlockAccessList.CodeChange codeChange =
        latestBefore(
            changes != null ? changes.codeChanges() : null,
            BlockAccessList.CodeChange::txIndex,
            maxTxIndexExclusive);
    if (codeChange != null) {
      setCode(codeChange.newCode());
    }

    this.parent =
        Suppliers.memoize(
            () -> {
              final Account account = parentLoader.get();
              return account instanceof BonsaiAccount bonsaiAccount ? bonsaiAccount : null;
            });
  }

  public BonsaiBalAccount(final BonsaiBalAccount source) {
    super(source);
    this.hasNonce = source.hasNonce;
    this.hasBalance = source.hasBalance;
    this.hasCode = source.hasCode;
    this.parent = source.parent;
  }

  public BonsaiBalAccount(
      final BonsaiBalAccount source, final PathBasedWorldView context, final boolean mutable) {
    super(source, context, mutable);
    this.hasNonce = source.hasNonce;
    this.hasBalance = source.hasBalance;
    this.hasCode = source.hasCode;
    this.parent = source.parent;
  }

  public boolean exists() {
    if (hasNonce || hasBalance || hasCode) {
      return true;
    }
    return parent.get() != null;
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
    if (hasNonce) {
      return super.getNonce();
    }
    final BonsaiAccount parentAccount = parent.get();
    return parentAccount != null ? parentAccount.getNonce() : 0L;
  }

  @Override
  public Wei getBalance() {
    if (hasBalance) {
      return super.getBalance();
    }
    final BonsaiAccount parentAccount = parent.get();
    return parentAccount != null ? parentAccount.getBalance() : Wei.ZERO;
  }

  @Override
  public Hash getCodeHash() {
    if (hasCode) {
      return super.getCodeHash();
    }
    final BonsaiAccount parentAccount = parent.get();
    return parentAccount != null ? parentAccount.getCodeHash() : Hash.EMPTY;
  }

  @Override
  public Bytes getCode() {
    return getOrCreateCachedCode().getBytes();
  }

  @Override
  public Hash getStorageRoot() {
    final BonsaiAccount parentAccount = parent.get();
    return parentAccount != null ? parentAccount.getStorageRoot() : Hash.EMPTY_TRIE_HASH;
  }

  @Override
  public Code getOrCreateCachedCode() {
    if (hasCode) {
      return super.getOrCreateCachedCode();
    }
    final BonsaiAccount parentAccount = parent.get();
    return parentAccount != null ? parentAccount.getOrCreateCachedCode() : Code.EMPTY_CODE;
  }

  @Override
  public boolean isStorageEmpty() {
    final BonsaiAccount parentAccount = parent.get();
    return parentAccount == null || parentAccount.isStorageEmpty();
  }

  private static <T> T latestBefore(
      final List<T> changes, final ToLongFunction<T> txIndexOf, final long maxTxIndexExclusive) {
    if (changes == null || changes.isEmpty()) {
      return null;
    }
    for (int i = changes.size() - 1; i >= 0; i--) {
      final T entry = changes.get(i);
      if (txIndexOf.applyAsLong(entry) < maxTxIndexExclusive) {
        return entry;
      }
    }
    return null;
  }
}
