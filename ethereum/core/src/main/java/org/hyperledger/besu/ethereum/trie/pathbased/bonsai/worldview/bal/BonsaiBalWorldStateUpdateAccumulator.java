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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedLazy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Bonsai accumulator for parallel BAL execution: account headers are loaded lazily via {@link
 * PathBasedLazy} and account trie prefetch is disabled.
 */
public class BonsaiBalWorldStateUpdateAccumulator extends BonsaiWorldStateUpdateAccumulator {

  private final BlockAccessListOverlay blockAccessListOverlay;

  public BonsaiBalWorldStateUpdateAccumulator(
      final PathBasedWorldView world,
      final EvmConfiguration evmConfiguration,
      final CodeCache codeCache,
      final BlockAccessListOverlay blockAccessListOverlay) {
    super(
        world,
        (address, value) -> {},
        (address, slot) -> {},
        evmConfiguration,
        codeCache);
    this.blockAccessListOverlay = blockAccessListOverlay;
  }

  public BlockAccessListOverlay getBlockAccessListOverlay() {
    return blockAccessListOverlay;
  }

  @Override
  public PathBasedWorldStateUpdateAccumulator<BonsaiAccount> copy() {
    final BonsaiBalWorldStateUpdateAccumulator copy =
        new BonsaiBalWorldStateUpdateAccumulator(
            wrappedWorldView(), getEvmConfiguration(), codeCache(), blockAccessListOverlay);
    copy.cloneFromUpdater(this);
    return copy;
  }

  @Override
  public BonsaiAccount loadAccount(
      final Address address,
      final Function<PathBasedValue<BonsaiAccount>, BonsaiAccount> accountFunction) {
    try {
      final PathBasedValue<BonsaiAccount> pathBasedValue = getAccountsToUpdate().get(address);
      if (pathBasedValue != null) {
        return accountFunction.apply(pathBasedValue);
      }

      final Optional<BonsaiAccount> fromParent =
          loadAccountFromParentAccumulator(address, accountFunction);
      if (fromParent.isPresent()) {
        return fromParent.get();
      }

      final PathBasedLazy<BonsaiAccount> value =
          new PathBasedLazy<>(
              () -> resolvePriorAccount(address),
              () -> resolveUpdatedAccount(address));
      getAccountsToUpdate().put(address, value);

      final BonsaiAccount account = accountFunction.apply(value);
      if (account instanceof BonsaiBalAccount balAccount && !balAccount.exists()) {
        getAccountsToUpdate().put(address, new PathBasedValue<>(null, null));
        return null;
      }
      return account;
    } catch (MerkleTrieException e) {
      throw new MerkleTrieException(
          e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
    }
  }

  @Override
  protected void onCodeValueLoaded(final Address address, final PathBasedValue<Bytes> codeValue) {
    blockAccessListOverlay.applyToCode(address, codeValue::setUpdated);
  }

  @Override
  protected void onStorageValueLoaded(
      final Address address,
      final StorageSlotKey storageSlotKey,
      final PathBasedValue<UInt256> storageValue) {
    blockAccessListOverlay.applyToStorage(address, storageSlotKey, storageValue::setUpdated);
  }

  @Override
  protected BonsaiAccount copyAccount(final BonsaiAccount account) {
    if (account instanceof BonsaiBalAccount balAccount) {
      return new BonsaiBalAccount(balAccount);
    }
    return super.copyAccount(account);
  }

  @Override
  protected BonsaiAccount copyAccount(
      final BonsaiAccount toCopy, final PathBasedWorldView context, final boolean mutable) {
    if (toCopy instanceof BonsaiBalAccount balAccount) {
      return new BonsaiBalAccount(balAccount, context, mutable);
    }
    return super.copyAccount(toCopy, context, mutable);
  }

  private BonsaiAccount resolvePriorAccount(final Address address) {
    final Account parent = wrappedWorldView().get(address);
    if (parent instanceof BonsaiAccount bonsaiAccount) {
      return copyAccount(bonsaiAccount, this, false);
    }
    return null;
  }

  private BonsaiAccount resolveUpdatedAccount(final Address address) {
    return new BonsaiBalAccount(
        this,
        address,
        () -> wrappedWorldView().get(address),
        blockAccessListOverlay.getAddressView(),
        blockAccessListOverlay.getMaxTxIndexExclusive(),
        codeCache());
  }
}
