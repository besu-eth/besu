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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldView;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Bonsai accumulator for parallel BAL execution: applies BAL overlays on first account, code, and
 * storage reads via {@link #onAccountValueLoaded}, {@link #onCodeValueLoaded}, and {@link
 * #onStorageValueLoaded}. Account trie prefetch is disabled.
 */
public class BonsaiBalWorldStateUpdateAccumulator extends BonsaiWorldStateUpdateAccumulator {

  private final BlockAccessListOverlay blockAccessListOverlay;

  public BonsaiBalWorldStateUpdateAccumulator(
      final BonsaiWorldView world,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache,
      final BlockAccessListOverlay blockAccessListOverlay) {
    super(world, (address, value) -> {}, (address, slot) -> {}, evmConfiguration, codeCache);
    this.blockAccessListOverlay = blockAccessListOverlay;
  }

  public BlockAccessListOverlay getBlockAccessListOverlay() {
    return blockAccessListOverlay;
  }

  @Override
  public BonsaiWorldStateUpdateAccumulator copy() {
    final BonsaiBalWorldStateUpdateAccumulator copy =
        new BonsaiBalWorldStateUpdateAccumulator(
            wrappedWorldView(), getEvmConfiguration(), codeCache(), blockAccessListOverlay);
    copy.cloneFromUpdater(this);
    return copy;
  }

  @Override
  protected void onAccountValueLoaded(
      final Address address, final BonsaiValue<BonsaiAccount> accountValue) {
    blockAccessListOverlay
        .applyToAccountState(
            address,
            () -> {
              final BonsaiAccount updated = accountValue.getUpdated();
              if (updated != null) {
                return updated;
              }
              final Hash addressHash =
                  blockAccessListOverlay
                      .getAccountLookup()
                      .getAddressHash(address)
                      .orElseGet(() -> hashAndSaveAccountPreImage(address));
              return createAccount(
                  this, address, addressHash, 0, Wei.ZERO, storageRootStrategy(), Hash.EMPTY, true);
            })
        .ifPresent(accountValue::setUpdated);
  }

  @Override
  protected void onCodeValueLoaded(final Address address, final BonsaiValue<Bytes> codeValue) {
    blockAccessListOverlay.applyToCode(address, codeValue::setUpdated);
  }

  @Override
  protected void onStorageValueLoaded(
      final Address address,
      final StorageSlotKey storageSlotKey,
      final BonsaiValue<UInt256> storageValue) {
    blockAccessListOverlay.applyToStorage(address, storageSlotKey, storageValue::setUpdated);
  }
}
