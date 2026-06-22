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
package org.hyperledger.besu.ethereum.referencetests;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiPreImageProxy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.Consumer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.units.bigints.UInt256;

public class BonsaiReferenceTestUpdateAccumulator extends BonsaiWorldStateUpdateAccumulator {
  private final BonsaiPreImageProxy preImageProxy;

  public BonsaiReferenceTestUpdateAccumulator(
      final PathBasedWorldView world,
      final Consumer<PathBasedValue<BonsaiAccount>> accountPreloader,
      final Consumer<StorageSlotKey> storagePreloader,
      final BonsaiPreImageProxy preImageProxy,
      final EvmConfiguration evmConfiguration) {
    // only used in tests no global code cache is needed
    super(world, accountPreloader, storagePreloader, evmConfiguration, new PathBasedCodeCache());
    this.preImageProxy = preImageProxy;
  }

  @Override
  protected Hash hashAndSaveAccountPreImage(final Address address) {
    return preImageProxy.hashAndSavePreImage(address.getBytes());
  }

  @Override
  protected Hash hashAndSaveSlotPreImage(final UInt256 slotKey) {
    return preImageProxy.hashAndSavePreImage(slotKey);
  }

  public BonsaiReferenceTestUpdateAccumulator createDetachedAccumulator() {
    final BonsaiReferenceTestUpdateAccumulator copy =
        new BonsaiReferenceTestUpdateAccumulator(
            wrappedWorldView(),
            accountPreloader,
            storagePreloader,
            preImageProxy,
            evmConfiguration);
    getAccountsToUpdate().forEach((k, v) -> copy.getAccountsToUpdate().put(k, v.copy()));
    getCodeToUpdate().forEach((k, v) -> copy.getCodeToUpdate().put(k, v.copy()));
    copy.getStorageToClear().addAll(getStorageToClear());
    getStorageToUpdate()
        .forEach(
            (k, v) -> {
              StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> newMap =
                  new StorageConsumingMap<>(k, new ConcurrentHashMap<>(), v.getConsumer());
              v.forEach((key, value) -> newMap.put(key, value.copy()));
              copy.getStorageToUpdate().put(k, newMap);
            });
    copy.updatedAccounts.putAll(updatedAccounts);
    copy.deletedAccounts.addAll(deletedAccounts);
    copy.isAccumulatorStateChanged = true;
    return copy;
  }
}
