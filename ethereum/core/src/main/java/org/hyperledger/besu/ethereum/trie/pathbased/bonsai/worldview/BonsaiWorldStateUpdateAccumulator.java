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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.Consumer;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;

public class BonsaiWorldStateUpdateAccumulator
    extends PathBasedWorldStateUpdateAccumulator<BonsaiAccount> {
  private final CodeCache codeCache;

  public BonsaiWorldStateUpdateAccumulator(
      final PathBasedWorldView world,
      final Consumer<PathBasedValue<BonsaiAccount>> accountPreloader,
      final Consumer<StorageSlotKey> storagePreloader,
      final EvmConfiguration evmConfiguration,
      final CodeCache codeCache) {
    super(world, accountPreloader, storagePreloader, evmConfiguration);

    this.codeCache = codeCache;
  }

  @Override
  public PathBasedWorldStateUpdateAccumulator<BonsaiAccount> copy() {
    final BonsaiWorldStateUpdateAccumulator copy =
        new BonsaiWorldStateUpdateAccumulator(
            wrappedWorldView(),
            getAccountPreloader(),
            getStoragePreloader(),
            getEvmConfiguration(),
            codeCache);
    copy.cloneFromUpdater(this);
    return copy;
  }

  @Override
  protected BonsaiAccount copyAccount(final BonsaiAccount account) {
    return new BonsaiAccount(account);
  }

  @Override
  protected BonsaiAccount copyAccount(
      final BonsaiAccount toCopy, final PathBasedWorldView context, final boolean mutable) {
    return new BonsaiAccount(toCopy, context, mutable);
  }

  @Override
  protected BonsaiAccount createAccount(
      final PathBasedWorldView context,
      final Address address,
      final AccountValue stateTrieAccount,
      final boolean mutable) {
    return new BonsaiAccount(context, address, stateTrieAccount, mutable, codeCache);
  }

  @Override
  protected BonsaiAccount createAccount(
      final PathBasedWorldView context,
      final Address address,
      final Hash addressHash,
      final long nonce,
      final Wei balance,
      final Hash storageRoot,
      final Hash codeHash,
      final boolean mutable) {
    return new BonsaiAccount(
        context, address, addressHash, nonce, balance, storageRoot, codeHash, mutable, codeCache);
  }

  @Override
  protected BonsaiAccount createAccount(
      final PathBasedWorldView context, final UpdateTrackingAccount<BonsaiAccount> tracked) {
    return new BonsaiAccount(context, tracked, codeCache);
  }

  @Override
  protected void assertCloseEnoughForDiffing(
      final BonsaiAccount source, final AccountValue account, final String context) {
    BonsaiAccount.assertCloseEnoughForDiffing(source, account, context);
  }

  @Override
  public CodeCache codeCache() {
    return codeCache;
  }
}
