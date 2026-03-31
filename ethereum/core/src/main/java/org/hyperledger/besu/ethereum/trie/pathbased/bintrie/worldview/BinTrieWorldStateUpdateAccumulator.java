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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.Consumer;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;

/** Accumulator for tracking worldstate updates in a Binary Trie. */
public class BinTrieWorldStateUpdateAccumulator
    extends PathBasedWorldStateUpdateAccumulator<BinTrieAccount> {

  private final CodeCache codeCache;

  public BinTrieWorldStateUpdateAccumulator(
      final PathBasedWorldView world,
      final EvmConfiguration evmConfiguration,
      final CodeCache codeCache) {
    super(world, (addr, value) -> {}, (addr, value) -> {}, evmConfiguration);
    this.codeCache = codeCache;
  }

  @Override
  public CodeCache codeCache() {
    return codeCache;
  }

  public BinTrieWorldStateUpdateAccumulator(
      final PathBasedWorldView world,
      final Consumer<PathBasedValue<BinTrieAccount>> accountPreloader,
      final Consumer<StorageSlotKey> storagePreloader,
      final EvmConfiguration evmConfiguration,
      final CodeCache codeCache) {
    super(world, accountPreloader, storagePreloader, evmConfiguration);
    this.codeCache = codeCache;
  }

  @Override
  public PathBasedWorldStateUpdateAccumulator<BinTrieAccount> copy() {
    final BinTrieWorldStateUpdateAccumulator copy =
        new BinTrieWorldStateUpdateAccumulator(
            wrappedWorldView(),
            getAccountPreloader(),
            getStoragePreloader(),
            getEvmConfiguration(),
            codeCache);
    copy.cloneFromUpdater(this);
    return copy;
  }

  @Override
  protected BinTrieAccount copyAccount(final BinTrieAccount account) {
    return new BinTrieAccount(account);
  }

  @Override
  protected BinTrieAccount copyAccount(
      final BinTrieAccount toCopy, final PathBasedWorldView context, final boolean mutable) {
    return new BinTrieAccount(toCopy, context, mutable);
  }

  @Override
  protected BinTrieAccount createAccount(
      final PathBasedWorldView context,
      final Address address,
      final AccountValue stateTrieAccount,
      final boolean mutable) {
    return new BinTrieAccount(context, address, stateTrieAccount, mutable, codeCache);
  }

  @Override
  protected BinTrieAccount createAccount(
      final PathBasedWorldView context,
      final Address address,
      final Hash addressHash,
      final long nonce,
      final Wei balance,
      final Hash storageRoot,
      final Hash codeHash,
      final boolean mutable) {
    return new BinTrieAccount(
        context, address, addressHash, nonce, balance, codeHash, 0L, mutable, codeCache);
  }

  @Override
  protected BinTrieAccount createAccount(
      final PathBasedWorldView context, final UpdateTrackingAccount<BinTrieAccount> tracked) {
    return new BinTrieAccount(context, tracked, codeCache);
  }

  @Override
  protected void assertCloseEnoughForDiffing(
      final BinTrieAccount source, final AccountValue expectedAccountValue, final String context) {
    BinTrieAccount.assertCloseEnoughForDiffing(source, expectedAccountValue, context);
  }

  public CodeCache getCodeCache() {
    return codeCache;
  }
}
