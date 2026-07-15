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

import java.util.ArrayList;
import java.util.Set;

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

  /**
   * For archive proof rolling: prepare the rolled accumulator for a {@code persist()} that only
   * materialises the storage tries the proof actually needs.
   *
   * <p>After rolling from the checkpoint to the target block, every account with storage changes
   * carries its target-block storage root, and its slot diffs are queued in {@code
   * storageToUpdate}. A full persist would rebuild the storage trie for <em>every</em> such account
   * — the dominant cost of an archive proof on a busy window — even though a proof only needs the
   * storage trie of the account(s) it is proving.
   *
   * <p>For each account in {@code accountsToRebuild}: reset its storage root to the checkpoint value
   * ({@code getPrior().storageRoot}) so persist() starts from a root whose nodes ARE in the archive
   * CF and re-derives the target root from the slot diffs, materialising the storage-trie nodes the
   * storage proof will traverse.
   *
   * <p>For every other account (a plain update): drop its slot diffs so persist() does no
   * storage-trie work for it. The account keeps its already-rolled target storage root, which is all
   * the account trie needs, so the computed state root still matches the target block. Created,
   * deleted and self-destruct-cleared accounts are left untouched for normal processing.
   *
   * @param accountsToRebuild the accounts whose storage tries must be materialised (typically just
   *     the account being proved, when storage keys were requested). Pass {@code null} to rebuild
   *     <em>all</em> storage tries — required when the rolled world state must be fully materialised
   *     (e.g. serving a historical {@code eth_call}/{@code eth_getBalance}, not a proof).
   */
  public void resetStorageRootsToCheckpointForArchiveProof(final Set<Address> accountsToRebuild) {
    for (final Address address : new ArrayList<>(getStorageToUpdate().keySet())) {
      final PathBasedValue<BonsaiAccount> accountValue = getAccountsToUpdate().get(address);
      if (accountValue == null
          || accountValue.getUpdated() == null
          || accountValue.getPrior() == null) {
        // Created (prior == null) or deleted (updated == null) account: leave for normal
        // processing to preserve creation / self-destruct semantics.
        continue;
      }
      if (accountsToRebuild == null || accountsToRebuild.contains(address)) {
        accountValue.getUpdated().setStorageRoot(accountValue.getPrior().getStorageRoot());
      } else if (!getStorageToClear().contains(address)) {
        // Plain storage update not needed by this proof: skip its storage-trie rebuild entirely.
        getStorageToUpdate().remove(address);
      }
    }
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
