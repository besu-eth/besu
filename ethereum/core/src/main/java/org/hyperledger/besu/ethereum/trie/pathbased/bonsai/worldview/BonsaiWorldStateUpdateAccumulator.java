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

import java.util.HashSet;
import java.util.Set;

public class BonsaiWorldStateUpdateAccumulator
    extends PathBasedWorldStateUpdateAccumulator<BonsaiAccount> {

  private static final CommittedTransactionListener NO_OP_LISTENER =
      new CommittedTransactionListener() {
        @Override
        public void onTransactionCommitted(final CommittedTransactionChanges changes) {}

        @Override
        public void onReset() {}
      };

  private final CodeCache codeCache;
  private CommittedTransactionListener committedTransactionListener = NO_OP_LISTENER;

  public BonsaiWorldStateUpdateAccumulator(
      final PathBasedWorldView world,
      final Consumer<PathBasedValue<BonsaiAccount>> accountPreloader,
      final Consumer<StorageSlotKey> storagePreloader,
      final EvmConfiguration evmConfiguration,
      final CodeCache codeCache) {
    super(world, accountPreloader, storagePreloader, evmConfiguration);

    this.codeCache = codeCache;
  }

  /** Copy constructor. */
  protected BonsaiWorldStateUpdateAccumulator(final BonsaiWorldStateUpdateAccumulator source) {
    this(source, source.committedTransactionListener);
  }

  private BonsaiWorldStateUpdateAccumulator(
      final BonsaiWorldStateUpdateAccumulator source,
      final CommittedTransactionListener committedTransactionListener) {
    super(source);
    this.codeCache = source.codeCache;
    this.committedTransactionListener = committedTransactionListener;
  }

  @Override
  public BonsaiWorldStateUpdateAccumulator copy() {
    // Copies serve as per-tx workers (parallel-tx, simulation) and must not re-emit committed-
    // transaction events to the registered listener; detach it on the copy.
    return new BonsaiWorldStateUpdateAccumulator(this, NO_OP_LISTENER);
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

  void setCommittedTransactionListener(final CommittedTransactionListener listener) {
    this.committedTransactionListener = listener == null ? NO_OP_LISTENER : listener;
  }

  @Override
  public void commit() {
    super.commit();
    final Set<Address> changed = new HashSet<>();
    getUpdatedAccounts().forEach(account -> changed.add(account.getAddress()));
    changed.addAll(getDeletedAccountAddresses());
    if (!changed.isEmpty()) {
      committedTransactionListener.onTransactionCommitted(new CommittedTransactionChanges(changed));
    }
  }

  @Override
  public void importStateChangesFromSource(
      final PathBasedWorldStateUpdateAccumulator<BonsaiAccount> source) {
    super.importStateChangesFromSource(source);
    // Parallel tx processing imports state changes here instead of via commit(); emit the same
    // snapshot from this path so listeners observe both code paths uniformly.
    final Set<Address> changed = new HashSet<>(source.getAccountsToUpdate().keySet());
    if (!changed.isEmpty()) {
      committedTransactionListener.onTransactionCommitted(new CommittedTransactionChanges(changed));
    }
  }

  @Override
  public void reset() {
    super.reset();
    // After super.reset(), accountsToUpdate is empty; any listener-side cache derived from the
    // accumulator is now stale and must be invalidated. revert() does NOT route through here
    // (it bypasses to AbstractWorldUpdater.reset()), which is the intended behavior — listeners
    // tracking committed deltas should survive a per-tx revert.
    committedTransactionListener.onReset();
  }

  @Override
  public CodeCache codeCache() {
    return codeCache;
  }
}
