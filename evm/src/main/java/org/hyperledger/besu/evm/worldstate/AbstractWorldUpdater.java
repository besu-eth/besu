/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.evm.worldstate;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An abstract implementation of a {@link WorldUpdater} that buffers update over the {@link
 * WorldView}* provided in the constructor in memory.
 *
 * <p>Concrete implementation have to implement the {@link #commit()} method.
 *
 * @param <W> the type parameter
 * @param <A> the type parameter
 */
public abstract class AbstractWorldUpdater<W extends WorldView, A extends Account>
    implements WorldUpdater {

  /** The worldview associated with this updater. */
  protected final W world;

  private final EvmConfiguration evmConfiguration;

  /** The Updated accounts. */
  protected Map<Address, UpdateTrackingAccount<A>> updatedAccounts = new ConcurrentHashMap<>();

  /** The Deleted accounts. */
  protected Set<Address> deletedAccounts = Collections.synchronizedSet(new HashSet<>(Address.SIZE));

  /**
   * Instantiates a new Abstract world updater.
   *
   * @param world the world
   * @param evmConfiguration the EVM Configuration parameters
   */
  protected AbstractWorldUpdater(final W world, final EvmConfiguration evmConfiguration) {
    this.world = world;
    this.evmConfiguration = evmConfiguration;
  }

  /**
   * Gets for mutation.
   *
   * @param address the address
   * @return the for mutation
   */
  protected abstract A getForMutation(Address address);

  /**
   * Track update tracking account.
   *
   * @param account the account
   * @return the update tracking account
   */
  protected UpdateTrackingAccount<A> track(final UpdateTrackingAccount<A> account) {
    final Address address = account.getAddress();
    updatedAccounts.put(address, account);
    deletedAccounts.remove(address);
    return account;
  }

  @Override
  public MutableAccount createAccount(final Address address, final long nonce, final Wei balance) {
    final UpdateTrackingAccount<A> account = new UpdateTrackingAccount<>(address);
    account.setNonce(nonce);
    account.setBalance(balance);
    return track(account);
  }

  @Override
  public Account get(final Address address) {
    // We may have updated it already, so check that first.
    final MutableAccount existing = updatedAccounts.get(address);
    if (existing != null) {
      return existing;
    }
    if (deletedAccounts.contains(address)) {
      return null;
    }
    return getForMutation(address);
  }

  @Override
  public MutableAccount getAccount(final Address address) {
    // We may have updated it already, so check that first.
    final MutableAccount existing = updatedAccounts.get(address);
    if (existing != null) {
      return existing;
    }
    if (deletedAccounts.contains(address)) {
      return null;
    }

    // Otherwise, get it from our wrapped view and create a new update tracker.
    final A origin = getForMutation(address);
    if (origin == null) {
      return null;
    } else {
      return track(new UpdateTrackingAccount<>(origin));
    }
  }

  @Override
  public void deleteAccount(final Address address) {
    deletedAccounts.add(address);
    updatedAccounts.remove(address);
  }

  /**
   * Creates an updater that buffer updates on top of this updater.
   *
   * @return a new updater on top of this updater. Updates made to the returned object will become
   *     visible on this updater when the returned updater is committed. Note however that updates
   *     to this updater <b>may or may not</b> be reflected to the created updater, so it is
   *     <b>strongly</b> advised to not update this updater until the returned one is discarded
   *     (either after having been committed, or because the updates it represents are meant to be
   *     discarded).
   */
  @Override
  public WorldUpdater updater() {
    return switch (evmConfiguration.worldUpdaterMode()) {
      case STACKED -> new StackedUpdater<>(this, evmConfiguration);
      case JOURNALED -> new JournaledUpdater<>(this, evmConfiguration);
    };
  }

  /**
   * The world view on top of which this buffer updates.
   *
   * @return The world view on top of which this buffer updates.
   */
  protected W wrappedWorldView() {
    return world;
  }

  @Override
  public Optional<WorldUpdater> parentUpdater() {
    if (world instanceof WorldUpdater worldUpdater) {
      return Optional.of(worldUpdater);
    } else {
      return Optional.empty();
    }
  }

  /**
   * The accounts modified in this updater.
   *
   * @return The accounts modified in this updater.
   */
  public Collection<UpdateTrackingAccount<A>> getUpdatedAccounts() {
    return updatedAccounts.values();
  }

  /**
   * The accounts deleted as part of this updater.
   *
   * @return The accounts deleted as part of this updater.
   */
  protected Collection<Address> getDeletedAccounts() {
    return deletedAccounts;
  }

  /** Reset. */
  protected void reset() {
    updatedAccounts.clear();
    deletedAccounts.clear();
  }
}
