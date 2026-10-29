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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ForwardingMap;
import jakarta.validation.constraints.NotNull;

/**
 * The accounts of a block. The consumer preloads the trie path of each account the block changes,
 * once, ahead of the state root. Accounts the block only reads are skipped, the state root leaves
 * them as they are.
 */
public class AccountConsumingMap<T extends BonsaiValue<? extends BonsaiAccount>>
    extends ForwardingMap<Address, T> {

  private final ConcurrentMap<Address, T> accounts;
  private final Consumer<T> consumer;
  private final Set<Address> preloaded = ConcurrentHashMap.newKeySet();

  public AccountConsumingMap(final ConcurrentMap<Address, T> accounts, final Consumer<T> consumer) {
    this.accounts = accounts;
    this.consumer = consumer;
  }

  @Override
  public T put(@NotNull final Address address, @NotNull final T value) {
    final T previous = accounts.put(address, value);
    if (isChanged(value)) {
      preload(address);
    }
    return previous;
  }

  /**
   * Preloads an account whose fields the block changed in place.
   *
   * @param address the account
   */
  public void preloadIfChanged(final Address address) {
    final T value = accounts.get(address);
    if (value != null && isChanged(value)) {
      preload(address);
    }
  }

  /**
   * Preloads an account once per block, such as one whose storage the block changes.
   *
   * @param address the account
   */
  public void preload(final Address address) {
    if (preloaded.add(address)) {
      consumer.process(address, accounts.get(address));
    }
  }

  private boolean isChanged(final T value) {
    return !BonsaiAccount.trieValueEquals(value.getPrior(), value.getUpdated());
  }

  /** An account put back after its removal is preloaded again if it changed. */
  @Override
  public T remove(final Object address) {
    final T previous = super.remove(address);
    preloaded.remove(address);
    return previous;
  }

  @Override
  public void clear() {
    super.clear();
    preloaded.clear();
  }

  public Consumer<T> getConsumer() {
    return consumer;
  }

  @Override
  protected Map<Address, T> delegate() {
    return accounts;
  }
}
