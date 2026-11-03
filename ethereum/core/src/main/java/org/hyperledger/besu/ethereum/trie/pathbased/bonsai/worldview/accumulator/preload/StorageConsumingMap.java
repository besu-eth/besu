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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ForwardingMap;
import jakarta.validation.constraints.NotNull;

/**
 * The storage slots of an account in a block. The consumer preloads the trie path of a slot when
 * the block changes it, ahead of the state root. Slots the block only reads are skipped, the state
 * root leaves them as they are.
 */
public class StorageConsumingMap<K, T extends BonsaiValue<?>> extends ForwardingMap<K, T> {

  private final Address address;

  private final ConcurrentMap<K, T> storages;
  private final Consumer<K> consumer;

  public StorageConsumingMap(
      final Address address, final ConcurrentMap<K, T> storages, final Consumer<K> consumer) {
    this.address = address;
    this.storages = storages;
    this.consumer = consumer;
  }

  @Override
  public T put(@NotNull final K slotKey, @NotNull final T value) {
    final T previous = storages.put(slotKey, value);
    if (value.isChanged() && (previous == null || !previous.isChanged())) {
      consumer.process(address, slotKey);
    }
    return previous;
  }

  /**
   * Sets the updated value of a slot of this map, and preloads the slot once the block changes it.
   *
   * @param slotKey the slot
   * @param slot the value of the slot in this map
   * @param value the slot's new value
   * @param <V> the type of the slot's value
   */
  public <V> void update(final K slotKey, final BonsaiValue<V> slot, final V value) {
    final boolean wasChanged = slot.isChanged();
    slot.setUpdated(value);
    if (!wasChanged && slot.isChanged()) {
      consumer.process(address, slotKey);
    }
  }

  /**
   * Puts a slot loaded for a read. A read is not preloaded, and the value is not compared with its
   * prior, which could load the prior from storage.
   *
   * @param slotKey the slot
   * @param value the loaded value
   * @return the previous value of the slot, or null
   */
  public T putLoaded(final K slotKey, final T value) {
    return storages.put(slotKey, value);
  }

  public Consumer<K> getConsumer() {
    return consumer;
  }

  @Override
  protected Map<K, T> delegate() {
    return storages;
  }
}
