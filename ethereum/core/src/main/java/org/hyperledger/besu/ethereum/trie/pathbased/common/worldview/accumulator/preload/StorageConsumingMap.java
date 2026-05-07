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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload;

import org.hyperledger.besu.datatypes.Address;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ForwardingMap;
import jakarta.validation.constraints.NotNull;

public class StorageConsumingMap<K, T> extends ForwardingMap<K, T> {

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
    consumer.process(address, slotKey);
    return putWithoutPreload(slotKey, value);
  }

  /**
   * Insert without triggering the preload consumer. Use when the slot is already known to the
   * caller (e.g. merging an executed transaction's changes into the block accumulator) so spawning
   * an async trie-node fetch would only burn cycles.
   */
  public T putWithoutPreload(@NotNull final K slotKey, @NotNull final T value) {
    return storages.put(slotKey, value);
  }

  /** Same as {@link #putWithoutPreload} but only inserts when the key is absent. */
  public T putIfAbsentWithoutPreload(@NotNull final K slotKey, @NotNull final T value) {
    return storages.putIfAbsent(slotKey, value);
  }

  public Consumer<K> getConsumer() {
    return consumer;
  }

  @Override
  protected Map<K, T> delegate() {
    return storages;
  }
}
