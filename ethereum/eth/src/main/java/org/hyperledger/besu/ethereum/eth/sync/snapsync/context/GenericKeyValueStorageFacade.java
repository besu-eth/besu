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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.context;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class GenericKeyValueStorageFacade<K, V> implements Closeable {
  protected final KeyValueStorage storage;
  private final KeyConvertor<K> keyConvertor;
  private final ValueConvertor<V> valueConvertor;

  public GenericKeyValueStorageFacade(
      final KeyConvertor<K> keyConvertor,
      final ValueConvertor<V> valueConvertor,
      final KeyValueStorage storageBySegmentIdentifier) {
    this.keyConvertor = keyConvertor;
    this.valueConvertor = valueConvertor;
    this.storage = storageBySegmentIdentifier;
  }

  public Optional<V> get(final K key) {
    return storage.get(keyConvertor.toBytes(key)).map(valueConvertor::fromBytes);
  }

  public Optional<V> get(final byte[] key) {
    return storage.get(key).map(valueConvertor::fromBytes);
  }

  public void put(final K key, final V value) {
    final KeyValueStorageTransaction keyValueStorageTransaction = storage.startTransaction();
    keyValueStorageTransaction.put(keyConvertor.toBytes(key), valueConvertor.toBytes(value));
    keyValueStorageTransaction.commit();
  }

  public void put(final byte[] key, final V value) {
    final KeyValueStorageTransaction keyValueStorageTransaction = storage.startTransaction();
    keyValueStorageTransaction.put(key, valueConvertor.toBytes(value));
    keyValueStorageTransaction.commit();
  }

  public void putAll(
      final Consumer<KeyValueStorageTransaction> keyValueStorageTransactionConsumer) {
    final KeyValueStorageTransaction keyValueStorageTransaction = storage.startTransaction();
    keyValueStorageTransactionConsumer.accept(keyValueStorageTransaction);
    keyValueStorageTransaction.commit();
  }

  public void drop(final K key) {
    storage.tryDelete(keyConvertor.toBytes(key));
  }

  public void clear() {
    storage.clear();
  }

  @Override
  public void close() throws IOException {
    storage.close();
  }

  public void putAll(final Map<K, V> map) {
    final KeyValueStorageTransaction keyValueStorageTransaction = storage.startTransaction();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      keyValueStorageTransaction.put(
          keyConvertor.toBytes(entry.getKey()), valueConvertor.toBytes(entry.getValue()));
    }
    keyValueStorageTransaction.commit();
  }

  public Stream<V> streamValuesFromKeysThat(final Predicate<byte[]> returnCondition)
      throws StorageException {
    return storage.getAllValuesFromKeysThat(returnCondition).stream()
        .map(valueConvertor::fromBytes);
  }
}
