/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.worldstate;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.TransactionDBRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.nio.file.Path;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WorldStateStorageCoordinatorTest {

  @TempDir Path folder;

  @Test
  @Timeout(30)
  void forestBatchUpdatersWritingTheSameNodeDoNotWaitForEachOther() throws Exception {
    try (final TransactionDBRocksDBColumnarKeyValueStorage database =
        new TransactionDBRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder().databaseDir(folder).build(),
            List.of(KeyValueSegmentIdentifier.DEFAULT, KeyValueSegmentIdentifier.WORLD_STATE),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)) {
      final WorldStateStorageCoordinator coordinator =
          new WorldStateStorageCoordinator(
              new ForestWorldStateKeyValueStorage(
                  new SegmentedKeyValueStorageAdapter(
                      KeyValueSegmentIdentifier.WORLD_STATE, database)));
      final Bytes node = Bytes.fromHexString("0x0123456789");
      final Bytes32 nodeHash = Bytes32.wrap(Hash.hash(node).getBytes());

      final WorldStateKeyValueStorage.Updater first = coordinator.batchUpdater();
      final WorldStateKeyValueStorage.Updater second = coordinator.batchUpdater();
      ((ForestWorldStateKeyValueStorage.Updater) first).putAccountStorageTrieNode(nodeHash, node);
      // a transaction would wait here for the first writer's lock until the lock timeout
      ((ForestWorldStateKeyValueStorage.Updater) second).putAccountStorageTrieNode(nodeHash, node);
      first.commit();
      second.commit();

      assertThat(coordinator.getAccountStorageTrieNode(Hash.EMPTY, Bytes.EMPTY, nodeHash))
          .contains(node);
    }
  }
}
