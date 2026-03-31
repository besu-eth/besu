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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.List;

import org.junit.jupiter.api.Test;

public class BonsaiArchiveReadFlatDbStrategyProviderTest {

  private static final DataStorageConfiguration CONFIG =
      ImmutableDataStorageConfiguration.builder()
          .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
          .pathBasedExtraStorageConfiguration(
              ImmutablePathBasedExtraStorageConfiguration.builder().build())
          .build();

  @Test
  void alwaysReturnsBonsaiArchiveFlatDbStrategy() {
    final SegmentedKeyValueStorage storage =
        new InMemoryKeyValueStorageProvider()
            .getStorageBySegmentIdentifiers(
                List.of(
                    org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                        .TRIE_BRANCH_STORAGE,
                    org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                        .ACCOUNT_INFO_STATE,
                    org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                        .CODE_STORAGE,
                    org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                        .ACCOUNT_STORAGE_STORAGE));

    final BonsaiArchiveReadFlatDbStrategyProvider provider =
        new BonsaiArchiveReadFlatDbStrategyProvider(new NoOpMetricsSystem(), CONFIG);
    provider.loadFlatDbStrategy(storage);

    assertThat(provider.getFlatDbStrategy(storage))
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  @Test
  void returnsArchiveStrategyEvenForFullMode() {
    // Simulate a DB that has FlatDbMode.FULL stored — provider should still return archive
    final SegmentedKeyValueStorage storage =
        new InMemoryKeyValueStorageProvider()
            .getStorageBySegmentIdentifiers(
                List.of(
                    org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                        .TRIE_BRANCH_STORAGE,
                    org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                        .ACCOUNT_INFO_STATE,
                    org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                        .CODE_STORAGE,
                    org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                        .ACCOUNT_STORAGE_STORAGE));

    // Write FULL mode to the DB
    final var tx = storage.startTransaction();
    tx.put(
        org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
        org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategyProvider.FLAT_DB_MODE,
        FlatDbMode.FULL.getVersion().toArrayUnsafe());
    tx.commit();

    final BonsaiArchiveReadFlatDbStrategyProvider provider =
        new BonsaiArchiveReadFlatDbStrategyProvider(new NoOpMetricsSystem(), CONFIG);
    provider.loadFlatDbStrategy(storage);

    assertThat(provider.getFlatDbStrategy(storage))
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }
}
