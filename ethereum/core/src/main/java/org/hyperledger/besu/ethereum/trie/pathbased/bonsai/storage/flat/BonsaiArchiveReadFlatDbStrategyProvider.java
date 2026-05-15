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

import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.MetricsSystem;

/**
 * A {@link BonsaiFlatDbStrategyProvider} that always returns {@link BonsaiArchiveFlatDbStrategy},
 * regardless of the {@link FlatDbMode} stored in the database. Used to create a read-only archive
 * storage view that shares the same underlying RocksDB segments as the main storage but routes all
 * flat-DB reads through the seekForPrev archive path.
 */
public class BonsaiArchiveReadFlatDbStrategyProvider extends BonsaiFlatDbStrategyProvider {

  public BonsaiArchiveReadFlatDbStrategyProvider(
      final MetricsSystem metricsSystem, final DataStorageConfiguration dataStorageConfiguration) {
    super(metricsSystem, dataStorageConfiguration);
  }

  @Override
  protected FlatDbStrategy createFlatDbStrategy(
      final FlatDbMode flatDbMode,
      final MetricsSystem metricsSystem,
      final CodeStorageStrategy codeStorageStrategy) {
    return new BonsaiArchiveFlatDbStrategy(metricsSystem, codeStorageStrategy);
  }
}
