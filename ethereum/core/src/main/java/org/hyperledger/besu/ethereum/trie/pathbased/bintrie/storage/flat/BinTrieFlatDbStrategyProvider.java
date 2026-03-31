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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.flat;

import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider for BinTrie flat database strategies. Supports two modes:
 *
 * <ul>
 *   <li>FULL: Uses {@link BinTrieFlatDbStrategy} which stores account and storage data in a flat
 *       key-value structure.
 *   <li>STEM: Uses {@link BinTrieStemFlatDbStrategy} which accesses data directly from the trie
 *       structure via stems, reducing data duplication.
 * </ul>
 */
public class BinTrieFlatDbStrategyProvider extends FlatDbStrategyProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BinTrieFlatDbStrategyProvider.class);

  public BinTrieFlatDbStrategyProvider(
      final MetricsSystem metricsSystem, final DataStorageConfiguration dataStorageConfiguration) {
    super(metricsSystem, dataStorageConfiguration);
  }

  @Override
  protected FlatDbMode getRequestedFlatDbMode(
      final DataStorageConfiguration dataStorageConfiguration) {
    return dataStorageConfiguration
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getFullFlatDbEnabled()
        ? FlatDbMode.FULL
        : FlatDbMode.STEM;
  }

  @Override
  protected FlatDbMode alternativeFlatDbModeForExistingDatabase() {
    return FlatDbMode.FULL;
  }

  @Override
  protected FlatDbStrategy createFlatDbStrategy(
      final FlatDbMode flatDbMode,
      final MetricsSystem metricsSystem,
      final CodeStorageStrategy codeStorageStrategy) {
    if (flatDbMode == FlatDbMode.STEM) {
      LOG.info("Using BinTrie STEM flat database strategy");
      return new BinTrieStemFlatDbStrategy(metricsSystem, codeStorageStrategy);
    } else {
      LOG.info("Using BinTrie FULL flat database strategy");
      return new BinTrieFlatDbStrategy(metricsSystem, codeStorageStrategy);
    }
  }
}
