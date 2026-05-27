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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlatDbStrategyProvider {
  private static final Logger LOG = LoggerFactory.getLogger(FlatDbStrategyProvider.class);

  // 0x666C61744462537461747573
  public static final byte[] FLAT_DB_MODE = "flatDbStatus".getBytes(StandardCharsets.UTF_8);
  protected final MetricsSystem metricsSystem;
  protected final DataStorageConfiguration dataStorageConfiguration;
  protected FlatDbMode flatDbMode;
  protected volatile FlatDbStrategy flatDbStrategy;

  public FlatDbStrategyProvider(
      final MetricsSystem metricsSystem, final DataStorageConfiguration dataStorageConfiguration) {
    this.metricsSystem = metricsSystem;
    this.dataStorageConfiguration = dataStorageConfiguration;
  }

  public void loadFlatDbStrategy(final SegmentedKeyValueStorage composedWorldStateStorage) {
    // derive our flatdb strategy from db or default:
    var newFlatDbMode = deriveFlatDbStrategy(composedWorldStateStorage);

    // if  flatDbMode is not loaded or has changed, reload flatDbStrategy
    if (this.flatDbMode == null || !this.flatDbMode.equals(newFlatDbMode)) {
      this.flatDbMode = newFlatDbMode;
      final CodeStorageStrategy codeStorageStrategy =
          deriveUseCodeStorageByHash(composedWorldStateStorage)
              ? new CodeHashCodeStorageStrategy()
              : new AccountHashCodeStorageStrategy();
      this.flatDbStrategy = createFlatDbStrategy(flatDbMode, metricsSystem, codeStorageStrategy);
    }
  }

  protected boolean deriveUseCodeStorageByHash(
      final SegmentedKeyValueStorage composedWorldStateStorage) {
    final boolean configCodeUsingHash =
        dataStorageConfiguration
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getCodeStoredByCodeHashEnabled();
    boolean codeUsingCodeByHash =
        detectCodeStorageByHash(composedWorldStateStorage)
            .map(
                dbCodeUsingHash -> {
                  if (dbCodeUsingHash != configCodeUsingHash) {
                    LOG.warn(
                        "Bonsai db is using code storage mode {} but config specifies mode {}. Using mode from database",
                        dbCodeUsingHash,
                        configCodeUsingHash);
                  }
                  return dbCodeUsingHash;
                })
            .orElse(configCodeUsingHash);
    LOG.info("DB mode with code stored using code hash enabled = {}", codeUsingCodeByHash);
    return codeUsingCodeByHash;
  }

  private Optional<Boolean> detectCodeStorageByHash(
      final SegmentedKeyValueStorage composedWorldStateStorage) {
    return composedWorldStateStorage.stream(CODE_STORAGE)
        .limit(1)
        .findFirst()
        .map(
            keypair ->
                CodeHashCodeStorageStrategy.isCodeHashValue(keypair.getKey(), keypair.getValue()));
  }

  @VisibleForTesting
  private FlatDbMode deriveFlatDbStrategy(
      final SegmentedKeyValueStorage composedWorldStateStorage) {
    final FlatDbMode requestedFlatDbMode = getRequestedFlatDbMode(dataStorageConfiguration);

    final var existingTrieData =
        composedWorldStateStorage.get(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY).isPresent();

    var flatDbMode =
        FlatDbMode.fromVersion(
            composedWorldStateStorage
                .get(TRIE_BRANCH_STORAGE, FLAT_DB_MODE)
                .map(Bytes::wrap)
                .orElseGet(
                    () -> {
                      // if we do not have a db-supplied config for flatdb, derive it:
                      //  - default to partial if trie data exists but the flat config does not,
                      //  - otherwise go with the requested mode
                      var flatDbModeVal =
                          existingTrieData
                              ? alternativeFlatDbModeForExistingDatabase().getVersion()
                              : requestedFlatDbMode.getVersion();
                      // persist this config in the db
                      var setDbModeTx = composedWorldStateStorage.startTransaction();
                      setDbModeTx.put(
                          TRIE_BRANCH_STORAGE, FLAT_DB_MODE, flatDbModeVal.toArrayUnsafe());
                      setDbModeTx.commit();

                      return flatDbModeVal;
                    }));
    LOG.info("Flat db mode found {}", flatDbMode);

    return flatDbMode;
  }

  public FlatDbStrategy getFlatDbStrategy(
      final SegmentedKeyValueStorage composedWorldStateStorage) {
    if (flatDbStrategy == null) {
      loadFlatDbStrategy(composedWorldStateStorage);
    }
    return flatDbStrategy;
  }

  public FlatDbMode getFlatDbMode() {
    return flatDbMode;
  }

  protected abstract FlatDbMode getRequestedFlatDbMode(
      final DataStorageConfiguration dataStorageConfiguration);

  protected abstract FlatDbMode alternativeFlatDbModeForExistingDatabase();

  protected abstract FlatDbStrategy createFlatDbStrategy(
      final FlatDbMode flatDbMode,
      final MetricsSystem metricsSystem,
      final CodeStorageStrategy codeStorageStrategy);
}
