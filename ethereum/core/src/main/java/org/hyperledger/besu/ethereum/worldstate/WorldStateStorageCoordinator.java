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
package org.hyperledger.besu.ethereum.worldstate;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class WorldStateStorageCoordinator {
  private final WorldStateKeyValueStorage worldStateKeyValueStorage;

  public WorldStateStorageCoordinator(final WorldStateKeyValueStorage worldStateKeyValueStorage) {
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
  }

  public DataStorageFormat getDataStorageFormat() {
    return worldStateKeyValueStorage.getDataStorageFormat();
  }

  public boolean isWorldStateAvailable(final Bytes32 nodeHash, final Hash blockHash) {
    return applyForStrategy(
        bonsai -> bonsai.isWorldStateAvailable(nodeHash, blockHash),
        forest -> forest.isWorldStateAvailable(nodeHash));
  }

  public Optional<Bytes> getTrieNodeUnsafe(final Bytes key) {
    return applyForStrategy(
        bonsai -> bonsai.getTrieNodeUnsafe(key),
        forest -> forest.getAccountStateTrieNode(Bytes32.wrap(key)));
  }

  public Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    return applyForStrategy(
        bonsai -> bonsai.getAccountStateTrieNode(location, nodeHash),
        forest -> forest.getAccountStateTrieNode(nodeHash));
  }

  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    return applyForStrategy(
        bonsai -> bonsai.getAccountStorageTrieNode(accountHash, location, nodeHash),
        forest -> forest.getAccountStorageTrieNode(nodeHash));
  }

  public Optional<Bytes> getCode(final Hash codeHash, final Hash accountHash) {
    return applyForStrategy(
        bonsai -> bonsai.getCode(codeHash, accountHash), forest -> forest.getCode(codeHash));
  }

  @SuppressWarnings("unchecked")
  public <STRATEGY extends WorldStateKeyValueStorage> STRATEGY getStrategy(
      final Class<STRATEGY> strategyClass) {
    return (STRATEGY) worldStateKeyValueStorage;
  }

  public boolean isMatchingFlatMode(final FlatDbMode flatDbMode) {
    if (getDataStorageFormat().isBonsaiFormat()) {
      final BonsaiWorldStateKeyValueStorage bonsaiWorldStateStorageStrategy =
          (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage();
      return bonsaiWorldStateStorageStrategy.getFlatDbMode().equals(flatDbMode);
    }
    return false;
  }

  public void applyOnMatchingFlatMode(
      final FlatDbMode flatDbMode, final Consumer<BonsaiWorldStateKeyValueStorage> onStrategy) {
    applyOnMatchingStrategies(
        List.of(DataStorageFormat.BONSAI, DataStorageFormat.X_BONSAI_ARCHIVE),
        worldStateKeyValueStorage -> {
          final BonsaiWorldStateKeyValueStorage bonsaiWorldStateStorageStrategy =
              (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage();
          if (bonsaiWorldStateStorageStrategy.getFlatDbMode().equals(flatDbMode)) {
            onStrategy.accept(bonsaiWorldStateStorageStrategy);
          }
        });
  }

  public void applyOnMatchingFlatModes(
      final List<FlatDbMode> flatDbModes,
      final Consumer<BonsaiWorldStateKeyValueStorage> onStrategy) {
    applyOnMatchingStrategies(
        List.of(DataStorageFormat.BONSAI, DataStorageFormat.X_BONSAI_ARCHIVE),
        worldStateKeyValueStorage -> {
          final BonsaiWorldStateKeyValueStorage bonsaiWorldStateStorageStrategy =
              (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage();

          if (flatDbModes.contains(bonsaiWorldStateStorageStrategy.getFlatDbMode())) {
            onStrategy.accept(bonsaiWorldStateStorageStrategy);
          }
        });
  }

  public void applyWhenFlatModeEnabled(final Consumer<BonsaiWorldStateKeyValueStorage> onStrategy) {
    applyOnMatchingStrategies(
        List.of(DataStorageFormat.BONSAI, DataStorageFormat.X_BONSAI_ARCHIVE),
        worldStateKeyValueStorage -> {
          final BonsaiWorldStateKeyValueStorage bonsaiWorldStateStorageStrategy =
              (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage();
          if (!bonsaiWorldStateStorageStrategy.getFlatDbMode().equals(FlatDbMode.NO_FLATTENED)) {
            onStrategy.accept(bonsaiWorldStateStorageStrategy);
          }
        });
  }

  public void applyOnMatchingStrategy(
      final DataStorageFormat dataStorageFormat,
      final Consumer<WorldStateKeyValueStorage> onStrategy) {
    if (getDataStorageFormat().equals(dataStorageFormat)) {
      onStrategy.accept(worldStateKeyValueStorage());
    }
  }

  public void applyOnMatchingStrategies(
      final List<DataStorageFormat> dataStorageFormats,
      final Consumer<WorldStateKeyValueStorage> onStrategy) {
    if (dataStorageFormats.contains(getDataStorageFormat())) {
      onStrategy.accept(worldStateKeyValueStorage());
    }
  }

  public <RESPONSE> RESPONSE applyForStrategy(
      final Function<BonsaiWorldStateKeyValueStorage, RESPONSE> onBonsai,
      final Function<ForestWorldStateKeyValueStorage, RESPONSE> onForest) {
    if (getDataStorageFormat().isBonsaiFormat()) {
      return onBonsai.apply(((BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage()));
    } else {
      return onForest.apply(((ForestWorldStateKeyValueStorage) worldStateKeyValueStorage()));
    }
  }

  public void consumeForStrategy(
      final Consumer<BonsaiWorldStateKeyValueStorage> onBonsai,
      final Consumer<ForestWorldStateKeyValueStorage> onForest) {
    if (getDataStorageFormat().isBonsaiFormat()) {
      onBonsai.accept(((BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage()));
    } else {
      onForest.accept(((ForestWorldStateKeyValueStorage) worldStateKeyValueStorage()));
    }
  }

  public static void applyForStrategy(
      final WorldStateKeyValueStorage.Updater updater,
      final Consumer<BonsaiWorldStateKeyValueStorage.Updater> onBonsai,
      final Consumer<ForestWorldStateKeyValueStorage.Updater> onForest) {
    if (updater instanceof BonsaiWorldStateKeyValueStorage.Updater) {
      onBonsai.accept(((BonsaiWorldStateKeyValueStorage.Updater) updater));
    } else if (updater instanceof ForestWorldStateKeyValueStorage.Updater) {
      onForest.accept(((ForestWorldStateKeyValueStorage.Updater) updater));
    }
  }

  public WorldStateKeyValueStorage.Updater updater() {
    return worldStateKeyValueStorage().updater();
  }

  public void clear() {
    worldStateKeyValueStorage.clear();
  }

  public WorldStateKeyValueStorage worldStateKeyValueStorage() {
    return worldStateKeyValueStorage;
  }
}
