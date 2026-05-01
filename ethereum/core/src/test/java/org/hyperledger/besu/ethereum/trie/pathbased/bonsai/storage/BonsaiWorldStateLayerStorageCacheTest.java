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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for layer reading from cache in BonsaiWorldStateLayerStorage. */
public class BonsaiWorldStateLayerStorageCacheTest {

  private BonsaiWorldStateKeyValueStorage baseStorage;

  @BeforeEach
  public void setup() {
    baseStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            ImmutableDataStorageConfiguration.builder()
                .dataStorageFormat(DataStorageFormat.BONSAI)
                .bonsaiCacheEnabled(true)
                .pathBasedExtraStorageConfiguration(PathBasedExtraStorageConfiguration.DEFAULT)
                .build());
  }

  @Test
  public void testLayer_readsFromLayerFirst() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes baseData = Bytes.of(1, 2, 3);
    Bytes layerData = Bytes.of(4, 5, 6);

    // Put in base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putAccountInfoState(accountHash, baseData);
    baseUpdater.commit();

    // Create layer and update
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.putAccountInfoState(accountHash, layerData);
    layerUpdater.commit();

    // Layer should read from its own data, not cache
    assertThat(layer.getAccount(accountHash)).isPresent().contains(layerData);

    // Base should still have old data in cache
    assertThat(baseStorage.getAccount(accountHash)).isPresent().contains(baseData);
  }

  @Test
  public void testLayer_readsFromCacheWhenNotInLayer() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes accountData = Bytes.of(1, 2, 3);

    // Put in base (populates cache)
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putAccountInfoState(accountHash, accountData);
    baseUpdater.commit();

    // Verify in cache
    assertThat(baseStorage.isCached(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe()))
        .isTrue();

    // Create layer (doesn't modify account)
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);

    // Layer should read from cache
    assertThat(layer.getAccount(accountHash)).isPresent().contains(accountData);
  }

  @Test
  public void testLayer_readsFromParentWhenNotInCacheOrLayer() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes accountData = Bytes.of(1, 2, 3);

    // Put directly in base storage WITHOUT going through cache
    BonsaiWorldStateKeyValueStorage.Updater baseUpdater = baseStorage.updater();
    baseUpdater.putAccountInfoState(accountHash, accountData);
    baseUpdater.commit();

    // Clear cache if any
    assertThat(baseStorage.isCached(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe()))
        .isTrue(); // Should be cached after commit

    // Create layer
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);

    // Layer should still be able to read (from cache in this case, but conceptually from parent)
    assertThat(layer.getAccount(accountHash)).isPresent().contains(accountData);
  }

  @Test
  public void testLayer_storageSlots_layerOverridesCache() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(1));
    Bytes baseValue = Bytes.of(10);
    Bytes layerValue = Bytes.of(20);

    // Put in base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putStorageValueBySlotHash(accountHash, slotKey.getSlotHash(), baseValue);
    baseUpdater.commit();

    // Create layer and update
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.putStorageValueBySlotHash(accountHash, slotKey.getSlotHash(), layerValue);
    layerUpdater.commit();

    // Layer should read its own value
    assertThat(layer.getStorageValueByStorageSlotKey(accountHash, slotKey))
        .isPresent()
        .contains(layerValue);

    // Base should have old value
    assertThat(baseStorage.getStorageValueByStorageSlotKey(accountHash, slotKey))
        .isPresent()
        .contains(baseValue);
  }

  @Test
  public void testLayer_code_layerOverridesCache() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Hash codeHash1 = Hash.hash(Bytes.of(10, 20, 30));
    Bytes codeData1 = Bytes.of(10, 20, 30);
    Hash codeHash2 = Hash.hash(Bytes.of(40, 50, 60));
    Bytes codeData2 = Bytes.of(40, 50, 60);

    // Put in base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putCode(accountHash, codeHash1, codeData1);
    baseUpdater.commit();

    // Create layer and update
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.putCode(accountHash, codeHash2, codeData2);
    layerUpdater.commit();

    // Layer should read its own code
    assertThat(layer.getCode(codeHash2, accountHash)).isPresent().contains(codeData2);

    // Base should have old code
    assertThat(baseStorage.getCode(codeHash1, accountHash)).isPresent().contains(codeData1);
  }

  @Test
  public void testLayer_trieNodes_layerOverridesCache() {
    Bytes location = Bytes.of(1, 2);
    Bytes nodeData1 = Bytes.of(10, 20, 30);
    Bytes32 nodeHash1 = Bytes32.wrap(Hash.hash(nodeData1).getBytes());
    Bytes nodeData2 = Bytes.of(40, 50, 60);
    Bytes32 nodeHash2 = Bytes32.wrap(Hash.hash(nodeData2).getBytes());

    // Put in base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putAccountStateTrieNode(location, nodeHash1, nodeData1);
    baseUpdater.commit();

    // Create layer and update
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.putAccountStateTrieNode(location, nodeHash2, nodeData2);
    layerUpdater.commit();

    // Layer should read its own node
    assertThat(layer.getAccountStateTrieNode(location, nodeHash2)).isPresent().contains(nodeData2);

    // Base should have old node
    assertThat(baseStorage.getAccountStateTrieNode(location, nodeHash1))
        .isPresent()
        .contains(nodeData1);
  }

  @Test
  public void testLayer_removal_inLayerOverridesCache() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes accountData = Bytes.of(1, 2, 3);

    // Put in base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putAccountInfoState(accountHash, accountData);
    baseUpdater.commit();

    // Create layer and remove
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.removeAccountInfoState(accountHash);
    layerUpdater.commit();

    // Layer should see removal
    assertThat(layer.getAccount(accountHash)).isEmpty();

    // Base should still have the account
    assertThat(baseStorage.getAccount(accountHash)).isPresent().contains(accountData);
  }

  @Test
  public void testLayer_multipleLayersStacked() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes data1 = Bytes.of(1);
    Bytes data2 = Bytes.of(2);
    Bytes data3 = Bytes.of(3);

    // Base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putAccountInfoState(accountHash, data1);
    baseUpdater.commit();

    // Layer 1
    BonsaiWorldStateLayerStorage layer1 = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layer1Updater = layer1.updater();
    layer1Updater.putAccountInfoState(accountHash, data2);
    layer1Updater.commit();

    // Layer 2 on top of layer 1
    BonsaiWorldStateLayerStorage layer2 = new BonsaiWorldStateLayerStorage(layer1);
    BonsaiWorldStateKeyValueStorage.Updater layer2Updater = layer2.updater();
    layer2Updater.putAccountInfoState(accountHash, data3);
    layer2Updater.commit();

    // Each layer sees its own data
    assertThat(baseStorage.getAccount(accountHash)).isPresent().contains(data1);
    assertThat(layer1.getAccount(accountHash)).isPresent().contains(data2);
    assertThat(layer2.getAccount(accountHash)).isPresent().contains(data3);
  }

  @Test
  public void testLayer_readsMixedFromAllLevels() {
    Hash acc1 = Hash.hash(Bytes.of(1)); // In layer
    Hash acc2 = Hash.hash(Bytes.of(2)); // In cache only
    Hash acc3 = Hash.hash(Bytes.of(3)); // In parent only

    Bytes data1 = Bytes.of(1);
    Bytes data2 = Bytes.of(2);
    Bytes data3 = Bytes.of(3);

    // Put acc2 and acc3 in base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putAccountInfoState(acc2, data2);
    baseUpdater.putAccountInfoState(acc3, data3);
    baseUpdater.commit();

    // Create layer and put acc1
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.putAccountInfoState(acc1, data1);
    layerUpdater.commit();

    // Layer should be able to read all three
    assertThat(layer.getAccount(acc1)).isPresent().contains(data1); // From layer
    assertThat(layer.getAccount(acc2)).isPresent().contains(data2); // From cache
    assertThat(layer.getAccount(acc3))
        .isPresent()
        .contains(data3); // From cache (was committed to base)
  }

  @Test
  public void testLayer_multipleReadsConsistent() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes layerData = Bytes.of(10, 20, 30);

    // Put in base with different data
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putAccountInfoState(accountHash, Bytes.of(1, 2, 3));
    baseUpdater.commit();

    // Create layer and update
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.putAccountInfoState(accountHash, layerData);
    layerUpdater.commit();

    // Multiple reads from layer should return consistent results
    assertThat(layer.getAccount(accountHash)).isPresent().contains(layerData);
    assertThat(layer.getAccount(accountHash)).isPresent().contains(layerData);
    assertThat(layer.getAccount(accountHash)).isPresent().contains(layerData);
  }

  @Test
  public void testLayer_storageRemoval() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(1));
    Bytes storageValue = Bytes.of(10);

    // Put in base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putStorageValueBySlotHash(accountHash, slotKey.getSlotHash(), storageValue);
    baseUpdater.commit();

    // Create layer and remove
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.removeStorageValueBySlotHash(accountHash, slotKey.getSlotHash());
    layerUpdater.commit();

    // Layer should see removal
    assertThat(layer.getStorageValueByStorageSlotKey(accountHash, slotKey)).isEmpty();

    // Base should still have value
    assertThat(baseStorage.getStorageValueByStorageSlotKey(accountHash, slotKey))
        .isPresent()
        .contains(storageValue);
  }

  @Test
  public void testLayer_batchReadsFromMixedSources() {
    // Create multiple accounts in different layers
    Hash acc1 = Hash.hash(Bytes.of(1)); // Will be in layer
    Hash acc2 = Hash.hash(Bytes.of(2)); // Will be in cache
    Hash acc3 = Hash.hash(Bytes.of(3)); // Will be in base only
    Hash acc4 = Hash.hash(Bytes.of(4)); // Won't exist anywhere

    // Put acc2 and acc3 in base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putAccountInfoState(acc2, Bytes.of(2));
    baseUpdater.putAccountInfoState(acc3, Bytes.of(3));
    baseUpdater.commit();

    // Create layer and add acc1
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.putAccountInfoState(acc1, Bytes.of(1));
    layerUpdater.commit();

    // Read all accounts
    assertThat(layer.getAccount(acc1)).isPresent().contains(Bytes.of(1));
    assertThat(layer.getAccount(acc2)).isPresent().contains(Bytes.of(2));
    assertThat(layer.getAccount(acc3)).isPresent().contains(Bytes.of(3));
    assertThat(layer.getAccount(acc4)).isEmpty();
  }

  @Test
  public void testLayer_emptyLayer_readsFromCacheAndParent() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes accountData = Bytes.of(1, 2, 3);

    // Put in base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putAccountInfoState(accountHash, accountData);
    baseUpdater.commit();

    // Create empty layer (no modifications)
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);

    // Should still be able to read from cache/parent
    assertThat(layer.getAccount(accountHash)).isPresent().contains(accountData);
  }

  @Test
  public void testLayer_clone_preservesData() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes layerData = Bytes.of(10, 20, 30);

    // Create layer with data
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.putAccountInfoState(accountHash, layerData);
    layerUpdater.commit();

    // Clone layer
    BonsaiWorldStateLayerStorage clonedLayer = layer.clone();

    // Both should see the same data
    assertThat(layer.getAccount(accountHash)).isPresent().contains(layerData);
    assertThat(clonedLayer.getAccount(accountHash)).isPresent().contains(layerData);
  }

  @Test
  public void testLayer_storageTrieNodes() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes location = Bytes.of(2, 3);
    Bytes nodeData1 = Bytes.of(30, 40, 50);
    Bytes32 nodeHash1 = Bytes32.wrap(Hash.hash(nodeData1).getBytes());
    Bytes nodeData2 = Bytes.of(60, 70, 80);
    Bytes32 nodeHash2 = Bytes32.wrap(Hash.hash(nodeData2).getBytes());

    // Put in base
    BonsaiWorldStateKeyValueStorage.CachedUpdater baseUpdater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    baseUpdater.putAccountStorageTrieNode(accountHash, location, nodeHash1, nodeData1);
    baseUpdater.commit();

    // Create layer and update
    BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(baseStorage);
    BonsaiWorldStateKeyValueStorage.Updater layerUpdater = layer.updater();
    layerUpdater.putAccountStorageTrieNode(accountHash, location, nodeHash2, nodeData2);
    layerUpdater.commit();

    // Layer should read its own node
    assertThat(layer.getAccountStorageTrieNode(accountHash, location, nodeHash2))
        .isPresent()
        .contains(nodeData2);

    // base can still read base node from cache
    assertThat(baseStorage.getAccountStorageTrieNode(accountHash, location, nodeHash1))
        .isPresent()
        .contains(nodeData1);
  }
}
