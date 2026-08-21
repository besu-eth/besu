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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.PatriciaAccountValue;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TrieLogLayerTests {
  private TrieLogLayer trieLogLayer;
  private TrieLogLayer otherTrieLogLayer;

  @BeforeEach
  public void setUp() {
    trieLogLayer = new TrieLogLayer();
    otherTrieLogLayer = new TrieLogLayer();
  }

  @Test
  public void testStorageSlotKeyEquivalence() {
    StorageSlotKey key = new StorageSlotKey(UInt256.ZERO);
    StorageSlotKey otherKey = new StorageSlotKey(UInt256.ZERO);

    Assertions.assertThat(key).isEqualTo(otherKey);
    Assertions.assertThat(key.hashCode()).isEqualTo(otherKey.hashCode());
  }

  @Test
  public void testAddAccountChange() {
    Address address = Address.fromHexString("0x00");
    PatriciaAccountValue oldValue = new PatriciaAccountValue(0, Wei.ZERO, Hash.EMPTY, Hash.EMPTY);
    PatriciaAccountValue newValue =
        new PatriciaAccountValue(1, Wei.fromEth(1), Hash.EMPTY, Hash.EMPTY);

    Address otherAddress = Address.fromHexString("0x000000");
    PatriciaAccountValue otherOldValue =
        new PatriciaAccountValue(0, Wei.ZERO, Hash.EMPTY, Hash.EMPTY);
    PatriciaAccountValue otherNewValue =
        new PatriciaAccountValue(1, Wei.fromEth(1), Hash.EMPTY, Hash.EMPTY);

    trieLogLayer.addAccountChange(address, oldValue, newValue);
    otherTrieLogLayer.addAccountChange(otherAddress, otherOldValue, otherNewValue);

    Assertions.assertThat(trieLogLayer).isEqualTo(otherTrieLogLayer);

    Optional<AccountValue> priorAccount = trieLogLayer.getPriorAccount(address);
    Assertions.assertThat(priorAccount).isPresent();
    Assertions.assertThat(priorAccount.get()).isEqualTo(oldValue);

    Optional<AccountValue> updatedAccount = trieLogLayer.getAccount(address);
    Assertions.assertThat(updatedAccount).isPresent();
    Assertions.assertThat(updatedAccount.get()).isEqualTo(newValue);
  }

  @Test
  public void testAddCodeChange() {
    Address address = Address.fromHexString("0xdeadbeef");
    Bytes oldValue = Bytes.fromHexString("0x00");
    Bytes newValue = Bytes.fromHexString("0x01030307");
    Hash blockHash = Hash.fromHexStringLenient("0xfeedbeef02dead");

    Address otherAddress = Address.fromHexString("0x0000deadbeef");
    Bytes otherOldValue = Bytes.fromHexString("0x00");
    Bytes otherNewValue = Bytes.fromHexString("0x01030307");
    Hash otherBlockHash = Hash.fromHexStringLenient("0x00feedbeef02dead");

    trieLogLayer.addCodeChange(address, oldValue, newValue, blockHash);
    otherTrieLogLayer.addCodeChange(otherAddress, otherOldValue, otherNewValue, otherBlockHash);

    Assertions.assertThat(trieLogLayer).isEqualTo(otherTrieLogLayer);

    Optional<Bytes> priorCode = trieLogLayer.getPriorCode(address);
    Assertions.assertThat(priorCode).isPresent();
    Assertions.assertThat(priorCode.get()).isEqualTo(oldValue);

    Optional<Bytes> updatedCode = trieLogLayer.getCode(address);
    Assertions.assertThat(updatedCode).isPresent();
    Assertions.assertThat(updatedCode.get()).isEqualTo(newValue);
  }

  @Test
  public void testAddStorageChange() {
    Address address = Address.fromHexString("0x00");
    UInt256 oldValue = UInt256.ZERO;
    UInt256 newValue = UInt256.ONE;
    UInt256 slot = UInt256.ONE;
    StorageSlotKey storageSlotKey = new StorageSlotKey(slot);

    Address otherAddress = Address.fromHexString("0x000000");
    UInt256 otherOldValue = UInt256.ZERO;
    UInt256 otherNewValue = UInt256.ONE;
    UInt256 otherSlot = UInt256.ONE;
    StorageSlotKey otherStorageSlotKey = new StorageSlotKey(otherSlot);

    trieLogLayer.addStorageChange(address, storageSlotKey, oldValue, newValue);
    otherTrieLogLayer.addStorageChange(
        otherAddress, otherStorageSlotKey, otherOldValue, otherNewValue);

    Assertions.assertThat(trieLogLayer).isEqualTo(otherTrieLogLayer);

    Optional<UInt256> priorStorageValue =
        trieLogLayer.getPriorStorageByStorageSlotKey(address, storageSlotKey);
    Assertions.assertThat(priorStorageValue).isPresent();
    Assertions.assertThat(priorStorageValue.get()).isEqualTo(oldValue);

    Optional<UInt256> updatedStorageValue =
        trieLogLayer.getStorageByStorageSlotKey(address, storageSlotKey);
    Assertions.assertThat(updatedStorageValue).isPresent();
    Assertions.assertThat(updatedStorageValue.get()).isEqualTo(newValue);
  }
}
