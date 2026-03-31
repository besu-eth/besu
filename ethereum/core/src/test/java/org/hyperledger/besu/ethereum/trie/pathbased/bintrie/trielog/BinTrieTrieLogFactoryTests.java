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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.trielog;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.trie.common.BinaryStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogFactory;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BinTrieTrieLogFactoryTests {

  final BlockchainSetupUtil setup = BlockchainSetupUtil.forTesting(DataStorageFormat.BINTRIE);

  final Address accountFixture = Address.fromHexString("0xdeadbeef");

  final BlockHeader headerFixture =
      new BlockHeaderTestFixture()
          .parentHash(setup.getGenesisState().getBlock().getHash())
          .coinbase(Address.ZERO)
          .buildHeader();

  final TrieLogLayer trieLogFixture =
      new TrieLogLayer()
          .setBlockHash(headerFixture.getBlockHash())
          .addAccountChange(
              accountFixture,
              null,
              new BinaryStateTrieAccountValue(0, Wei.fromEth(1), Hash.EMPTY, 0))
          .addCodeChange(
              Address.ZERO,
              null,
              Bytes.fromHexString("0xfeeddeadbeef"),
              headerFixture.getBlockHash())
          .addStorageChange(Address.ZERO, new StorageSlotKey(UInt256.ZERO), null, UInt256.ONE);

  @Test
  public void testSerializeDeserialize() {
    TrieLogFactory factory = new BinTrieTrieLogFactoryImpl();
    byte[] rlp = factory.serialize(trieLogFixture);

    TrieLog layer = factory.deserialize(rlp);
    // Verify key properties match
    assertThat(layer.getBlockHash()).isEqualTo(trieLogFixture.getBlockHash());
    assertThat(layer.getAccountChanges().size())
        .isEqualTo(trieLogFixture.getAccountChanges().size());
    assertThat(layer.getCodeChanges().size()).isEqualTo(trieLogFixture.getCodeChanges().size());
    assertThat(layer.getStorageChanges().size())
        .isEqualTo(trieLogFixture.getStorageChanges().size());
  }

  @Test
  public void testSerializeDeserializeWithMultipleAccounts() {
    final Address accountTwo = Address.fromHexString("0xbeefdead");

    TrieLogLayer complexTrieLog =
        new TrieLogLayer()
            .setBlockHash(headerFixture.getBlockHash())
            .addAccountChange(
                accountFixture,
                null,
                new BinaryStateTrieAccountValue(1, Wei.fromEth(10), Hash.EMPTY, 0))
            .addAccountChange(
                accountTwo,
                null,
                new BinaryStateTrieAccountValue(0, Wei.fromEth(5), Hash.EMPTY, 100))
            .addCodeChange(
                accountFixture,
                null,
                Bytes.fromHexString("0x6080604052"),
                headerFixture.getBlockHash())
            .addStorageChange(
                accountFixture, new StorageSlotKey(UInt256.ONE), null, UInt256.valueOf(42))
            .addStorageChange(
                accountFixture,
                new StorageSlotKey(UInt256.valueOf(2)),
                UInt256.ONE,
                UInt256.valueOf(100));

    TrieLogFactory factory = new BinTrieTrieLogFactoryImpl();
    byte[] rlp = factory.serialize(complexTrieLog);

    TrieLog layer = factory.deserialize(rlp);
    assertThat(layer.getBlockHash()).isEqualTo(complexTrieLog.getBlockHash());
    assertThat(layer.getAccountChanges().size()).isEqualTo(2);
    assertThat(layer.getCodeChanges().size()).isEqualTo(1);
    assertThat(layer.getStorageChanges().size()).isEqualTo(1); // One address has storage changes
  }

  @Test
  public void testSerializeDeserializeWithAccountUpdate() {
    BinaryStateTrieAccountValue oldValue =
        new BinaryStateTrieAccountValue(0, Wei.fromEth(1), Hash.EMPTY, 0);
    BinaryStateTrieAccountValue newValue =
        new BinaryStateTrieAccountValue(1, Wei.fromEth(2), Hash.EMPTY, 100);

    TrieLogLayer updateTrieLog =
        new TrieLogLayer()
            .setBlockHash(headerFixture.getBlockHash())
            .addAccountChange(accountFixture, oldValue, newValue);

    TrieLogFactory factory = new BinTrieTrieLogFactoryImpl();
    byte[] rlp = factory.serialize(updateTrieLog);

    TrieLog layer = factory.deserialize(rlp);
    assertThat(layer.getBlockHash()).isEqualTo(updateTrieLog.getBlockHash());
    assertThat(layer.getAccountChanges().size()).isEqualTo(1);
  }

  @Test
  public void testSerializeDeserializeWithAccountDeletion() {
    BinaryStateTrieAccountValue oldValue =
        new BinaryStateTrieAccountValue(5, Wei.fromEth(100), Hash.EMPTY, 1000);

    TrieLogLayer deleteTrieLog =
        new TrieLogLayer()
            .setBlockHash(headerFixture.getBlockHash())
            .addAccountChange(accountFixture, oldValue, null);

    TrieLogFactory factory = new BinTrieTrieLogFactoryImpl();
    byte[] rlp = factory.serialize(deleteTrieLog);

    TrieLog layer = factory.deserialize(rlp);
    assertThat(layer.getBlockHash()).isEqualTo(deleteTrieLog.getBlockHash());
    assertThat(layer.getAccountChanges().size()).isEqualTo(1);
  }

  @Test
  public void testSerializeDeserializeWithStorageUpdates() {
    TrieLogLayer storageTrieLog =
        new TrieLogLayer()
            .setBlockHash(headerFixture.getBlockHash())
            .addStorageChange(
                accountFixture, new StorageSlotKey(UInt256.ONE), UInt256.ZERO, UInt256.valueOf(42))
            .addStorageChange(
                accountFixture,
                new StorageSlotKey(UInt256.valueOf(100)),
                UInt256.valueOf(50),
                UInt256.ZERO);

    TrieLogFactory factory = new BinTrieTrieLogFactoryImpl();
    byte[] rlp = factory.serialize(storageTrieLog);

    TrieLog layer = factory.deserialize(rlp);
    assertThat(layer.getBlockHash()).isEqualTo(storageTrieLog.getBlockHash());
    assertThat(layer.getStorageChanges().size()).isEqualTo(1); // One address
    assertThat(layer.getStorageChanges().get(accountFixture).size()).isEqualTo(2); // Two slots
  }
}
