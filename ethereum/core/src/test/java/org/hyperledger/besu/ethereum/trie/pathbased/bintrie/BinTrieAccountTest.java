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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview.BinTrieWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BinTrieAccountTest {

  @Mock BinTrieWorldState binTrieWorldState;

  @Test
  void shouldCopyBinTrieAccountCorrectly() {
    final BinTrieAccount account =
        new BinTrieAccount(
            binTrieWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ONE,
            Hash.EMPTY,
            0L,
            true,
            new CodeCache());
    account.setCode(Bytes.of(1));
    account.setStorageValue(UInt256.ONE, UInt256.ONE);
    assertThat(new BinTrieAccount(account, binTrieWorldState, true))
        .usingRecursiveComparison()
        .ignoringFields("context")
        .isEqualTo(account);
  }

  @Test
  void shouldSerializeAndDeserializeAccount() {
    final BinTrieAccount account =
        new BinTrieAccount(
            binTrieWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            42,
            Wei.of(1000),
            Hash.hash(Bytes.of(1, 2, 3)),
            100L,
            true,
            new CodeCache());

    final Bytes serialized = account.serializeAccount();
    final BinTrieAccount deserialized =
        BinTrieAccount.fromRlp(binTrieWorldState, Address.ZERO, serialized, true);

    assertThat(deserialized.getNonce()).isEqualTo(account.getNonce());
    assertThat(deserialized.getBalance()).isEqualTo(account.getBalance());
    assertThat(deserialized.getCodeHash()).isEqualTo(account.getCodeHash());
    assertThat(deserialized.getCodeSize()).isEqualTo(account.getCodeSize());
  }

  @Test
  void shouldHandleZeroValues() {
    final BinTrieAccount account =
        new BinTrieAccount(
            binTrieWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ZERO,
            Hash.EMPTY,
            0L,
            true,
            new CodeCache());

    final Bytes serialized = account.serializeAccount();
    final BinTrieAccount deserialized =
        BinTrieAccount.fromRlp(binTrieWorldState, Address.ZERO, serialized, true);

    assertThat(deserialized.getNonce()).isEqualTo(0);
    assertThat(deserialized.getBalance()).isEqualTo(Wei.ZERO);
    assertThat(deserialized.getCodeHash()).isEqualTo(Hash.EMPTY);
    assertThat(deserialized.getCodeSize()).contains(0L);
  }

  @Test
  void shouldHandleLargeValues() {
    final BinTrieAccount account =
        new BinTrieAccount(
            binTrieWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            Long.MAX_VALUE,
            Wei.of(UInt256.MAX_VALUE),
            Hash.hash(Bytes.random(32)),
            Long.MAX_VALUE,
            true,
            new CodeCache());

    final Bytes serialized = account.serializeAccount();
    final BinTrieAccount deserialized =
        BinTrieAccount.fromRlp(binTrieWorldState, Address.ZERO, serialized, true);

    assertThat(deserialized.getNonce()).isEqualTo(account.getNonce());
    assertThat(deserialized.getBalance()).isEqualTo(account.getBalance());
    assertThat(deserialized.getCodeHash()).isEqualTo(account.getCodeHash());
    assertThat(deserialized.getCodeSize()).isEqualTo(account.getCodeSize());
  }

  @Test
  void shouldUpdateStorageValues() {
    final BinTrieAccount account =
        new BinTrieAccount(
            binTrieWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ONE,
            Hash.EMPTY,
            0L,
            true,
            new CodeCache());

    account.setStorageValue(UInt256.ONE, UInt256.valueOf(100));
    account.setStorageValue(UInt256.valueOf(2), UInt256.valueOf(200));

    // Verify the storage values were set
    assertThat(account.getUpdatedStorage()).containsKey(UInt256.ONE);
    assertThat(account.getUpdatedStorage()).containsKey(UInt256.valueOf(2));
  }

  @Test
  void shouldSetAndGetCode() {
    final BinTrieAccount account =
        new BinTrieAccount(
            binTrieWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ONE,
            Hash.EMPTY,
            0L,
            true,
            new CodeCache());

    final Bytes code = Bytes.of(0x60, 0x00, 0x60, 0x00, 0xF3);
    account.setCode(code);

    assertThat(account.getCode()).isEqualTo(code);
    assertThat(account.getCodeHash()).isEqualTo(Hash.hash(code));
  }

  @Test
  void shouldSetAndGetBalance() {
    final BinTrieAccount account =
        new BinTrieAccount(
            binTrieWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ONE,
            Hash.EMPTY,
            0L,
            true,
            new CodeCache());

    account.setBalance(Wei.of(12345));
    assertThat(account.getBalance()).isEqualTo(Wei.of(12345));
  }

  @Test
  void shouldSetAndGetNonce() {
    final BinTrieAccount account =
        new BinTrieAccount(
            binTrieWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ONE,
            Hash.EMPTY,
            0L,
            true,
            new CodeCache());

    account.setNonce(42);
    assertThat(account.getNonce()).isEqualTo(42);
  }

  @Test
  void shouldReturnCorrectAddressHash() {
    final Address address = Address.fromHexString("0x1234567890123456789012345678901234567890");
    final BinTrieAccount account =
        new BinTrieAccount(
            binTrieWorldState,
            address,
            address.addressHash(),
            0,
            Wei.ONE,
            Hash.EMPTY,
            0L,
            true,
            new CodeCache());

    assertThat(account.getAddressHash()).isEqualTo(address.addressHash());
    assertThat(account.getAddress()).isEqualTo(address);
  }

  @Test
  void shouldReturnEmptyTrieHashForStorageRoot() {
    final BinTrieAccount account =
        new BinTrieAccount(
            binTrieWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ONE,
            Hash.EMPTY,
            0L,
            true,
            new CodeCache());

    assertThat(account.getStorageRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
  }
}
