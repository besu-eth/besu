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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.MptStorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.common.StorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class BonsaiAccountStorageRootStrategyTest {

  @Test
  void mptRoundTripProducesFourFieldRlp() {
    final Hash storageRoot = Hash.hash(Bytes.of(1, 2, 3));
    final BonsaiAccount account = mptAccountWithRoot(storageRoot);

    final Bytes encoded = account.serializeAccount();
    final BonsaiAccount decoded =
        BonsaiAccount.fromRLP(null, account.getAddress(), encoded, true, new BonsaiCodeCache());

    assertThat(encoded)
        .isEqualTo(
            RLP.encode(
                new PatriciaTrieAccountValue(1L, Wei.ONE, storageRoot, Hash.EMPTY)::writeTo));
    assertThat(decoded.getNonce()).isEqualTo(1L);
    assertThat(decoded.getBalance()).isEqualTo(Wei.ONE);
    assertThat(decoded.getCodeHash()).isEqualTo(Hash.EMPTY);
    assertThat(decoded.getStorageRoot()).isEqualTo(storageRoot);
  }

  @Test
  void assertStorageRootMatchesMptVsMptEqual() {
    final Hash root = Hash.hash(Bytes.of(1, 2, 3));
    new MptStorageRootStrategy(root).assertStorageRootMatches(mptAccountWithRoot(root), "test");
    // no exception thrown
  }

  @Test
  void assertStorageRootMatchesMptVsMptDifferThrows() {
    final MptStorageRootStrategy a = new MptStorageRootStrategy(Hash.hash(Bytes.of(1)));
    final BonsaiAccount b = mptAccountWithRoot(Hash.hash(Bytes.of(2)));
    assertThatThrownBy(() -> a.assertStorageRootMatches(b, "ctx"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Storage Roots differ");
  }

  @Test
  void accountValueOwnsStrategyMatchingItsEncodedShape() {
    final Hash storageRoot = Hash.hash(Bytes.of(1, 2, 3));
    final PatriciaTrieAccountValue accountValue =
        new PatriciaTrieAccountValue(1L, Wei.ONE, storageRoot, Hash.EMPTY);

    final StorageRootStrategy strategy = accountValue.storageRootStrategy();

    assertThat(strategy).isInstanceOf(MptStorageRootStrategy.class);
    assertThat(strategy.getStorageRoot()).isEqualTo(storageRoot);
    assertThat(strategy).isNotSameAs(accountValue.storageRootStrategy());
  }

  @Test
  void bonsaiAccountStrategyIsCopiedSoAccountsNeverShareARoot() {
    final Hash storageRoot = Hash.hash(Bytes.of(4, 5, 6));
    final BonsaiAccount account = mptAccountWithRoot(storageRoot);

    final StorageRootStrategy strategy = account.storageRootStrategy();
    strategy.setStorageRoot(Hash.EMPTY_TRIE_HASH);

    assertThat(strategy).isNotSameAs(account.getStorageRootStrategy());
    assertThat(account.getStorageRoot()).isEqualTo(storageRoot);
    assertThat(new BonsaiAccount(account).getStorageRootStrategy())
        .isNotSameAs(account.getStorageRootStrategy());
  }

  @Test
  void trieValueEqualsComparesTheStrategyThatWritesTheStorageRoot() {
    final Hash storageRoot = Hash.hash(Bytes.of(7, 8, 9));
    final StorageRootStrategy otherFormat = mock(StorageRootStrategy.class);
    when(otherFormat.getStorageRoot()).thenReturn(storageRoot);

    final BonsaiAccount account = mptAccountWithRoot(storageRoot);

    assertThat(BonsaiAccount.trieValueEquals(account, mptAccountWithRoot(storageRoot))).isTrue();
    assertThat(BonsaiAccount.trieValueEquals(account, accountWith(otherFormat))).isFalse();
  }

  private static BonsaiAccount mptAccountWithRoot(final Hash storageRoot) {
    return accountWith(new MptStorageRootStrategy(storageRoot));
  }

  private static BonsaiAccount accountWith(final StorageRootStrategy storageRootStrategy) {
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000dd");
    return new BonsaiAccount(
        null,
        address,
        address.addressHash(),
        1L,
        Wei.ONE,
        storageRootStrategy,
        Hash.EMPTY,
        true,
        new BonsaiCodeCache());
  }
}
