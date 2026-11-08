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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.common.MptStorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;

class AccountConsumingMapTest {

  private static final Address ACCOUNT =
      Address.fromHexString("0x1000000000000000000000000000000000000001");

  private final List<Address> preloaded = new ArrayList<>();
  private final AccountConsumingMap<BonsaiValue<BonsaiAccount>> accounts =
      new AccountConsumingMap<>(
          new ConcurrentHashMap<>(), (address, value) -> preloaded.add(address));

  @Test
  void preloadsAChangedAccountOnce() {
    accounts.put(ACCOUNT, new BonsaiValue<>(account(1L), account(1L)));
    assertThat(preloaded).isEmpty();

    accounts.put(ACCOUNT, new BonsaiValue<>(account(1L), account(2L)));
    accounts.put(ACCOUNT, new BonsaiValue<>(account(1L), account(3L)));

    assertThat(preloaded).containsExactly(ACCOUNT);
  }

  @Test
  void preloadsAnAccountPutBackAfterItsRemoval() {
    accounts.put(ACCOUNT, new BonsaiValue<>(null, account(1L)));
    accounts.remove(ACCOUNT);
    accounts.put(ACCOUNT, new BonsaiValue<>(null, account(2L)));

    assertThat(preloaded).containsExactly(ACCOUNT, ACCOUNT);
  }

  @Test
  void preloadIfChanged_preloadsAnAccountChangedInPlaceOnce() {
    final BonsaiValue<BonsaiAccount> value = new BonsaiValue<>(account(1L), account(1L));
    accounts.put(ACCOUNT, value);
    accounts.preloadIfChanged(ACCOUNT);
    assertThat(preloaded).isEmpty();

    value.getUpdated().setNonce(2L);
    accounts.preloadIfChanged(ACCOUNT);
    accounts.preloadIfChanged(ACCOUNT);

    assertThat(preloaded).containsExactly(ACCOUNT);
  }

  @Test
  void preload_preloadsAnUnchangedAccountOnce() {
    accounts.put(ACCOUNT, new BonsaiValue<>(account(1L), account(1L)));

    accounts.preload(ACCOUNT);
    accounts.preload(ACCOUNT);
    accounts.put(ACCOUNT, new BonsaiValue<>(account(1L), account(2L)));

    assertThat(preloaded).containsExactly(ACCOUNT);
  }

  private static BonsaiAccount account(final long nonce) {
    return new BonsaiAccount(
        null,
        ACCOUNT,
        ACCOUNT.addressHash(),
        nonce,
        Wei.ONE,
        new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH),
        Hash.EMPTY,
        true,
        new BonsaiCodeCache());
  }
}
