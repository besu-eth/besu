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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldView;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class BonsaiAccountTest {

  @Mock BonsaiWorldState bonsaiWorldState;

  @Test
  void shouldCopyTrackedBonsaiAccountCorrectly() {
    final BonsaiAccount trackedAccount =
        new BonsaiAccount(
            bonsaiWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ONE,
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY,
            true,
            new BonsaiCodeCache());
    trackedAccount.setCode(Bytes.of(1));
    final UpdateTrackingAccount<BonsaiAccount> bonsaiAccountUpdateTrackingAccount =
        new UpdateTrackingAccount<>(trackedAccount);
    bonsaiAccountUpdateTrackingAccount.setStorageValue(UInt256.ONE, UInt256.ONE);

    final BonsaiAccount expectedAccount = new BonsaiAccount(trackedAccount, bonsaiWorldState, true);
    expectedAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    assertThat(
            new BonsaiAccount(
                bonsaiWorldState,
                bonsaiAccountUpdateTrackingAccount,
                trackedAccount.getCodeCache()))
        .isEqualToComparingFieldByField(expectedAccount);
  }

  @Test
  void shouldCopyBonsaiAccountCorrectly() {
    final BonsaiAccount account =
        new BonsaiAccount(
            bonsaiWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ONE,
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY,
            true,
            new BonsaiCodeCache());
    account.setCode(Bytes.of(1));
    account.setStorageValue(UInt256.ONE, UInt256.ONE);
    assertThat(new BonsaiAccount(account, bonsaiWorldState, true))
        .isEqualToComparingFieldByField(account);
  }

  @Test
  void unreadableCodeShouldNotPoisonSharedCodeCache() {
    final BonsaiCodeCache codeCache = new BonsaiCodeCache();
    final Bytes bytecode = Bytes.fromHexString("0x5b00");
    final Hash codeHash = Hash.hash(bytecode);

    // a world state closed under a block creation thread that is still executing returns no code
    final BonsaiWorldView closedWorldView = mock(BonsaiWorldView.class);
    when(closedWorldView.getCode(any(), any())).thenReturn(Optional.empty());
    assertThat(contract(closedWorldView, codeHash, codeCache).getOrCreateCachedCode().getBytes())
        .isEqualTo(Bytes.EMPTY);
    assertThat(
            new UpdateTrackingAccount<>(contract(closedWorldView, codeHash, codeCache))
                .getOrCreateCachedCode()
                .getBytes())
        .isEqualTo(Bytes.EMPTY);

    // block import on a live world state must still execute the real code
    final BonsaiWorldView liveWorldView = mock(BonsaiWorldView.class);
    when(liveWorldView.getCode(any(), any())).thenReturn(Optional.of(bytecode));
    assertThat(contract(liveWorldView, codeHash, codeCache).getOrCreateCachedCode().getBytes())
        .isEqualTo(bytecode);
  }

  private static BonsaiAccount contract(
      final BonsaiWorldView worldView, final Hash codeHash, final BonsaiCodeCache codeCache) {
    return new BonsaiAccount(
        worldView,
        Address.ZERO,
        Hash.hash(Address.ZERO.getBytes()),
        0,
        Wei.ZERO,
        Hash.EMPTY_TRIE_HASH,
        codeHash,
        false,
        codeCache);
  }
}
