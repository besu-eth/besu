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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class StateHarnessTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  static Stream<StateHarness> harnesses() {
    return Stream.of(new BonsaiStateHarness(), new ForestStateHarness());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("harnesses")
  void accountWithStorageAndCodeRoundTrips(final StateHarness h) {
    final UInt256 slotKey = UInt256.valueOf(7);
    h.seedAccount(ALICE, 3L, Wei.of(100), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    h.seedStorageSlot(ALICE, slotKey, UInt256.valueOf(42));
    h.seedCode(ALICE, org.apache.tuweni.bytes.Bytes.fromHexString("0x6001"));

    final Optional<PmtStateTrieAccountValue> account = h.readAccount(ALICE);
    assertThat(account).isPresent();
    assertThat(account.get().getNonce()).isEqualTo(3L);
    assertThat(account.get().getBalance()).isEqualTo(Wei.of(100));
    assertThat(h.readStorageSlot(ALICE, slotKey)).hasValue(UInt256.valueOf(42));
    assertThat(h.readCode(ALICE)).hasValue(org.apache.tuweni.bytes.Bytes.fromHexString("0x6001"));
  }
}
