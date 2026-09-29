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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Arrays;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class JumpDestCodeStorageStrategyTest {

  // PUSH1 0x5b; JUMPDEST; PUSH2 0x5b5b; JUMPDEST: only the two instructions are destinations
  private static final Bytes CODE = Bytes.fromHexString("0x60 5b 5b 61 5b5b 5b".replace(" ", ""));

  private final JumpDestCodeStorageStrategy strategy = new JumpDestCodeStorageStrategy();

  @Test
  void roundTripsCodeAndAnalysis() {
    final Code stored = roundTrip(CODE);

    assertThat(stored.getBytes()).isEqualTo(CODE);
    assertThat(stored.getJumpDestBitMask()).isEqualTo(Code.jumpDestBitMaskOf(CODE));
    assertThat(stored.getJumpDestBitMask()).containsExactly(0b1000100L);
  }

  @Test
  void roundTripsCodeSpanningManyEntries() {
    final byte[] raw = new byte[24576];
    new Random(42).nextBytes(raw);
    final Bytes code = Bytes.wrap(raw);

    final Code stored = roundTrip(code);

    assertThat(stored.getBytes()).isEqualTo(code);
    assertThat(stored.getJumpDestBitMask()).isEqualTo(Code.jumpDestBitMaskOf(code));
  }

  @Test
  void storedAnalysisIsUsedAsIs() {
    final Code code = roundTrip(CODE);

    assertThat(code.getJumpDestBitMask()).isNotNull();
    assertThat(code.isJumpDestInvalid(2)).isFalse();
    assertThat(code.isJumpDestInvalid(1)).isTrue();
    assertThat(code.isJumpDestInvalid(4)).isTrue();
    assertThat(code.isJumpDestInvalid(6)).isFalse();
  }

  @Test
  void rejectsValuesOfAnotherLayout() {
    assertThatThrownBy(() -> JumpDestCodeStorageStrategy.decode(CODE.toArrayUnsafe(), null))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> JumpDestCodeStorageStrategy.decode(new byte[] {0x5b}, null))
        .isInstanceOf(IllegalStateException.class);
    final byte[] truncated = Arrays.copyOf(JumpDestCodeStorageStrategy.encode(CODE), 10);
    assertThatThrownBy(() -> JumpDestCodeStorageStrategy.decode(truncated, null))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void storesAndLoadsCodeByItsHash() {
    final SegmentedKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    final SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
    strategy.putFlatCode(storage, transaction, Hash.EMPTY, Hash.hash(CODE), CODE);
    transaction.commit();

    assertThat(storage.get(CODE_STORAGE, Hash.hash(CODE).getBytes().toArrayUnsafe()))
        .contains(JumpDestCodeStorageStrategy.encode(CODE));
    final Code loaded = strategy.getFlatCode(Hash.hash(CODE), Hash.EMPTY, storage).orElseThrow();
    assertThat(loaded.getBytes()).isEqualTo(CODE);
    assertThat(loaded.getCodeHash()).isEqualTo(Hash.hash(CODE));
    assertThat(loaded.getJumpDestBitMask()).containsExactly(0b1000100L);
  }

  @Test
  void marksAnEmptyColumnFamily() {
    final SegmentedKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    assertThat(JumpDestCodeStorageStrategy.isMarked(storage)).isFalse();

    strategy.markEmpty(storage);

    assertThat(JumpDestCodeStorageStrategy.isMarked(storage)).isTrue();
    assertThat(storage.get(CODE_STORAGE, JumpDestCodeStorageStrategy.MARKER_KEY))
        .contains(JumpDestCodeStorageStrategy.MARKER);
    assertThat(JumpDestCodeStorageStrategy.isMarkerKey(JumpDestCodeStorageStrategy.MARKER_KEY))
        .isTrue();
    assertThat(JumpDestCodeStorageStrategy.isMarkerKey(Hash.hash(CODE).getBytes().toArrayUnsafe()))
        .isFalse();
  }

  @Test
  void rejectsAMarkerOfAnUnknownStrategy() {
    final SegmentedKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    final SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
    transaction.put(CODE_STORAGE, JumpDestCodeStorageStrategy.MARKER_KEY, new byte[] {1});
    transaction.commit();

    assertThatThrownBy(() -> JumpDestCodeStorageStrategy.isMarked(storage))
        .isInstanceOf(IllegalStateException.class);
  }

  private static Code roundTrip(final Bytes code) {
    return JumpDestCodeStorageStrategy.decode(
        JumpDestCodeStorageStrategy.encode(code), Hash.hash(code));
  }
}
