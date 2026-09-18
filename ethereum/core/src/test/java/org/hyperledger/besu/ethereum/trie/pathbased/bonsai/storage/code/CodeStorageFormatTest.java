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
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class CodeStorageFormatTest {

  private static final Bytes CODE = Bytes.fromHexString("0x605b5b615b5b5b");
  private static final List<Bytes> CODES =
      List.of(CODE, Bytes.of(0x5b), Bytes.fromHexString("0x60005b"));

  @Test
  void roundTripsCode() {
    final byte[] value = CodeStorageFormat.encode(CODE);

    assertThat(value[0]).isEqualTo((byte) 1);
    assertThat(CodeStorageFormat.decode(value)).isEqualTo(CODE);
  }

  @Test
  void rejectsLegacyValues() {
    assertThatThrownBy(() -> CodeStorageFormat.decode(CODE.toArrayUnsafe()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> CodeStorageFormat.decode(new byte[0]))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void marksAnEmptyColumnFamilyWithoutRewritingAnything() {
    final SegmentedKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();

    CodeStorageFormat.migrate(storage);

    assertThat(storage.get(CODE_STORAGE, CodeStorageFormat.FORMAT_KEY)).isPresent();
    assertThat(storage.get(CODE_STORAGE, CodeStorageFormat.MIGRATION_KEY)).isEmpty();
  }

  @Test
  void migratesLegacyEntriesOnce() {
    final SegmentedKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    final SegmentedKeyValueStorageTransaction setup = storage.startTransaction();
    for (final Bytes code : CODES) {
      setup.put(CODE_STORAGE, keyOf(code), code.toArrayUnsafe());
    }
    setup.commit();

    CodeStorageFormat.migrate(storage);
    assertMigrated(storage);

    // a second run finds the marker and leaves the entries alone
    CodeStorageFormat.migrate(storage);
    assertMigrated(storage);
  }

  @Test
  void resumesAnInterruptedMigrationAfterTheLastMigratedKey() {
    final SegmentedKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    final List<Bytes> inKeyOrder =
        CODES.stream()
            .sorted((a, b) -> Bytes.wrap(keyOf(a)).compareTo(Bytes.wrap(keyOf(b))))
            .toList();
    // the first two entries were rewritten and committed before the interruption
    final SegmentedKeyValueStorageTransaction setup = storage.startTransaction();
    for (int i = 0; i < inKeyOrder.size(); i++) {
      final Bytes code = inKeyOrder.get(i);
      setup.put(
          CODE_STORAGE, keyOf(code), i < 2 ? CodeStorageFormat.encode(code) : code.toArrayUnsafe());
    }
    setup.put(CODE_STORAGE, CodeStorageFormat.MIGRATION_KEY, keyOf(inKeyOrder.get(1)));
    setup.commit();

    CodeStorageFormat.migrate(storage);

    assertMigrated(storage);
  }

  @Test
  void reservedKeysAreNotCode() {
    assertThat(CodeStorageFormat.isReservedKey(CodeStorageFormat.FORMAT_KEY)).isTrue();
    assertThat(CodeStorageFormat.isReservedKey(CodeStorageFormat.MIGRATION_KEY)).isTrue();
    assertThat(CodeStorageFormat.isReservedKey(keyOf(CODE))).isFalse();
  }

  private static void assertMigrated(final SegmentedKeyValueStorage storage) {
    for (final Bytes code : CODES) {
      final byte[] value = storage.get(CODE_STORAGE, keyOf(code)).orElseThrow();
      assertThat(CodeStorageFormat.decode(value)).isEqualTo(code);
    }
    assertThat(storage.get(CODE_STORAGE, CodeStorageFormat.FORMAT_KEY)).isPresent();
    assertThat(storage.get(CODE_STORAGE, CodeStorageFormat.MIGRATION_KEY)).isEmpty();
  }

  private static byte[] keyOf(final Bytes code) {
    return Hash.hash(code).getBytes().toArrayUnsafe();
  }
}
