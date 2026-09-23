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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class CodeStorageMigrationTest {

  private static final Bytes CODE = Bytes.fromHexString("0x605b5b615b5b5b");

  @Test
  void marksAnEmptyColumnFamilyWithoutRewritingAnything() {
    final SegmentedKeyValueStorage storage = storage();

    CodeStorageMigration.migrate(storage);

    assertThat(storage.get(CODE_STORAGE, CodeStorageMigration.FORMAT_KEY)).isPresent();
    assertThat(storage.get(CODE_STORAGE, CodeStorageMigration.MIGRATION_KEY)).isEmpty();
  }

  @Test
  void migratesLegacyEntriesOnce() {
    final SegmentedKeyValueStorage storage = storage();
    final List<Bytes> codes = List.of(CODE, Bytes.of(0x5b), Bytes.fromHexString("0x60005b"));
    final SegmentedKeyValueStorageTransaction setup = storage.startTransaction();
    for (final Bytes code : codes) {
      setup.put(CODE_STORAGE, Hash.hash(code).getBytes().toArrayUnsafe(), code.toArrayUnsafe());
    }
    setup.commit();

    CodeStorageMigration.migrate(storage);
    assertMigrated(storage, codes);

    // a second run finds the marker and leaves the entries alone
    CodeStorageMigration.migrate(storage);
    assertMigrated(storage, codes);
  }

  @Test
  void resumesAnInterruptedMigrationAfterTheLastMigratedKey() {
    final SegmentedKeyValueStorage storage = storage();
    final List<Bytes> codes = List.of(CODE, Bytes.of(0x5b), Bytes.fromHexString("0x60005b"));
    final List<byte[]> keys =
        codes.stream()
            .<byte[]>map(code -> Hash.hash(code).getBytes().toArrayUnsafe())
            .sorted((a, b) -> Bytes.wrap(a).compareTo(Bytes.wrap(b)))
            .toList();
    // the first two keys were rewritten and committed before the interruption
    final SegmentedKeyValueStorageTransaction setup = storage.startTransaction();
    for (int i = 0; i < keys.size(); i++) {
      final Bytes code = codeFor(codes, keys.get(i));
      setup.put(
          CODE_STORAGE,
          keys.get(i),
          i < 2 ? CodeStorageFormat.CURRENT.encode(code) : code.toArrayUnsafe());
    }
    setup.put(CODE_STORAGE, CodeStorageMigration.MIGRATION_KEY, keys.get(1));
    setup.commit();

    CodeStorageMigration.migrate(storage);

    assertMigrated(storage, codes);
  }

  @Test
  void reservedKeysAreNotCode() {
    assertThat(CodeStorageMigration.isReservedKey(CodeStorageMigration.FORMAT_KEY)).isTrue();
    assertThat(CodeStorageMigration.isReservedKey(CodeStorageMigration.MIGRATION_KEY)).isTrue();
    assertThat(CodeStorageMigration.isReservedKey(Hash.hash(CODE).getBytes().toArrayUnsafe()))
        .isFalse();
  }

  private static void assertMigrated(
      final SegmentedKeyValueStorage storage, final List<Bytes> codes) {
    for (final Bytes code : codes) {
      final byte[] value =
          storage.get(CODE_STORAGE, Hash.hash(code).getBytes().toArrayUnsafe()).orElseThrow();
      final Code stored = CodeStorageFormat.of(value).decode(value, Hash.hash(code));
      assertThat(stored.getBytes()).isEqualTo(code);
      assertThat(stored.getJumpDestBitMask()).isEqualTo(Code.jumpDestBitMaskOf(code));
    }
    assertThat(storage.get(CODE_STORAGE, CodeStorageMigration.FORMAT_KEY)).isPresent();
    assertThat(storage.get(CODE_STORAGE, CodeStorageMigration.MIGRATION_KEY)).isEmpty();
  }

  private static Bytes codeFor(final List<Bytes> codes, final byte[] key) {
    return codes.stream()
        .filter(code -> Hash.hash(code).equals(Hash.wrap(Bytes32.wrap(key))))
        .findFirst()
        .orElseThrow();
  }

  private static SegmentedKeyValueStorage storage() {
    return new SegmentedInMemoryKeyValueStorage();
  }
}
