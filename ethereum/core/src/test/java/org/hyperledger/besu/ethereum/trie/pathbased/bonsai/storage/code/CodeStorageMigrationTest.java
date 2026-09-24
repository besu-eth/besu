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
import org.junit.jupiter.api.Test;

class CodeStorageMigrationTest {

  private static final Bytes CODE = Bytes.fromHexString("0x605b5b615b5b5b");
  private static final List<Bytes> CODES =
      List.of(CODE, Bytes.of(0x5b), Bytes.fromHexString("0x60005b"));

  @Test
  void marksAnEmptyColumnFamilyWithoutRewritingAnything() {
    final SegmentedKeyValueStorage storage = storage();

    CodeStorageMigration.migrate(storage);

    assertThat(storage.get(CODE_STORAGE, CodeStorageMigration.FORMAT_KEY)).isPresent();
    assertThat(storage.stream(CODE_STORAGE).count()).isEqualTo(1);
  }

  @Test
  void migratesLegacyEntriesOnce() {
    final SegmentedKeyValueStorage storage = storage();
    putBare(storage, CODES);

    CodeStorageMigration.migrate(storage);
    assertMigrated(storage, CODES);

    // a second run finds the marker and leaves the entries alone
    CodeStorageMigration.migrate(storage);
    assertMigrated(storage, CODES);
  }

  @Test
  void revertsMigratedEntriesToBareCode() {
    final SegmentedKeyValueStorage storage = storage();
    putBare(storage, CODES);
    CodeStorageMigration.migrate(storage);

    CodeStorageMigration.revert(storage);
    assertBare(storage, CODES);

    // a second run has nothing to revert
    CodeStorageMigration.revert(storage);
    assertBare(storage, CODES);
  }

  @Test
  void migratesAgainAfterARevert() {
    final SegmentedKeyValueStorage storage = storage();
    putBare(storage, CODES);
    CodeStorageMigration.migrate(storage);
    CodeStorageMigration.revert(storage);

    CodeStorageMigration.migrate(storage);

    assertMigrated(storage, CODES);
  }

  @Test
  void marksAClearedColumnFamilyAsCurrent() {
    final SegmentedKeyValueStorage storage = storage();

    CodeStorageMigration.markCurrent(storage);

    assertThat(storage.get(CODE_STORAGE, CodeStorageMigration.FORMAT_KEY))
        .contains(new byte[] {CodeStorageFormat.CURRENT.version});
  }

  @Test
  void reservedKeysAreNotCode() {
    assertThat(CodeStorageMigration.isReservedKey(CodeStorageMigration.FORMAT_KEY)).isTrue();
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
    assertThat(storage.get(CODE_STORAGE, CodeStorageMigration.FORMAT_KEY))
        .contains(new byte[] {CodeStorageFormat.CURRENT.version});
    assertThat(storage.stream(CODE_STORAGE).count()).isEqualTo(codes.size() + 1);
  }

  private static void putBare(final SegmentedKeyValueStorage storage, final List<Bytes> codes) {
    final SegmentedKeyValueStorageTransaction setup = storage.startTransaction();
    for (final Bytes code : codes) {
      setup.put(CODE_STORAGE, Hash.hash(code).getBytes().toArrayUnsafe(), code.toArrayUnsafe());
    }
    setup.commit();
  }

  private static void assertBare(final SegmentedKeyValueStorage storage, final List<Bytes> codes) {
    for (final Bytes code : codes) {
      assertThat(storage.get(CODE_STORAGE, Hash.hash(code).getBytes().toArrayUnsafe()))
          .contains(code.toArrayUnsafe());
    }
    assertThat(storage.get(CODE_STORAGE, CodeStorageMigration.FORMAT_KEY)).isEmpty();
    assertThat(storage.stream(CODE_STORAGE).count()).isEqualTo(codes.size());
  }

  private static SegmentedKeyValueStorage storage() {
    return new SegmentedInMemoryKeyValueStorage();
  }
}
