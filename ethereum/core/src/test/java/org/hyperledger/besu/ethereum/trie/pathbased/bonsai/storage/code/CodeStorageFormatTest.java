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

import java.util.List;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class CodeStorageFormatTest {

  // PUSH1 0x5b; JUMPDEST; PUSH2 0x5b5b; JUMPDEST: only the two instructions are destinations
  private static final Bytes CODE = Bytes.fromHexString("0x60 5b 5b 61 5b5b 5b".replace(" ", ""));

  @Test
  void roundTripsCodeAndAnalysis() {
    final StoredCode stored = CodeStorageFormat.decode(CodeStorageFormat.encode(CODE));

    assertThat(stored.code()).isEqualTo(CODE);
    assertThat(stored.jumpDestBitMask()).isEqualTo(Code.jumpDestBitMaskOf(CODE));
    assertThat(stored.jumpDestBitMask()).containsExactly(0b1000100L);
  }

  @Test
  void roundTripsCodeSpanningManyEntries() {
    final byte[] raw = new byte[24576];
    new Random(42).nextBytes(raw);
    final Bytes code = Bytes.wrap(raw);

    final StoredCode stored = CodeStorageFormat.decode(CodeStorageFormat.encode(code));

    assertThat(stored.code()).isEqualTo(code);
    assertThat(stored.jumpDestBitMask()).isEqualTo(Code.jumpDestBitMaskOf(code));
  }

  @Test
  void storedAnalysisIsUsedAsIs() {
    final StoredCode stored = CodeStorageFormat.decode(CodeStorageFormat.encode(CODE));
    final Code code = stored.toCode(Hash.EMPTY);

    assertThat(code.getJumpDestBitMask()).isSameAs(stored.jumpDestBitMask());
    assertThat(code.getBytes()).isSameAs(stored.code());
    assertThat(code.isJumpDestInvalid(2)).isFalse();
    assertThat(code.isJumpDestInvalid(1)).isTrue();
    assertThat(code.isJumpDestInvalid(4)).isTrue();
    assertThat(code.isJumpDestInvalid(6)).isFalse();
  }

  @Test
  void rejectsLegacyValues() {
    assertThatThrownBy(() -> CodeStorageFormat.decode(CODE.toArrayUnsafe()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> CodeStorageFormat.decode(Bytes.fromHexString("0x02").toArrayUnsafe()))
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> CodeStorageFormat.decode(new byte[0]))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void decodesCodeStoredWithoutAnalysis() {
    final StoredCode stored =
        CodeStorageFormat.decode(Bytes.concatenate(Bytes.of(1), CODE).toArrayUnsafe());

    assertThat(stored.code()).isEqualTo(CODE);
    assertThat(stored.jumpDestBitMask()).isNull();
  }

  @Test
  void upgradesEntriesStoredWithoutAnalysis() {
    final SegmentedKeyValueStorage storage = storage();
    final List<Bytes> codes = List.of(CODE, Bytes.of(0x5b), Bytes.fromHexString("0x60005b"));
    final SegmentedKeyValueStorageTransaction setup = storage.startTransaction();
    // one entry is already current, the others only carry the format byte
    setup.put(
        CODE_STORAGE,
        Hash.hash(codes.get(0)).getBytes().toArrayUnsafe(),
        CodeStorageFormat.encode(codes.get(0)));
    for (final Bytes code : codes.subList(1, codes.size())) {
      setup.put(
          CODE_STORAGE,
          Hash.hash(code).getBytes().toArrayUnsafe(),
          Bytes.concatenate(Bytes.of(1), code).toArrayUnsafe());
    }
    setup.put(CODE_STORAGE, CodeStorageFormat.FORMAT_KEY, new byte[] {1});
    setup.commit();

    CodeStorageFormat.migrate(storage);

    assertMigrated(storage, codes);
    assertThat(storage.get(CODE_STORAGE, CodeStorageFormat.FORMAT_KEY)).contains(new byte[] {2});
  }

  @Test
  void marksAnEmptyColumnFamilyWithoutRewritingAnything() {
    final SegmentedKeyValueStorage storage = storage();

    CodeStorageFormat.migrate(storage);

    assertThat(storage.get(CODE_STORAGE, CodeStorageFormat.FORMAT_KEY)).isPresent();
    assertThat(storage.get(CODE_STORAGE, CodeStorageFormat.MIGRATION_KEY)).isEmpty();
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

    CodeStorageFormat.migrate(storage);
    assertMigrated(storage, codes);

    // a second run finds the marker and leaves the entries alone
    CodeStorageFormat.migrate(storage);
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
          CODE_STORAGE, keys.get(i), i < 2 ? CodeStorageFormat.encode(code) : code.toArrayUnsafe());
    }
    setup.put(CODE_STORAGE, CodeStorageFormat.MIGRATION_KEY, keys.get(1));
    setup.commit();

    CodeStorageFormat.migrate(storage);

    assertMigrated(storage, codes);
  }

  @Test
  void reservedKeysAreNotCode() {
    assertThat(CodeStorageFormat.isReservedKey(CodeStorageFormat.FORMAT_KEY)).isTrue();
    assertThat(CodeStorageFormat.isReservedKey(CodeStorageFormat.MIGRATION_KEY)).isTrue();
    assertThat(CodeStorageFormat.isReservedKey(Hash.hash(CODE).getBytes().toArrayUnsafe()))
        .isFalse();
  }

  private static void assertMigrated(
      final SegmentedKeyValueStorage storage, final List<Bytes> codes) {
    for (final Bytes code : codes) {
      final byte[] value =
          storage.get(CODE_STORAGE, Hash.hash(code).getBytes().toArrayUnsafe()).orElseThrow();
      final StoredCode stored = CodeStorageFormat.decode(value);
      assertThat(stored.code()).isEqualTo(code);
      assertThat(stored.jumpDestBitMask()).isEqualTo(Code.jumpDestBitMaskOf(code));
    }
    assertThat(storage.get(CODE_STORAGE, CodeStorageFormat.FORMAT_KEY)).isPresent();
    assertThat(storage.get(CODE_STORAGE, CodeStorageFormat.MIGRATION_KEY)).isEmpty();
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
