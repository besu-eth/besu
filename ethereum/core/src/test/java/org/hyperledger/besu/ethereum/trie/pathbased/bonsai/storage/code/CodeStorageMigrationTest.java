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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.OptimisticRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.nio.file.Path;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CodeStorageMigrationTest {

  /** Code next to the analysis it has to be stored with. */
  private record AnalysedCode(Bytes code, long... bitMask) {}

  private static final List<AnalysedCode> CODES =
      List.of(
          // PUSH1 0x5b; JUMPDEST; PUSH2 0x5b5b; JUMPDEST
          new AnalysedCode(Bytes.fromHexString("0x605b5b615b5b5b"), 0b1000100L),
          new AnalysedCode(Bytes.of(0x5b), 0b1L),
          new AnalysedCode(Bytes.fromHexString("0x60005b"), 0b100L),
          // JUMPDEST; 61 STOP; PUSH2 with one byte of data in each word; JUMPDEST
          new AnalysedCode(
              Bytes.concatenate(
                  Bytes.of(0x5b), Bytes.wrap(new byte[61]), Bytes.fromHexString("0x615b5b5b")),
              0b1L,
              0b10L));

  /** The storages the migration runs on: the default rewrite and the one of RocksDB. */
  enum StorageKind {
    IN_MEMORY,
    ROCKSDB
  }

  @TempDir Path tempDir;

  private SegmentedKeyValueStorage storage;

  @AfterEach
  void tearDown() throws Exception {
    if (storage != null) {
      storage.close();
    }
  }

  @Test
  void marksAnEmptyColumnFamilyWithoutRewritingAnything() {
    storage = spy(new SegmentedInMemoryKeyValueStorage());

    CodeStorageMigration.migrate(storage);

    verify(storage, never()).rewrite(any(), any(), any());
    assertThat(JumpDestCodeStorageStrategy.isMarked(storage)).isTrue();
    assertThat(storage.stream(CODE_STORAGE).count()).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(StorageKind.class)
  void migratesLegacyEntriesOnce(final StorageKind kind) {
    storage = storage(kind);
    putBare(storage);

    CodeStorageMigration.migrate(storage);
    assertMigrated(storage);

    // a second run finds the marker and leaves the entries alone
    CodeStorageMigration.migrate(storage);
    assertMigrated(storage);
  }

  @ParameterizedTest
  @EnumSource(StorageKind.class)
  void revertsMigratedEntriesToBareCode(final StorageKind kind) {
    storage = storage(kind);
    putBare(storage);
    CodeStorageMigration.migrate(storage);

    CodeStorageMigration.revert(storage);
    assertBare(storage);

    // a second run has nothing to revert
    CodeStorageMigration.revert(storage);
    assertBare(storage);
  }

  @ParameterizedTest
  @EnumSource(StorageKind.class)
  void migratesAgainAfterARevert(final StorageKind kind) {
    storage = storage(kind);
    putBare(storage);
    CodeStorageMigration.migrate(storage);
    CodeStorageMigration.revert(storage);

    CodeStorageMigration.migrate(storage);

    assertMigrated(storage);
  }

  private static void assertMigrated(final SegmentedKeyValueStorage storage) {
    for (final AnalysedCode analysed : CODES) {
      final Hash codeHash = Hash.hash(analysed.code());
      final byte[] value =
          storage.get(CODE_STORAGE, codeHash.getBytes().toArrayUnsafe()).orElseThrow();
      final Code stored = JumpDestCodeStorageStrategy.decode(value, codeHash);
      assertThat(stored.getBytes()).isEqualTo(analysed.code());
      assertThat(stored.getJumpDestBitMask()).containsExactly(analysed.bitMask());
    }
    assertThat(storage.get(CODE_STORAGE, JumpDestCodeStorageStrategy.MARKER_KEY))
        .contains(JumpDestCodeStorageStrategy.MARKER);
    assertThat(storage.stream(CODE_STORAGE).count()).isEqualTo(CODES.size() + 1);
  }

  private static void putBare(final SegmentedKeyValueStorage storage) {
    final SegmentedKeyValueStorageTransaction setup = storage.startTransaction();
    for (final AnalysedCode analysed : CODES) {
      setup.put(
          CODE_STORAGE,
          Hash.hash(analysed.code()).getBytes().toArrayUnsafe(),
          analysed.code().toArrayUnsafe());
    }
    setup.commit();
  }

  private static void assertBare(final SegmentedKeyValueStorage storage) {
    for (final AnalysedCode analysed : CODES) {
      assertThat(storage.get(CODE_STORAGE, Hash.hash(analysed.code()).getBytes().toArrayUnsafe()))
          .contains(analysed.code().toArrayUnsafe());
    }
    assertThat(JumpDestCodeStorageStrategy.isMarked(storage)).isFalse();
    assertThat(storage.stream(CODE_STORAGE).count()).isEqualTo(CODES.size());
  }

  private SegmentedKeyValueStorage storage(final StorageKind kind) {
    return switch (kind) {
      case IN_MEMORY -> new SegmentedInMemoryKeyValueStorage();
      case ROCKSDB ->
          new OptimisticRocksDBColumnarKeyValueStorage(
              // the rewrite stages its files next to the database directory
              new RocksDBConfigurationBuilder().databaseDir(tempDir.resolve("database")).build(),
              List.of(KeyValueSegmentIdentifier.DEFAULT, CODE_STORAGE),
              List.of(),
              new NoOpMetricsSystem(),
              RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
    };
  }
}
