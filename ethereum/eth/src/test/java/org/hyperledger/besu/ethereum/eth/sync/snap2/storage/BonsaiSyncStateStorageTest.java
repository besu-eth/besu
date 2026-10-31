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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_LOG_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageTransactionValidatorDecorator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BonsaiSyncStateStorageTest {

  private static final Bytes32 FIRST = key(0x10);
  private static final Bytes32 SECOND = key(0x20);
  private static final Bytes ACCOUNT = Bytes.of(1);

  private final BonsaiWorldStateKeyValueStorage bonsai =
      bonsai(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  private final SyncStateStorage storage = new BonsaiSyncStateStorage(bonsai);
  // the transaction of the batches of storageWritingTo
  private final SegmentedKeyValueStorageTransaction transaction =
      mock(SegmentedKeyValueStorageTransaction.class);

  @Test
  void deleteStorageWritesNothingForAnAccountWithoutSlots() {
    final SyncStateStorage spied = spy(storage);

    spied.deleteStorage(FIRST);

    verify(spied, never()).batch();
  }

  @Test
  void deleteStorageAbortsTheBatchOfAFailedWrite() {
    final SyncStateStorage.Batch slots = storage.batch();
    slots.putStorage(FIRST, key(0x01), value(1));
    slots.commit();
    final StorageException writeFailed = new StorageException("write failed");
    doThrow(writeFailed).when(transaction).remove(any(), any());

    assertThatThrownBy(() -> storageWritingTo(transaction).deleteStorage(FIRST))
        .isSameAs(writeFailed);
    verify(transaction, never()).commit();
    verify(transaction).rollback();
  }

  @Test
  void writeAbortsTheBatchOfAFailedWrite() {
    final StorageException writeFailed = new StorageException("write failed");
    doThrow(writeFailed).when(transaction).put(any(), any(), any());

    assertThatThrownBy(
            () -> storageWritingTo(transaction).write(batch -> batch.putAccount(FIRST, ACCOUNT)))
        .isSameAs(writeFailed);
    verify(transaction, never()).commit();
    verify(transaction).rollback();
  }

  @Test
  void writeThrowsTheErrorOfAFailedCommit() {
    final StorageException diskFull = new StorageException("disk full");
    doThrow(diskFull).when(transaction).commit();

    assertThatThrownBy(
            () -> storageWritingTo(transaction).write(batch -> batch.putAccount(FIRST, ACCOUNT)))
        .isSameAs(diskFull);
  }

  @Test
  void writeThrowsTheErrorOfABatchThatFailsToStart() {
    final SyncStateStorage spied = spy(storage);
    final StorageException closed = new StorageException("storage closed");
    doThrow(closed).when(spied).batch();

    assertThatThrownBy(() -> spied.write(batch -> batch.putAccount(FIRST, ACCOUNT)))
        .isSameAs(closed);
  }

  @Test
  void abortDropsTheWritesOfABatch() {
    final SyncStateStorage.Batch batch = storageWritingTo(transaction).batch();
    batch.putAccount(FIRST, ACCOUNT);

    batch.abort();

    verify(transaction).rollback();
  }

  @Test
  void abortDoesNothingAfterAFailedCommit() {
    final StorageException diskFull = new StorageException("disk full");
    doThrow(diskFull).when(transaction).commit();
    final SyncStateStorage.Batch batch = storageWritingTo(transaction).batch();
    batch.putAccount(FIRST, ACCOUNT);

    assertThatThrownBy(batch::commit).isSameAs(diskFull);
    assertThatCode(batch::abort).doesNotThrowAnyException();
  }

  @Test
  void storesTheCodeForEveryAccount() {
    final Bytes code = Bytes.fromHexString("0x6001600155");
    final Bytes32 codeHash = Bytes32.wrap(Hash.hash(code).getBytes());
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putCode(codeHash, code);
    batch.commit();

    assertThat(bonsai.getCodeBytes(Hash.wrap(codeHash), Hash.wrap(FIRST))).contains(code);
  }

  @Test
  void keepsTheStorageTrieOfEachAccountApart() {
    final Bytes node = Bytes.fromHexString("0xc28080");
    final Bytes32 hash = Bytes32.wrap(Hash.hash(node).getBytes());
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    writer.storageNode(FIRST, Bytes.EMPTY, hash, node);
    writer.commit();

    assertThat(storage.hasStorageTrie(FIRST, hash)).isTrue();
    assertThat(storage.hasStorageTrie(SECOND, hash)).isFalse();
  }

  @Test
  void savesTheWorldStateOfTheBlock() {
    final Bytes rootNode = Bytes.fromHexString("0xc28080");
    final BlockHeader header =
        new BlockHeaderTestFixture().number(7).stateRoot(Hash.hash(rootNode)).buildHeader();

    storage.saveWorldState(header, rootNode);

    assertThat(bonsai.getWorldStateRootHash()).contains(header.getStateRoot().getBytes());
    assertThat(bonsai.getWorldStateBlockHash()).contains(header.getBlockHash());
    assertThat(bonsai.getTrieNode(Bytes.EMPTY, Bytes32.wrap(header.getStateRoot().getBytes())))
        .contains(rootNode);
    assertThat(
            bonsai.getComposedWorldStateStorage().get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY))
        .hasValueSatisfying(number -> assertThat(Bytes.wrap(number).toLong()).isEqualTo(7));
  }

  // the sync writes trie nodes and the world state, never a trie log
  @Test
  void opensNoTrieLogTransaction() {
    // the trie log storage Bonsai is built with, which its updaters use
    final InMemoryKeyValueStorageProvider provider = spy(new InMemoryKeyValueStorageProvider());
    final KeyValueStorage trieLogs = spy(provider.getStorageBySegmentIdentifier(TRIE_LOG_STORAGE));
    doReturn(trieLogs).when(provider).getStorageBySegmentIdentifier(TRIE_LOG_STORAGE);
    final SyncStateStorage sync =
        new BonsaiSyncStateStorage(
            bonsai(provider, DataStorageConfiguration.DEFAULT_BONSAI_CONFIG));
    final Bytes node = Bytes.fromHexString("0xc28080");
    final Bytes32 hash = Bytes32.wrap(Hash.hash(node).getBytes());

    final SyncStateStorage.TrieWriter writer = sync.trieWriter();
    writer.storageNode(FIRST, Bytes.EMPTY, hash, node);
    writer.commit();
    sync.saveWorldState(
        new BlockHeaderTestFixture().stateRoot(Hash.wrap(hash)).buildHeader(), node);

    verify(trieLogs, never()).startBatchTransaction();
    verify(trieLogs, never()).startTransaction();
  }

  @Test
  void prepareForSyncRefusesCodeStoredByAccount() {
    final BonsaiWorldStateKeyValueStorage byAccount =
        bonsai(
            ImmutableDataStorageConfiguration.builder()
                .dataStorageFormat(DataStorageFormat.BONSAI)
                .extraStorageConfiguration(
                    ImmutableExtraStorageConfiguration.builder()
                        .unstable(
                            ImmutableExtraStorageConfiguration.Unstable.builder()
                                .codeStoredByCodeHashEnabled(false)
                                .build())
                        .build())
                .build());
    final SyncStateStorage sync = new BonsaiSyncStateStorage(byAccount);
    final SyncStateStorage.Batch batch = sync.batch();
    batch.putAccount(FIRST, ACCOUNT);
    batch.commit();

    assertThatThrownBy(sync::prepareForSync).isInstanceOf(IllegalStateException.class);
    assertThat(sync.getAccount(FIRST)).as("nothing is wiped").isPresent();
  }

  @Test
  void completeSyncMovesToTheFullFlatDatabase() {
    final BonsaiWorldStateKeyValueStorage partial =
        bonsai(DataStorageConfiguration.DEFAULT_BONSAI_PARTIAL_DB_CONFIG);
    assertThat(partial.getFlatDbMode()).isEqualTo(FlatDbMode.PARTIAL);

    new BonsaiSyncStateStorage(partial).completeSync();

    assertThat(partial.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);
  }

  @Test
  void completeSyncKeepsAnArchiveDatabase() {
    final BonsaiWorldStateKeyValueStorage archive =
        bonsai(
            ImmutableDataStorageConfiguration.builder()
                .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
                .build());
    archive.upgradeToArchiveFlatDbMode();

    new BonsaiSyncStateStorage(archive).completeSync();

    assertThat(archive.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE);
  }

  // the same storage, whose batches write to this transaction behind the real validator
  private SyncStateStorage storageWritingTo(final SegmentedKeyValueStorageTransaction transaction) {
    final SegmentedKeyValueStorage flat = spy(bonsai.getComposedWorldStateStorage());
    doAnswer(
            invocation ->
                new SegmentedKeyValueStorageTransactionValidatorDecorator(transaction, () -> false))
        .when(flat)
        .startBatchTransaction();
    final BonsaiWorldStateKeyValueStorage spied = spy(bonsai);
    doReturn(flat).when(spied).getComposedWorldStateStorage();
    return new BonsaiSyncStateStorage(spied);
  }

  private static BonsaiWorldStateKeyValueStorage bonsai(final DataStorageConfiguration config) {
    return bonsai(new InMemoryKeyValueStorageProvider(), config);
  }

  private static BonsaiWorldStateKeyValueStorage bonsai(
      final InMemoryKeyValueStorageProvider provider, final DataStorageConfiguration config) {
    return (BonsaiWorldStateKeyValueStorage) provider.createWorldStateStorage(config);
  }

  private static Bytes32 key(final int last) {
    return Bytes32.leftPad(Bytes.of(last));
  }

  private static Bytes32 value(final int value) {
    return Bytes32.leftPad(Bytes.of(value));
  }
}
