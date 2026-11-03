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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.slots;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ForestSyncStateStorageTest {

  private static final Bytes32 FIRST = key(0x10);
  private static final Bytes32 SECOND = key(0x20);
  private static final Bytes32 SLOT = key(0x01);
  private static final Bytes ACCOUNT = Bytes.of(1);
  private static final Bytes CODE = Bytes.fromHexString("0x6001600155");
  private static final Bytes32 CODE_HASH = hash(CODE);
  private static final Bytes NODE = Bytes.fromHexString("0xc28080");
  private static final Bytes32 NODE_HASH = hash(NODE);

  private final KeyValueStorage worldState = spy(new InMemoryKeyValueStorage());
  private final ForestWorldStateKeyValueStorage forest =
      new ForestWorldStateKeyValueStorage(worldState);
  private final SyncStateStorage storage =
      new ForestSyncStateStorage(forest, new InMemoryKeyValueStorageProvider());

  @Test
  void stagesTheSlotValuesWithoutTheirLeadingZeros() {
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putStorage(FIRST, SLOT, Bytes32.leftPad(Bytes.of(1, 2)));
    batch.commit();

    final SyncStateStorage.ScanVisitor visitor = mock(SyncStateStorage.ScanVisitor.class);
    storage.scan(FIRST, FIRST, visitor);

    verify(visitor).onSlot(FIRST, SLOT, Bytes.of(1, 2));
  }

  @Test
  void findsAStorageTrieWhicheverAccountWroteIt() {
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    writer.storageNode(FIRST, Bytes.EMPTY, NODE_HASH, NODE);
    writer.commit();

    assertThat(storage.hasStorageTrie(SECOND, NODE_HASH)).isTrue();
    assertThat(storage.storageTrieNode(SECOND, Bytes.of(1), NODE_HASH)).contains(NODE);
  }

  @Test
  void completeSyncClearsTheStagingOnly() {
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putAccount(FIRST, ACCOUNT);
    batch.putStorage(FIRST, SLOT, Bytes32.leftPad(Bytes.of(1)));
    batch.putCode(CODE_HASH, CODE);
    batch.commit();
    storage.saveWorldState(header(NODE), NODE);

    storage.completeSync();

    assertThat(storage.getAccount(FIRST)).isEmpty();
    assertThat(slots(storage, FIRST)).isEmpty();
    assertThat(storage.hasCode(CODE_HASH)).isTrue();
    assertThat(forest.isWorldStateAvailable(NODE_HASH)).isTrue();
  }

  // a transaction locks each node it puts until its commit, other writers of the node time out
  @Test
  void writesTheWorldStateInWriteBatchesOnly() {
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putCode(CODE_HASH, CODE);
    batch.commit();
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    writer.storageNode(FIRST, Bytes.EMPTY, NODE_HASH, NODE);
    writer.commit();
    storage.saveWorldState(header(NODE), NODE);

    verify(worldState, never()).startTransaction();
  }

  // the code first: no account is left pointing to a code whose commit failed
  @Test
  void dropsTheFlatWritesOfABatchWhoseCodeFailsToCommit() {
    final SegmentedKeyValueStorageTransaction flatWrites =
        mock(SegmentedKeyValueStorageTransaction.class);
    final KeyValueStorageTransaction codes = mock(KeyValueStorageTransaction.class);
    final StorageException diskFull = new StorageException("disk full");
    doThrow(diskFull).when(codes).commit();
    doReturn(codes).when(worldState).startBatchTransaction();

    assertThatThrownBy(() -> writingFlatStateWith(flatWrites).write(this::putAccountAndCode))
        .isSameAs(diskFull);

    verify(flatWrites, never()).commit();
    verify(flatWrites).rollback();
    verify(codes, never()).rollback();
  }

  // the code is written first, and stays: it is keyed by hash
  @Test
  void keepsTheCodeOfABatchWhoseFlatWritesFailToCommit() {
    final SegmentedKeyValueStorageTransaction flatWrites =
        mock(SegmentedKeyValueStorageTransaction.class);
    final StorageException diskFull = new StorageException("disk full");
    doThrow(diskFull).when(flatWrites).commit();
    final SyncStateStorage writing = writingFlatStateWith(flatWrites);

    assertThatThrownBy(() -> writing.write(this::putAccountAndCode)).isSameAs(diskFull);

    verify(flatWrites, never()).rollback();
    assertThat(writing.hasCode(CODE_HASH)).isTrue();
  }

  @Test
  void abortDropsTheCodeAndTheFlatWrites() {
    final SegmentedKeyValueStorageTransaction flatWrites =
        mock(SegmentedKeyValueStorageTransaction.class);
    final KeyValueStorageTransaction codes = mock(KeyValueStorageTransaction.class);
    doReturn(codes).when(worldState).startBatchTransaction();
    final SyncStateStorage.Batch batch = writingFlatStateWith(flatWrites).batch();
    batch.putAccount(FIRST, ACCOUNT);
    batch.putCode(CODE_HASH, CODE);

    batch.abort();

    verify(codes).rollback();
    verify(flatWrites).rollback();
  }

  @Test
  void abortDropsTheNodesOfAWriter() {
    final KeyValueStorageTransaction nodes = mock(KeyValueStorageTransaction.class);
    doReturn(nodes).when(worldState).startBatchTransaction();
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    writer.storageNode(FIRST, Bytes.EMPTY, NODE_HASH, NODE);

    writer.abort();

    verify(nodes).rollback();
  }

  private void putAccountAndCode(final SyncStateStorage.Batch batch) {
    batch.putAccount(FIRST, ACCOUNT);
    batch.putCode(CODE_HASH, CODE);
  }

  // a sync storage whose batches write the flat state through the given transaction
  private SyncStateStorage writingFlatStateWith(
      final SegmentedKeyValueStorageTransaction transaction) {
    final SegmentedKeyValueStorage staging = mock(SegmentedKeyValueStorage.class);
    when(staging.startBatchTransaction()).thenReturn(transaction);
    final StorageProvider provider = mock(StorageProvider.class);
    when(provider.getStorageBySegmentIdentifiers(any())).thenReturn(staging);
    return new ForestSyncStateStorage(forest, provider);
  }

  private static BlockHeader header(final Bytes rootNode) {
    return new BlockHeaderTestFixture().stateRoot(Hash.hash(rootNode)).buildHeader();
  }

  private static Bytes32 key(final int last) {
    return Bytes32.leftPad(Bytes.of(last));
  }

  private static Bytes32 hash(final Bytes bytes) {
    return Bytes32.wrap(Hash.hash(bytes).getBytes());
  }
}
