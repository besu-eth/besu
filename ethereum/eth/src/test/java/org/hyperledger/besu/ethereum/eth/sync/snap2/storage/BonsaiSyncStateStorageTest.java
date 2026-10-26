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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_LOG_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.mockito.ArgumentMatchers.any;
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
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BonsaiSyncStateStorageTest {

  private static final Bytes32 BEFORE = key(0x05);
  private static final Bytes32 FIRST = key(0x10);
  private static final Bytes32 ORPHAN = key(0x15);
  private static final Bytes32 SECOND = key(0x20);
  private static final Bytes32 THIRD = key(0x30);
  private static final Bytes32 LAST_ORPHAN = key(0x35);
  private static final Bytes32 AFTER = key(0x50);
  private static final Bytes ACCOUNT = Bytes.of(1);

  private final BonsaiWorldStateKeyValueStorage bonsai =
      bonsai(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  private final SyncStateStorage storage = new BonsaiSyncStateStorage(bonsai);

  // slots of a missing account come after the previous account, an account may skip its slots
  @Test
  void scanVisitsTheAccountsOfTheRangeEachFollowedByItsSlots() {
    final SyncStateStorage.Batch batch = storage.batch();
    for (final Bytes32 account : List.of(BEFORE, FIRST, SECOND, THIRD, AFTER)) {
      batch.putAccount(account, ACCOUNT);
    }
    for (final Bytes32 account :
        List.of(BEFORE, FIRST, ORPHAN, SECOND, THIRD, LAST_ORPHAN, AFTER)) {
      batch.putStorage(account, key(0x02), value(2));
      batch.putStorage(account, key(0x01), value(1));
    }
    batch.commit();

    final List<String> visits = new ArrayList<>();
    storage.scan(
        FIRST,
        key(0x3f),
        new SyncStateStorage.ScanVisitor() {
          @Override
          public boolean onAccount(final Bytes32 accountHash, final Bytes account) {
            visits.add("account " + name(accountHash));
            return !accountHash.equals(SECOND);
          }

          @Override
          public void onSlot(final Bytes32 accountHash, final Bytes32 slotHash, final Bytes value) {
            visits.add(
                "slot "
                    + name(accountHash)
                    + "/"
                    + name(slotHash)
                    + "="
                    + name(Bytes32.wrap(value)));
          }
        });

    assertThat(visits)
        .containsExactly(
            "account 10",
            "slot 10/01=01",
            "slot 10/02=02",
            "slot 15/01=01",
            "slot 15/02=02",
            "account 20",
            "account 30",
            "slot 30/01=01",
            "slot 30/02=02",
            "slot 35/01=01",
            "slot 35/02=02");
  }

  // more slots than one delete batch holds
  @Test
  void deleteStorageRemovesEverySlotOfTheAccountOnly() {
    final SyncStateStorage.Batch batch = storage.batch();
    for (int i = 0; i < 10_001; i++) {
      batch.putStorage(FIRST, Bytes32.leftPad(Bytes.ofUnsignedInt(i)), value(1));
    }
    batch.putStorage(SECOND, key(0x01), value(1));
    batch.commit();

    storage.deleteStorage(FIRST);

    assertThat(slots(storage, FIRST)).isEmpty();
    assertThat(slots(storage, SECOND)).containsOnlyKeys(key(0x01));
  }

  @Test
  void deleteStorageAbortsTheBatchOfAFailedWriteButNotOneWhoseCommitFailed() {
    final SyncStateStorage.Batch slots = storage.batch();
    slots.putStorage(FIRST, key(0x01), value(1));
    slots.commit();
    final SyncStateStorage failing = spy(storage);
    final SyncStateStorage.Batch batch = mock(SyncStateStorage.Batch.class);
    doReturn(batch).when(failing).batch();

    // the commit of the transaction has started, it cannot be rolled back
    doThrow(new IllegalStateException("disk full")).when(batch).commit();
    assertThatThrownBy(() -> failing.deleteStorage(FIRST)).hasMessage("disk full");
    verify(batch, never()).abort();

    doThrow(new IllegalStateException("write failed")).when(batch).removeStorage(any(), any());
    assertThatThrownBy(() -> failing.deleteStorage(FIRST)).hasMessage("write failed");
    verify(batch).abort();
  }

  // the next batch fails to start once the first one is committed
  @Test
  void deleteStorageAbortsNoBatchItCommitted() {
    final SyncStateStorage.Batch slots = storage.batch();
    for (int i = 0; i < 10_001; i++) {
      slots.putStorage(FIRST, Bytes32.leftPad(Bytes.ofUnsignedInt(i)), value(1));
    }
    slots.commit();
    final SyncStateStorage failing = spy(storage);
    final SyncStateStorage.Batch batch = mock(SyncStateStorage.Batch.class);
    doReturn(batch).doThrow(new IllegalStateException("storage closed")).when(failing).batch();

    assertThatThrownBy(() -> failing.deleteStorage(FIRST)).hasMessage("storage closed");
    verify(batch).commit();
    verify(batch, never()).abort();
  }

  @Test
  void deleteStorageWritesNothingForAnAccountWithoutSlots() {
    final SyncStateStorage spied = spy(storage);

    spied.deleteStorage(FIRST);

    verify(spied, never()).batch();
  }

  @Test
  void removesAccountsAndSlots() {
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putAccount(FIRST, ACCOUNT);
    batch.putStorage(FIRST, key(0x01), value(1));
    batch.commit();

    final SyncStateStorage.Batch removal = storage.batch();
    removal.removeAccount(FIRST);
    removal.removeStorage(FIRST, key(0x01));
    removal.commit();

    assertThat(storage.getAccount(FIRST)).isEmpty();
    assertThat(slots(storage, FIRST)).isEmpty();
  }

  @Test
  void abortDropsTheWritesOfABatch() {
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putAccount(FIRST, ACCOUNT);
    batch.putStorage(FIRST, key(0x01), value(1));
    batch.abort();

    assertThat(storage.getAccount(FIRST)).isEmpty();
    assertThat(slots(storage, FIRST)).isEmpty();
  }

  @Test
  void storesTheCodeByItsHash() {
    final Bytes code = Bytes.fromHexString("0x6001600155");
    final Bytes32 codeHash = Bytes32.wrap(Hash.hash(code).getBytes());
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putCode(codeHash, code);
    batch.commit();

    assertThat(storage.hasCode(codeHash)).isTrue();
    assertThat(storage.hasCode(FIRST)).isFalse();
    assertThat(bonsai.getCodeBytes(Hash.wrap(codeHash), Hash.wrap(FIRST))).contains(code);
  }

  @Test
  void writesTheNodesOfATrieOnCommitOnly() {
    final Bytes node = Bytes.fromHexString("0xc28080");
    final Bytes32 hash = Bytes32.wrap(Hash.hash(node).getBytes());
    final SyncStateStorage.TrieWriter aborted = storage.trieWriter();
    aborted.storageNode(FIRST, Bytes.EMPTY, hash, node);
    aborted.abort();
    assertThat(storage.hasStorageTrie(FIRST, hash)).isFalse();

    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    writer.storageNode(FIRST, Bytes.EMPTY, hash, node);
    writer.accountNode(Bytes.of(1), hash, node);
    assertThat(storage.hasStorageTrie(FIRST, hash)).isFalse();
    writer.commit();

    assertThat(storage.hasStorageTrie(FIRST, hash)).isTrue();
    assertThat(storage.hasStorageTrie(SECOND, hash)).isFalse();
    assertThat(storage.storageTrieNode(FIRST, Bytes.EMPTY, hash)).contains(node);
    assertThat(bonsai.getTrieNode(Bytes.of(1), hash)).contains(node);
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
  void prepareForSyncWipesTheState() {
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putAccount(FIRST, ACCOUNT);
    batch.commit();

    storage.prepareForSync();

    assertThat(storage.getAccount(FIRST)).isEmpty();
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

  // the last byte names a key in the expected visits
  private static String name(final Bytes32 key) {
    return String.format("%02x", key.get(31));
  }
}
