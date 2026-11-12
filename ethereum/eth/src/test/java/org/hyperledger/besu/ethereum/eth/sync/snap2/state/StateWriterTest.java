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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.slots;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.RangeProofs;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class StateWriterTest {

  private static final Bytes32 FIRST = key(0x10);
  private static final Bytes32 SECOND = key(0x20);
  private static final Bytes32 THIRD = key(0x30);
  private static final Bytes32 FOURTH = key(0x40);
  private static final Bytes CODE = Bytes.fromHexString("0x6001600155");

  private final Storages storages = SyncStorageFixture.create(Format.BONSAI);
  private final StateCoverage coverage = new StateCoverage();
  private final StateWriter writer = new StateWriter(storages.sync(), coverage);
  // six slots, and the same with the fifth changed
  private final SimpleMerklePatriciaTrie<Bytes32, Bytes> storage = storageTrie(0);
  private final SimpleMerklePatriciaTrie<Bytes32, Bytes> changed = storageTrie(5);

  // the proof of the range reaches the first account after its limit
  @Test
  void writesTheAccountsOfARangeUpToItsLimit() {
    final Bytes withStorage = account(Hash.wrap(root(storage)), Hash.EMPTY);
    final Bytes withCode = account(Hash.EMPTY_TRIE_HASH, Hash.hash(CODE));

    final StateWriter.AccountsWritten written =
        writer.accounts(
            accountRange(FIRST, THIRD),
            new AccountRangeRequest.Result(
                List.of(FIRST, SECOND, FOURTH), List.of(withStorage, withCode, withCode), true));

    assertThat(written.done()).isTrue();
    assertThat(written.end()).isEqualTo(THIRD);
    assertThat(storages.sync().getAccount(FIRST)).contains(withStorage);
    assertThat(storages.sync().getAccount(SECOND)).contains(withCode);
    assertThat(storages.sync().getAccount(FOURTH)).isEmpty();
    assertThat(coverage.accounts().encloses(FIRST, THIRD)).isTrue();
    assertThat(coverage.accounts().contains(FOURTH)).isFalse();
    assertThat(written.storageTasks()).extracting(StorageTask::accountHash).containsExactly(FIRST);
    assertThat(coverage.isPending(written.storageTasks().getFirst())).isTrue();
    assertThat(written.codeHashes()).containsExactly(Bytes32.wrap(Hash.hash(CODE).getBytes()));
    assertThat(writer.accountsWritten()).isEqualTo(2);
  }

  @Test
  void endsARangeCutShortAtItsLastAccount() {
    final Bytes plain = account(Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    final StateWriter.AccountsWritten written =
        writer.accounts(
            accountRange(FIRST, FOURTH),
            new AccountRangeRequest.Result(List.of(FIRST, SECOND), List.of(plain, plain), true));

    assertThat(written.done()).isFalse();
    assertThat(written.end()).isEqualTo(SECOND);
    assertThat(coverage.accounts().encloses(FIRST, SECOND)).isTrue();
    assertThat(coverage.accounts().contains(THIRD)).isFalse();
  }

  // the peer has more accounts, none of them up to the limit
  @Test
  void endsARangeWhoseLastAccountIsItsLimit() {
    final Bytes plain = account(Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    final StateWriter.AccountsWritten written =
        writer.accounts(
            accountRange(FIRST, THIRD),
            new AccountRangeRequest.Result(List.of(FIRST, THIRD), List.of(plain, plain), true));

    assertThat(written.done()).isTrue();
    assertThat(written.end()).isEqualTo(THIRD);
  }

  // the proof shows no account after the last one
  @Test
  void endsARangeWithoutAccountsAfterTheLastOneAtItsLimit() {
    final Bytes plain = account(Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    final StateWriter.AccountsWritten written =
        writer.accounts(
            accountRange(FIRST, FOURTH),
            new AccountRangeRequest.Result(List.of(FIRST, SECOND), List.of(plain, plain), false));

    assertThat(written.done()).isTrue();
    assertThat(written.end()).isEqualTo(FOURTH);
    assertThat(coverage.accounts().encloses(FIRST, FOURTH)).isTrue();
  }

  @Test
  void writesTheTrieOfAStorageOnceItsLastRangeIsIn() {
    final StorageTask task = pending(FIRST, storage);
    final NavigableMap<Bytes32, Bytes> leaves = leaves(storage);

    final StateWriter.StorageWritten first =
        writer.storages(chunk(task, RangeManager.MIN_RANGE), served(storage, 3));
    assertThat(first.lastComplete()).isFalse();
    assertThat(storages.sync().hasStorageTrie(FIRST, root(storage))).isFalse();
    writer.storages(chunk(task, next(first.lastSlot())), served(storage, next(first.lastSlot())));

    assertThat(storages.sync().hasStorageTrie(FIRST, root(storage))).isTrue();
    assertThat(storages.storageTrie(FIRST, root(storage)).entriesFrom(Bytes32.ZERO, 10))
        .isEqualTo(leaves);
    assertThat(slots(storages.sync(), FIRST)).hasSize(6);
    assertThat(coverage.storageTask(FIRST)).isNull();
    assertThat(writer.isTrieLeft(FIRST)).isFalse();
    assertThat(writer.storageTriesWritten()).isOne();
    assertThat(writer.storageTrieSlots()).isEqualTo(6);
    assertThat(writer.slotsWritten()).isEqualTo(6);
  }

  // the trie of the first ranges misses the slots that changed since
  @Test
  void leavesToTheFinalBuildAStorageWhoseRangesCameAtTwoRoots() {
    final StorageTask task = pending(FIRST, storage);
    final StateWriter.StorageWritten first =
        writer.storages(chunk(task, RangeManager.MIN_RANGE), served(storage, 3));

    task.refreshRoot(root(changed));
    writer.storages(chunk(task, next(first.lastSlot())), served(changed, next(first.lastSlot())));

    assertThat(coverage.storageTask(FIRST)).isNull();
    assertThat(storages.sync().hasStorageTrie(FIRST, root(changed))).isFalse();
    assertThat(writer.isTrieLeft(FIRST)).isTrue();
    assertThat(writer.storageTriesLeft()).isOne();
    assertThat(writer.storageTriesWritten()).isZero();
  }

  // the slots, and the trie nodes of the storage it did not complete, are committed first
  @Test
  void leavesToTheFinalBuildAStorageWhoseRootNodeWasLost() {
    // the commit of the root nodes fails
    final StateWriter failingWriter = writerFailingOnce(Bytes::isEmpty);
    final StorageTask whole = pending(FIRST, changed);
    final StorageTask cut = pending(SECOND, storage);
    final NavigableMap<Bytes32, Bytes> firstFour = firstSlots(storage, 4);

    assertThatThrownBy(() -> failingWriter.storages(batch(whole, cut), wholeThenCut(firstFour)))
        .isInstanceOf(StorageException.class);

    assertThat(coverage.storageTask(FIRST)).isNull();
    assertThat(failingWriter.isTrieLeft(FIRST)).isTrue();
    assertThat(failingWriter.storageTriesWritten()).isZero();
    assertThat(storages.sync().hasStorageTrie(FIRST, root(changed))).isFalse();
    assertThat(slots(storages.sync(), FIRST)).hasSize(6);
    // the storage cut short completes, its trie whole
    final Bytes32 rest = next(firstFour.lastKey());
    failingWriter.storages(chunk(cut, rest), served(storage, rest));
    assertThat(failingWriter.isTrieLeft(SECOND)).isFalse();
    assertThat(storages.storageTrie(SECOND, root(storage)).entriesFrom(Bytes32.ZERO, 10))
        .isEqualTo(leaves(storage));
  }

  // the trie nodes are committed before the slots, so that no slot lies outside a covered range
  @Test
  void storesNoSlotWhenTheNodesOfItsRangeFailToCommit() {
    // the first commit fails, that of the nodes of the range
    final StateWriter failingWriter = writerFailingOnce(location -> true);
    final StorageTask task = pending(FIRST, storage);

    assertThatThrownBy(
            () -> failingWriter.storages(chunk(task, RangeManager.MIN_RANGE), served(storage, 3)))
        .isInstanceOf(StorageException.class);

    assertThat(slots(storages.sync(), FIRST)).isEmpty();
    assertThat(task.covers(RangeManager.MIN_RANGE)).isFalse();
  }

  // a retried request stores its range again
  @Test
  void countsTheSlotsOfARetriedRangeOnce() {
    final StorageTask task = pending(FIRST, storage);

    writer.storages(chunk(task, RangeManager.MIN_RANGE), served(storage, 3));
    writer.storages(chunk(task, RangeManager.MIN_RANGE), served(storage, 3));

    assertThat(writer.slotsWritten()).isEqualTo(3);
  }

  // the last slot of a chunk's answer may lie past its limit, as the proof reaches it
  @Test
  void storesAChunkUpToItsLimit() {
    final StorageTask task = pending(FIRST, storage);
    // between the third slot and the fourth
    final Bytes32 limit = key(0x70);

    final StateWriter.StorageWritten written =
        writer.storages(chunk(task, RangeManager.MIN_RANGE, limit), served(storage, 4));

    assertThat(written).isEqualTo(new StateWriter.StorageWritten(1, key(0x80), true, 4));
    assertThat(slots(storages.sync(), FIRST)).hasSize(3);
    assertThat(task.covers(limit)).isTrue();
    assertThat(task.covers(next(limit))).isFalse();
    // the next chunk completes the storage, its trie whole
    writer.storages(chunk(task, next(limit)), served(storage, next(limit)));
    assertThat(storages.storageTrie(FIRST, root(storage)).entriesFrom(Bytes32.ZERO, 10))
        .isEqualTo(leaves(storage));
  }

  // a peer may answer a range without slots with the proof of its origin alone
  @Test
  void completesAStorageWhoseLastRangeIsAProofAlone() {
    final StorageTask task = pending(FIRST, storage);
    final Bytes32 lastSlot = leaves(storage).lastKey();
    writer.storages(chunk(task, RangeManager.MIN_RANGE, lastSlot), served(storage, 6));

    final StateWriter.StorageWritten written =
        writer.storages(chunk(task, next(lastSlot)), provenEmpty(storage, next(lastSlot)));

    assertThat(written).isEqualTo(new StateWriter.StorageWritten(1, null, true, 0));
    assertThat(coverage.storageTask(FIRST)).isNull();
    assertThat(storages.storageTrie(FIRST, root(storage)).entriesFrom(Bytes32.ZERO, 10))
        .isEqualTo(leaves(storage));
  }

  // a response serves the first storages asked, the last one possibly cut short
  @Test
  void tellsWhereTheLastStorageOfAResponseStopped() {
    final StorageTask whole = pending(FIRST, changed);
    final StorageTask cut = pending(SECOND, storage);
    final NavigableMap<Bytes32, Bytes> firstFour = firstSlots(storage, 4);

    final StateWriter.StorageWritten written =
        writer.storages(batch(whole, cut), wholeThenCut(firstFour));

    assertThat(written).isEqualTo(new StateWriter.StorageWritten(2, firstFour.lastKey(), false, 4));
    assertThat(coverage.storageTask(FIRST)).isNull();
    assertThat(coverage.isPending(cut)).isTrue();
    assertThat(slots(storages.sync(), SECOND)).hasSize(4);
  }

  @Test
  void writesNothingForAStorageNoLongerPending() {
    final StorageTask task = pending(FIRST, storage);
    final StorageTask newer = pending(FIRST, storage);

    writer.storages(chunk(task, RangeManager.MIN_RANGE), served(storage, 3));

    assertThat(slots(storages.sync(), FIRST)).isEmpty();
    assertThat(coverage.isPending(newer)).isTrue();
    assertThat(newer.covers(RangeManager.MIN_RANGE)).isFalse();
  }

  @Test
  void refreshesTheRootOfAStorageFromItsAccountFetchedAgain() {
    final StorageTask task = pending(FIRST, storage);
    task.markRootStale();
    final Bytes fetched = account(Hash.wrap(root(changed)), Hash.EMPTY);

    assertThat(writer.refresh(task, accounts(FIRST, fetched))).isTrue();

    assertThat(storages.sync().getAccount(FIRST)).contains(fetched);
    assertThat(task.isRootStale()).isFalse();
    assertThat(task.storageRoot()).isEqualTo(root(changed));
    assertThat(coverage.isPending(task)).isTrue();
  }

  @Test
  void dropsTheStorageOfAnAccountGoneOrEmptied() {
    final StorageTask gone = pending(FIRST, storage);
    final StorageTask emptied = pending(SECOND, storage);
    writeSlots(FIRST, SECOND);
    final Bytes empty = account(Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    // the account after the one asked comes back, the one asked is gone
    writer.refresh(gone, accounts(THIRD, empty));
    writer.refresh(emptied, accounts(SECOND, empty));

    assertThat(storages.sync().getAccount(FIRST)).isEmpty();
    assertThat(storages.sync().getAccount(SECOND)).contains(empty);
    assertThat(slots(storages.sync(), FIRST)).isEmpty();
    assertThat(slots(storages.sync(), SECOND)).isEmpty();
    assertThat(coverage.pendingStorageCount()).isZero();
  }

  @Test
  void refreshesNothingOnceTheStorageIsNoLongerPending() {
    final StorageTask task = pending(FIRST, storage);
    coverage.removeStorageTask(FIRST);
    task.markRootStale();

    assertThat(writer.refresh(task, accounts(FIRST, account(Hash.EMPTY_TRIE_HASH, Hash.EMPTY))))
        .isFalse();

    assertThat(storages.sync().getAccount(FIRST)).isEmpty();
    assertThat(task.isRootStale()).isTrue();
  }

  @Test
  void writesBytecodes() {
    final Bytes32 codeHash = Bytes32.wrap(Hash.hash(CODE).getBytes());

    writer.codes(Map.of(codeHash, CODE));

    assertThat(storages.sync().hasCode(codeHash)).isTrue();
    assertThat(writer.codesWritten()).isOne();
  }

  @Test
  void abortsTheBatchOfAFailedWrite() {
    final SyncStateStorage mocked = mock(SyncStateStorage.class, Answers.CALLS_REAL_METHODS);
    final SyncStateStorage.Batch batch = mock(SyncStateStorage.Batch.class);
    when(mocked.batch()).thenReturn(batch);
    final StateWriter mockedWriter = new StateWriter(mocked, coverage);
    final Bytes32 codeHash = Bytes32.wrap(Hash.hash(CODE).getBytes());
    doThrow(new StorageException("write failed")).when(batch).putCode(any(), any());

    assertThatThrownBy(() -> mockedWriter.codes(Map.of(codeHash, CODE))).hasMessage("write failed");
    verify(batch, never()).commit();
    verify(batch).abort();
  }

  // a storage of six slots, the slot at the given index changed if any
  private static SimpleMerklePatriciaTrie<Bytes32, Bytes> storageTrie(final int changedSlot) {
    final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie =
        new SimpleMerklePatriciaTrie<>(Function.identity());
    for (int i = 1; i <= 6; i++) {
      final int value = i == changedSlot ? 100 + i : i;
      trie.put(key(i * 0x20), RLP.encodeOne(UInt256.valueOf(value).trimLeadingZeros()));
    }
    return trie;
  }

  private StorageTask pending(
      final Bytes32 accountHash, final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie) {
    final StorageTask task = new StorageTask(accountHash, root(trie));
    coverage.addStorageTask(task);
    return task;
  }

  private void writeSlots(final Bytes32... accounts) {
    final SyncStateStorage.Batch batch = storages.sync().batch();
    for (final Bytes32 account : accounts) {
      batch.putStorage(account, key(0x20), Bytes32.leftPad(Bytes.of(1)));
    }
    batch.commit();
  }

  // the first slots of a storage, verified as a peer serves them with their proof
  private static StorageRangesRequest.Result served(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie, final int count) {
    return new StorageRangesRequest.Result(
        List.of(servedSlots(trie, RangeManager.MIN_RANGE, firstSlots(trie, count))));
  }

  // the changed storage whole, then the first slots of the other one, with their proof
  private StorageRangesRequest.Result wholeThenCut(final NavigableMap<Bytes32, Bytes> cut) {
    return new StorageRangesRequest.Result(
        List.of(
            served(changed, RangeManager.MIN_RANGE).accounts().getFirst(),
            servedSlots(storage, RangeManager.MIN_RANGE, cut)));
  }

  // the slots of a storage from an origin, verified as a peer serves the last ones of a storage
  private static StorageRangesRequest.Result served(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie, final Bytes32 origin) {
    return new StorageRangesRequest.Result(
        List.of(servedSlots(trie, origin, new TreeMap<>(leaves(trie).tailMap(origin, true)))));
  }

  private static StorageRangesRequest.Slots servedSlots(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie,
      final Bytes32 origin,
      final NavigableMap<Bytes32, Bytes> served) {
    final boolean whole = origin.isZero() && served.size() == leaves(trie).size();
    final List<Bytes> proof = whole ? List.of() : RangeProofs.proof(trie, origin, served.lastKey());
    return verified(trie, origin, new ArrayDeque<>(List.of(served)), proof);
  }

  // no slot from an origin on, as a peer proves it with the proof of the origin alone
  private static StorageRangesRequest.Result provenEmpty(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie, final Bytes32 origin) {
    return new StorageRangesRequest.Result(
        List.of(
            verified(trie, origin, new ArrayDeque<>(), RangeProofs.proof(trie, origin, origin))));
  }

  // an answer verified as the download verifies it
  private static StorageRangesRequest.Slots verified(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie,
      final Bytes32 origin,
      final ArrayDeque<NavigableMap<Bytes32, Bytes>> slots,
      final List<Bytes> proof) {
    return chunk(new StorageTask(Bytes32.ZERO, root(trie)), origin)
        .decode(
            StorageRangeMessage.create(slots, proof).wrapMessageData(BigInteger.ONE),
            mock(EthPeer.class))
        .accounts()
        .getFirst();
  }

  private static StorageRangesRequest chunk(final StorageTask task, final Bytes32 origin) {
    return chunk(task, origin, RangeManager.MAX_RANGE);
  }

  private static StorageRangesRequest chunk(
      final StorageTask task, final Bytes32 origin, final Bytes32 limit) {
    return new StorageRangesRequest(pivot(), List.of(task), origin, limit, 4096, "storage_chunk");
  }

  private static StorageRangesRequest batch(final StorageTask... tasks) {
    return new StorageRangesRequest(
        pivot(),
        List.of(tasks),
        RangeManager.MIN_RANGE,
        RangeManager.MAX_RANGE,
        4096,
        "storage_ranges");
  }

  private static AccountRangeRequest accountRange(final Bytes32 origin, final Bytes32 limit) {
    return new AccountRangeRequest(pivot(), origin, limit, 4096, "account_range");
  }

  private static AccountRangeRequest.Result accounts(final Bytes32 hash, final Bytes account) {
    return new AccountRangeRequest.Result(List.of(hash), List.of(account), true);
  }

  private static NavigableMap<Bytes32, Bytes> leaves(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie) {
    return new TreeMap<>(trie.entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE));
  }

  private static NavigableMap<Bytes32, Bytes> firstSlots(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie, final int count) {
    return new TreeMap<>(trie.entriesFrom(Bytes32.ZERO, count));
  }

  private static Bytes account(final Hash storageRoot, final Hash codeHash) {
    return Accounts.encode(new PatriciaTrieAccountValue(1, Wei.of(1), storageRoot, codeHash));
  }

  private static Bytes32 root(final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie) {
    return Bytes32.wrap(trie.getRootHash());
  }

  private static Bytes32 next(final Bytes32 key) {
    return KeyRangeSet.next(key).orElseThrow();
  }

  private static Pivot pivot() {
    return new Pivot(0, new BlockHeaderTestFixture().number(1).buildHeader());
  }

  private static Bytes32 key(final int firstByte) {
    return Bytes32.wrap(Bytes.concatenate(Bytes.of(firstByte), Bytes.wrap(new byte[31])));
  }

  // a writer whose first commit holding a node at such a location fails, dropping what it holds
  private StateWriter writerFailingOnce(final Predicate<Bytes> nodeLocation) {
    return new StateWriter(
        FailingTrieWriter.failingOnce(storages.sync(), nodeLocation, new AtomicBoolean()),
        coverage);
  }
}
