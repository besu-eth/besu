/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Regression test for <a href="https://github.com/hyperledger/besu/issues/9963">#9963</a>.
 *
 * <p>Bonsai {@code clearStorage()} used to iterate storage entries in batches of 256 and always
 * re-read from {@code Bytes32.ZERO} after each batch deletion. When a contract had more than 256
 * storage slots, re-traversing the heavily mutated trie from zero caused an effectively infinite
 * hang (200 slots: ~28 ms, 257 slots: >10 min).
 */
class BonsaiClearStorageTest {

  private static final Address CONTRACT =
      Address.fromHexString("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

  static Stream<Arguments> storageSlotCounts() {
    return Stream.of(
        Arguments.of(200, "single batch (below 256)"),
        Arguments.of(300, "multi-batch (above 256, reproduces #9963)"),
        Arguments.of(512, "exactly two full batches"),
        Arguments.of(513, "two full batches plus one"));
  }

  /**
   * Verifies that {@code frontierRootHash()} completes promptly after clearing storage, regardless
   * of how many slots the contract has. Before the fix, any count above 256 would hang. Also
   * verifies that all storage slots are actually deleted from the underlying key-value storage.
   */
  @ParameterizedTest(name = "{1} — {0} slots")
  @MethodSource("storageSlotCounts")
  void frontierRootHashCompletesAfterClearingStorage(
      final int slotCount, @SuppressWarnings("unused") final String label) throws Exception {
    final Blockchain blockchain = mock(Blockchain.class);
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);
    populateStorage(archive, slotCount);

    // Clear the contract's storage and commit so storageToClear is populated
    final WorldUpdater updater = archive.getWorldState().updater();
    updater.deleteAccount(CONTRACT);
    updater.commit();

    // frontierRootHash triggers DefaultStateRootCommitter.compute → clearStorage
    final CompletableFuture<Hash> future =
        CompletableFuture.supplyAsync(
            archive.getWorldState()::frontierRootHash,
            r -> {
              final Thread t = new Thread(r);
              t.setDaemon(true);
              t.start();
            });

    final Hash root = future.get(10, TimeUnit.SECONDS);
    assertThat(root).isNotNull();

    // Persist so deletions are flushed to the key-value storage
    archive.getWorldState().persist(null);

    // Verify all storage slots for the contract are gone from the key-value storage
    final SegmentedKeyValueStorage kvStorage =
        archive.getWorldStateKeyValueStorage().getComposedWorldStateStorage();
    final Bytes accountHashPrefix = CONTRACT.addressHash().getBytes();
    final Set<byte[]> remainingSlots =
        kvStorage.getAllKeysThat(
            KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
            key -> Bytes.wrap(key).slice(0, accountHashPrefix.size()).equals(accountHashPrefix));
    assertThat(remainingSlots).isEmpty();
  }

  /**
   * Regression test for <a href="https://github.com/besu-eth/besu/issues/10978">#10978</a>.
   *
   * <p>When a contract is selfdestructed and re-created in the same block with a storage slot whose
   * value matches the pre-destruction persisted value, the {@code isUnchanged()} guard must not
   * skip the write: the storage trie is rebuilt from empty, so every slot must be written.
   */
  @Test
  void storageIsWrittenWhenSelfdestructedContractRecreatedWithIdenticalValuesInSameBlock() {
    final Blockchain blockchain = mock(Blockchain.class);
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);

    final UInt256 slot = UInt256.ONE;
    final UInt256 value = UInt256.valueOf(42);

    // block 1: create contract with slot=value
    final WorldUpdater setup = archive.getWorldState().updater();
    setup.createAccount(CONTRACT, 1, Wei.of(1)).setStorageValue(slot, value);
    setup.commit();
    archive.getWorldState().persist(null);

    // block 2: selfdestruct then recreate with the same slot value (two separate tx commits,
    // mirroring AbstractBlockProcessor which commit()s the block updater after each tx)
    final WorldUpdater blockUpdater = archive.getWorldState().updater();
    final WorldUpdater tx1 = blockUpdater.updater();
    tx1.deleteAccount(CONTRACT);
    tx1.commit();
    blockUpdater.commit();

    final WorldUpdater tx2 = blockUpdater.updater();
    tx2.createAccount(CONTRACT, 0, Wei.of(1)).setStorageValue(slot, value);
    tx2.commit();
    blockUpdater.commit();

    archive.getWorldState().persist(null);

    assertThat(archive.getWorldState().get(CONTRACT).getStorageValue(slot)).isEqualTo(value);
  }

  /**
   * A contract selfdestructed and re-created in the same block with the nonce, balance and slot it
   * had leaves the state root as it was, but its storage trie is still rebuilt from empty, so the
   * account must not be skipped as unchanged.
   */
  @Test
  void stateIsKeptWhenSelfdestructedContractRecreatedWithIdenticalAccountInSameBlock() {
    final Blockchain blockchain = mock(Blockchain.class);
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);

    final UInt256 slot = UInt256.ONE;
    final UInt256 value = UInt256.valueOf(42);

    final WorldUpdater setup = archive.getWorldState().updater();
    setup.createAccount(CONTRACT, 1, Wei.of(1)).setStorageValue(slot, value);
    setup.commit();
    archive.getWorldState().persist(null);
    final Hash rootBefore = archive.getWorldState().rootHash();

    final WorldUpdater blockUpdater = archive.getWorldState().updater();
    final WorldUpdater tx1 = blockUpdater.updater();
    tx1.deleteAccount(CONTRACT);
    tx1.commit();
    blockUpdater.commit();

    final WorldUpdater tx2 = blockUpdater.updater();
    tx2.createAccount(CONTRACT, 1, Wei.of(1)).setStorageValue(slot, value);
    tx2.commit();
    blockUpdater.commit();

    archive.getWorldState().persist(null);

    final BonsaiWorldState worldState = (BonsaiWorldState) archive.getWorldState();
    assertThat(worldState.rootHash()).isEqualTo(rootBefore);
    assertThat(worldState.get(CONTRACT).getStorageValue(slot)).isEqualTo(value);
    final Hash storageRoot = ((BonsaiAccount) worldState.get(CONTRACT)).getStorageRoot();
    assertThat(worldState.getAllAccountStorage(CONTRACT, storageRoot)).hasSize(1);
  }

  /**
   * Regression test for <a href="https://github.com/besu-eth/besu/issues/11522">#11522</a>.
   *
   * <p>After a {@code clearStorage()} on an existing account is committed, reads of slots that are
   * only in the persisted world state must return zero, matching the storage root that is rebuilt
   * from an empty trie.
   */
  @Test
  void persistedStorageIsNotReadAfterClearStorageIsCommitted() {
    final Blockchain blockchain = mock(Blockchain.class);
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);

    final UInt256 clearedSlot = UInt256.ONE;
    final UInt256 overriddenSlot = UInt256.valueOf(3);
    final UInt256 value = UInt256.valueOf(42);

    final WorldUpdater setup = archive.getWorldState().updater();
    final MutableAccount account = setup.createAccount(CONTRACT, 1, Wei.of(1));
    account.setStorageValue(clearedSlot, UInt256.ONE);
    account.setStorageValue(overriddenSlot, UInt256.valueOf(3));
    setup.commit();
    archive.getWorldState().persist(null);

    final WorldUpdater updater = archive.getWorldState().updater();
    final MutableAccount cleared = updater.getAccount(CONTRACT);
    cleared.clearStorage();
    cleared.setStorageValue(overriddenSlot, value);
    updater.commit();

    final MutableAccount reread = archive.getWorldState().updater().getAccount(CONTRACT);
    assertThat(reread.getStorageValue(overriddenSlot)).isEqualTo(value);
    assertThat(reread.getStorageValue(clearedSlot)).isEqualTo(UInt256.ZERO);

    archive.getWorldState().persist(null);

    assertThat(archive.getWorldState().get(CONTRACT).getStorageValue(clearedSlot))
        .isEqualTo(UInt256.ZERO);
    assertThat(archive.getWorldState().rootHash())
        .isEqualTo(rootWithOnlySlot(1, overriddenSlot, value));
  }

  /**
   * Regression test for <a href="https://github.com/besu-eth/besu/issues/11522">#11522</a>.
   *
   * <p>When an account deleted earlier in the block is recreated and then has its storage cleared,
   * the cleared-slot markers recorded by the deletion are dropped. Reads of slots that are only in
   * the persisted world state must still return zero, and must not be written back on persist.
   */
  @Test
  void persistedStorageIsNotReadWhenDeletedAccountIsRecreatedAndClearedInSameBlock() {
    final Blockchain blockchain = mock(Blockchain.class);
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);

    final UInt256 clearedSlot = UInt256.ONE;
    final UInt256 writtenSlot = UInt256.valueOf(3);
    final UInt256 value = UInt256.valueOf(42);

    final WorldUpdater setup = archive.getWorldState().updater();
    final MutableAccount account = setup.createAccount(CONTRACT, 1, Wei.of(1));
    account.setStorageValue(clearedSlot, UInt256.ONE);
    account.setStorageValue(writtenSlot, UInt256.valueOf(3));
    setup.commit();
    archive.getWorldState().persist(null);

    // block 2, one commit per tx as in AbstractBlockProcessor: delete the account, recreate it,
    // then clear its storage and write a slot (the accumulator sequence a CREATE2 redeploy
    // produces)
    final WorldUpdater blockUpdater = archive.getWorldState().updater();
    final WorldUpdater tx1 = blockUpdater.updater();
    tx1.deleteAccount(CONTRACT);
    tx1.commit();
    blockUpdater.commit();
    blockUpdater.markTransactionBoundary();

    final WorldUpdater tx2 = blockUpdater.updater();
    tx2.createAccount(CONTRACT, 0, Wei.of(1));
    tx2.commit();
    blockUpdater.commit();
    blockUpdater.markTransactionBoundary();

    final WorldUpdater tx3 = blockUpdater.updater();
    final MutableAccount created = tx3.getAccount(CONTRACT);
    created.clearStorage();
    created.setStorageValue(writtenSlot, value);
    tx3.commit();
    blockUpdater.commit();
    blockUpdater.markTransactionBoundary();

    assertThat(blockUpdater.get(CONTRACT).getStorageValue(clearedSlot)).isEqualTo(UInt256.ZERO);

    archive.getWorldState().persist(null);

    assertThat(archive.getWorldState().get(CONTRACT).getStorageValue(clearedSlot))
        .isEqualTo(UInt256.ZERO);
    assertThat(archive.getWorldState().rootHash())
        .isEqualTo(rootWithOnlySlot(0, writtenSlot, value));
  }

  private Hash rootWithOnlySlot(final long nonce, final UInt256 slot, final UInt256 value) {
    final BonsaiWorldStateProvider expected =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
            mock(Blockchain.class));
    final WorldUpdater updater = expected.getWorldState().updater();
    updater.createAccount(CONTRACT, nonce, Wei.of(1)).setStorageValue(slot, value);
    updater.commit();
    expected.getWorldState().persist(null);
    return expected.getWorldState().rootHash();
  }

  private void populateStorage(final BonsaiWorldStateProvider archive, final int slots) {
    final WorldUpdater setup = archive.getWorldState().updater();
    final MutableAccount account = setup.createAccount(CONTRACT);
    account.setBalance(Wei.of(1));
    for (int i = 0; i < slots; i++) {
      account.setStorageValue(UInt256.valueOf(i), UInt256.valueOf(i + 1));
    }
    setup.commit();
    archive.getWorldState().persist(null);
  }
}
