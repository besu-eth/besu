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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.TrieLogReplayArchiver.UnarchivableBlockException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TrieLogReplayArchiverTest {

  private final ArchiveChainFixture chain = new ArchiveChainFixture();
  private final TrieLogReplayArchiver replayArchiver = chain.newReplayArchiver();

  @AfterEach
  void tearDown() {
    replayArchiver.close();
    chain.close();
  }

  @Test
  void replayedBlocksMatchTheirStateRoots() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    for (int i = 0; i < 6; i++) {
      chain.persistBusyBlock();
    }
    chain.persistEmptyBlock();
    chain.persistBusyBlock();
    assertThat(chain.lastArchived()).isZero();

    for (int b = 1; b <= chain.persistedHead(); b++) {
      replayArchiver.archive(chain.headers.get(b - 1), chain.headers.get(b));
    }

    assertThat(chain.lastArchived()).isEqualTo(chain.persistedHead());
    for (int b = 0; b <= chain.persistedHead(); b++) {
      chain.assertArchived(b);
    }
  }

  @Test
  void replayedNodesDiffAgainstTheParent() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    chain.persistBusyBlock();

    replayArchiver.archive(chain.headers.get(0), chain.headers.get(1));

    // the account trie's root branch changed, and diffs against block 0's
    assertThat(
            chain
                .history
                .getLatestBefore(ArchiveNodeKey.account(Bytes.EMPTY), 1L)
                .map(ArchiveNodeHistoryStore.HistoryEntry::counter))
        .contains(1);
  }

  @Test
  void replaysStorageChangesWithoutAnAccountChange() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    final BlockHeader block1 = chain.persistBusyBlock();
    // a trie log read back from storage leaves out accounts whose value didn't change
    final TrieLogReplayArchiver storageOnlyArchiver =
        new TrieLogReplayArchiver(
            chain.liveStorage,
            chain.history,
            chain.writer,
            hash ->
                chain
                    .provider
                    .getTrieLogManager()
                    .getTrieLogLayer(hash)
                    .map(this::withoutContractAccountChange),
            Executors.newFixedThreadPool(1),
            1024);

    storageOnlyArchiver.archive(chain.headers.get(0), block1);

    assertThat(chain.lastArchived()).isEqualTo(1);
    chain.assertArchived(1);
    storageOnlyArchiver.close();
  }

  private TrieLog withoutContractAccountChange(final TrieLog trieLog) {
    assertThat(trieLog.getAccountChanges()).containsKey(ArchiveChainFixture.CONTRACT);
    assertThat(trieLog.getStorageChanges()).containsKey(ArchiveChainFixture.CONTRACT);
    return copyOf(trieLog, address -> !address.equals(ArchiveChainFixture.CONTRACT));
  }

  @Test
  void archivesTheSameWithoutACache() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    for (int i = 0; i < 5; i++) {
      chain.persistBusyBlock();
    }
    final TrieLogReplayArchiver uncached = replayArchiverWith(this::trieLog, 0);

    for (int b = 1; b <= chain.persistedHead(); b++) {
      uncached.archive(chain.headers.get(b - 1), chain.headers.get(b));
    }

    for (int b = 0; b <= chain.persistedHead(); b++) {
      chain.assertArchived(b);
    }
    uncached.close();
  }

  @Test
  void archivesABlockAfterAFailedAttemptAtIt() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    final BlockHeader block1 = chain.persistBusyBlock();
    chain.persistBusyBlock();
    final AtomicBoolean tamper = new AtomicBoolean(true);
    // the first attempt replays a wrong slot value, so its nodes are rolled back
    final TrieLogReplayArchiver retrying =
        replayArchiverWith(
            hash ->
                trieLog(hash)
                    .map(
                        log ->
                            hash.equals(block1.getBlockHash()) && tamper.getAndSet(false)
                                ? withContractSlot(log, UInt256.valueOf(999))
                                : log),
            16L * 1024 * 1024);
    final Optional<Bytes> rootBefore = rootEntryAt(1L);

    assertThatThrownBy(() -> retrying.archive(chain.headers.get(0), block1))
        .isInstanceOf(UnarchivableBlockException.class);
    assertThat(chain.lastArchived()).isZero();
    assertThat(rootEntryAt(1L)).isEqualTo(rootBefore);

    retrying.archive(chain.headers.get(0), block1);
    retrying.archive(block1, chain.headers.get(2));

    assertThat(chain.lastArchived()).isEqualTo(2);
    chain.assertArchived(1);
    chain.assertArchived(2);
    retrying.close();
  }

  @Test
  void storageChangeForAMissingAccountIsUnarchivable() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    final BlockHeader block1 = chain.persistBusyBlock();
    final Address missing = Address.fromHexString("0x00000000000000000000000000000000000000ff");
    final TrieLogReplayArchiver missingAccountArchiver =
        replayArchiverWith(
            hash ->
                trieLog(hash)
                    .map(
                        log ->
                            copyOf(log, address -> true)
                                .addStorageChange(
                                    missing, new StorageSlotKey(UInt256.ONE), null, UInt256.ONE)),
            1024);

    assertThatThrownBy(() -> missingAccountArchiver.archive(chain.headers.get(0), block1))
        .isInstanceOf(UnarchivableBlockException.class)
        .hasMessageContaining("storage change for missing account");
    assertThat(chain.lastArchived()).isZero();
    missingAccountArchiver.close();
  }

  @Test
  void replaysAnAccountDeletedAndRecreatedInOneBlock() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    chain.persistBlock(
        updater -> {
          updater.deleteAccount(ArchiveChainFixture.CONTRACT);
          final MutableAccount recreated =
              updater.createAccount(ArchiveChainFixture.CONTRACT, 1, Wei.of(1L));
          // slot 0 keeps its old value, the other old slots are gone
          recreated.setStorageValue(UInt256.ZERO, UInt256.ONE);
          recreated.setStorageValue(UInt256.valueOf(40), UInt256.valueOf(5));
        });

    replayArchiver.archive(chain.headers.get(0), chain.headers.get(1));

    assertThat(chain.lastArchived()).isEqualTo(1);
    chain.assertArchived(1);
  }

  @Test
  void replaysNonceAndCodeChanges() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    chain.persistBlock(
        updater -> {
          updater.getAccount(ArchiveChainFixture.account(1)).setNonce(7);
          updater.getAccount(ArchiveChainFixture.account(2)).setCode(Bytes.fromHexString("0x6001"));
          updater
              .createAccount(ArchiveChainFixture.account(200), 0, Wei.ZERO)
              .setCode(Bytes.fromHexString("0x6002"));
        });
    assertThat(chain.headers.get(1).getStateRoot())
        .isNotEqualTo(chain.headers.get(0).getStateRoot());

    replayArchiver.archive(chain.headers.get(0), chain.headers.get(1));

    assertThat(chain.lastArchived()).isEqualTo(1);
    chain.assertArchived(1);
  }

  @Test
  void replaysABlockThatWritesBackTheSameValues() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    chain.persistBlock(
        updater -> {
          updater.getAccount(ArchiveChainFixture.account(3)).setBalance(Wei.of(1L));
          updater
              .getAccount(ArchiveChainFixture.CONTRACT)
              .setStorageValue(UInt256.valueOf(4), UInt256.valueOf(5));
        });
    assertThat(chain.headers.get(1).getStateRoot()).isEqualTo(chain.headers.get(0).getStateRoot());

    replayArchiver.archive(chain.headers.get(0), chain.headers.get(1));

    assertThat(chain.lastArchived()).isEqualTo(1);
    chain.assertArchived(1);
  }

  private Optional<TrieLog> trieLog(final Hash blockHash) {
    return chain.provider.getTrieLogManager().getTrieLogLayer(blockHash);
  }

  private TrieLogReplayArchiver replayArchiverWith(
      final Function<Hash, Optional<TrieLog>> trieLogs, final long cacheBytes) {
    return new TrieLogReplayArchiver(
        chain.liveStorage,
        chain.history,
        chain.writer,
        trieLogs,
        Executors.newFixedThreadPool(1),
        cacheBytes);
  }

  private Optional<Bytes> rootEntryAt(final long block) {
    return chain
        .history
        .getLatestBefore(ArchiveNodeKey.account(Bytes.EMPTY), block)
        .map(ArchiveNodeHistoryStore.HistoryEntry::rawEntryBytes);
  }

  private TrieLogLayer copyOf(final TrieLog trieLog, final Predicate<Address> keepAccount) {
    final TrieLogLayer copy = new TrieLogLayer();
    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) -> {
              if (keepAccount.test(address)) {
                copy.addAccountChange(address, change.getPrior(), change.getUpdated());
              }
            });
    trieLog
        .getStorageChanges()
        .forEach(
            (address, slots) ->
                slots.forEach(
                    (slot, change) ->
                        copy.addStorageChange(
                            address, slot, change.getPrior(), change.getUpdated())));
    return copy;
  }

  private TrieLog withContractSlot(final TrieLog trieLog, final UInt256 value) {
    final TrieLogLayer copy = copyOf(trieLog, address -> true);
    final var slot =
        copy.getStorageChanges(ArchiveChainFixture.CONTRACT).entrySet().iterator().next();
    copy.addStorageChange(
        ArchiveChainFixture.CONTRACT, slot.getKey(), slot.getValue().getPrior(), value);
    return copy;
  }

  @Test
  void wrongStateRootIsUnarchivable() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    final BlockHeader block1 = chain.persistBusyBlock();
    final BlockHeader wrongRoot =
        new BlockHeaderTestFixture()
            .number(1)
            .parentHash(block1.getParentHash())
            .stateRoot(Hash.EMPTY_TRIE_HASH)
            .buildHeader();
    final TrieLogReplayArchiver wrongRootArchiver =
        new TrieLogReplayArchiver(
            chain.liveStorage,
            chain.history,
            chain.writer,
            hash -> chain.provider.getTrieLogManager().getTrieLogLayer(block1.getHash()),
            Executors.newFixedThreadPool(1),
            1024);

    assertThatThrownBy(() -> wrongRootArchiver.archive(chain.headers.get(0), wrongRoot))
        .isInstanceOf(UnarchivableBlockException.class)
        .hasMessageContaining("replays to state root");
    assertThat(chain.lastArchived()).isZero();
    wrongRootArchiver.close();
  }

  @Test
  void missingTrieLogIsUnarchivable() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    chain.persistBusyBlock();
    final TrieLogReplayArchiver noTrieLogs =
        new TrieLogReplayArchiver(
            chain.liveStorage,
            chain.history,
            chain.writer,
            hash -> Optional.empty(),
            Executors.newFixedThreadPool(1),
            1024);

    assertThatThrownBy(() -> noTrieLogs.archive(chain.headers.get(0), chain.headers.get(1)))
        .isInstanceOf(UnarchivableBlockException.class)
        .hasMessageContaining("no trie log");
    assertThat(chain.lastArchived()).isZero();
    noTrieLogs.close();
  }
}
