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
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/** A Bonsai chain whose blocks are captured inline while they are final. */
final class ArchiveChainFixture implements AutoCloseable {

  static final Address CONTRACT =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  final Blockchain blockchain = mock(Blockchain.class);
  final List<BlockHeader> headers = new ArrayList<>();
  final BonsaiWorldStateKeyValueStorage worldStateStorage;
  final SegmentedKeyValueStorage liveStorage;
  final ArchiveNodeHistoryStore history;
  final ArchiveTrieNodeWriter writer;
  final BonsaiWorldStateProvider provider;
  private final ArchiveTrieNodeStrategy strategy;
  private final BonsaiWorldState worldState;

  // Blocks up to here are final.
  volatile long finalThrough = Long.MAX_VALUE;

  ArchiveChainFixture() {
    lenient()
        .when(blockchain.getBlockHeader(anyLong()))
        .thenAnswer(
            inv -> {
              final long number = inv.getArgument(0);
              return number < headers.size()
                  ? Optional.of(headers.get((int) number))
                  : Optional.empty();
            });
    lenient()
        .when(blockchain.getBlockHeader(any(Hash.class)))
        .thenAnswer(
            inv ->
                headers.stream()
                    .filter(h -> h.getBlockHash().equals(inv.getArgument(0)))
                    .findFirst());
    provider = InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);
    worldStateStorage = provider.getWorldStateKeyValueStorage();
    liveStorage = worldStateStorage.getComposedWorldStateStorage();
    history = new ArchiveNodeHistoryStore(liveStorage);
    writer =
        new ArchiveTrieNodeWriter(
            history,
            Executors.newFixedThreadPool(2),
            DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
            DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL);
    strategy =
        new ArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(), writer, history, liveStorage, b -> b <= finalThrough);
    worldStateStorage.setTrieNodeStrategy(strategy);
    worldState = (BonsaiWorldState) provider.getWorldState();
  }

  TrieLogReplayArchiver newReplayArchiver() {
    return new TrieLogReplayArchiver(
        liveStorage,
        history,
        writer,
        hash -> provider.getTrieLogManager().getTrieLogLayer(hash),
        Executors.newFixedThreadPool(2),
        16L * 1024 * 1024);
  }

  /** Genesis: a contract with enough slots for a branch root, and enough accounts for one too. */
  BlockHeader persistGenesis() {
    return persistBlock(
        updater -> {
          final MutableAccount contract = updater.createAccount(CONTRACT, 1, Wei.of(1L));
          for (int slot = 0; slot < 32; slot++) {
            contract.setStorageValue(UInt256.valueOf(slot), UInt256.valueOf(slot + 1L));
          }
          for (int i = 0; i < 32; i++) {
            updater.createAccount(account(i), 1, Wei.of(1L));
          }
        });
  }

  /** Changes balances and storage, deletes slots, and creates and deletes accounts. */
  BlockHeader persistBusyBlock() {
    final long n = headers.size();
    return persistBlock(
        updater -> {
          updater.getAccount(account((int) (n % 32))).setBalance(Wei.of(n + 10));
          final MutableAccount contract = updater.getAccount(CONTRACT);
          contract.setStorageValue(UInt256.valueOf(n % 32), UInt256.valueOf(n * 100));
          contract.setStorageValue(UInt256.valueOf((n + 7) % 32), UInt256.ZERO);
          final MutableAccount created = updater.createAccount(account(100 + (int) n), 0, Wei.ONE);
          created.setStorageValue(UInt256.ONE, UInt256.valueOf(n));
          if (n > 2) {
            updater.deleteAccount(account(100 + (int) n - 2));
          }
        });
  }

  BlockHeader persistEmptyBlock() {
    return persistBlock(updater -> {});
  }

  BlockHeader persistBlock(final Consumer<WorldUpdater> changes) {
    final WorldUpdater updater = worldState.updater();
    changes.accept(updater);
    updater.commit();
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(headers.size())
            .parentHash(headers.isEmpty() ? Hash.EMPTY : headers.getLast().getHash())
            .stateRoot(worldState.frontierRootHash())
            .buildHeader();
    headers.add(header);
    worldState.persist(header);
    return header;
  }

  long persistedHead() {
    return headers.size() - 1L;
  }

  long lastArchived() {
    return history
        .getArchivedBlock()
        .map(ArchiveNodeHistoryStore.ArchivedBlock::number)
        .orElse(-1L);
  }

  static Address account(final int i) {
    return Address.fromHexString(String.format("0x%040x", 0x2000 + i));
  }

  /** Walks the block's account trie and every storage trie from the archive alone. */
  void assertArchived(final long block) {
    final ArchiveHistoryReader reader = new ArchiveHistoryReader(history);
    final Map<Bytes32, Bytes> accounts =
        walk(reader, block, headers.get((int) block).getStateRoot(), ArchiveNodeKey::account);
    accounts.forEach(
        (accountHash, rlp) -> {
          final Bytes32 storageRoot = storageRootOf(rlp);
          walk(
              reader,
              block,
              Hash.wrap(storageRoot),
              loc -> ArchiveNodeKey.storage(accountHash, loc));
        });
  }

  private static Map<Bytes32, Bytes> walk(
      final ArchiveHistoryReader reader,
      final long block,
      final Hash root,
      final Function<Bytes, Bytes> keyOf) {
    return new StoredMerklePatriciaTrie<Bytes, Bytes>(
            (location, hash) -> {
              final Optional<Bytes> node = reader.nodeAt(keyOf.apply(location), block);
              assertThat(node)
                  .as("block %d node at %s", block, location)
                  .map(n -> Hash.hash(n).getBytes())
                  .contains(hash);
              return node;
            },
            Bytes32.wrap(root.getBytes()),
            Function.identity(),
            Function.identity())
        .entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE);
  }

  private static Bytes32 storageRootOf(final Bytes accountRlp) {
    final RLPInput in = RLP.input(accountRlp);
    in.enterList();
    in.skipNext();
    in.skipNext();
    return in.readBytes32();
  }

  @Override
  public void close() {
    strategy.close();
  }
}
