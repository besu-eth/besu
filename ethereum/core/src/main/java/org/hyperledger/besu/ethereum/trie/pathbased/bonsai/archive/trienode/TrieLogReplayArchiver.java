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

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldView.encodeTrieValue;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Archives a block's trie nodes after the block was imported, by replaying its trie log over the
 * trie-node archive. It needs no world state. It opens the account and storage tries at the parent
 * block from the archive, applies the trie log's changes, and writes the nodes that changed as the
 * block's history. Each one is stored against the node read at the parent.
 *
 * <p>Not thread safe: the trailing archiver calls it from one thread. Owns {@code storageTriePool}.
 */
public class TrieLogReplayArchiver implements Closeable {

  /** Thrown when a block can't be archived and retrying won't help. */
  public static final class UnarchivableBlockException extends IllegalStateException {
    UnarchivableBlockException(final String message) {
      super(message);
    }
  }

  private final SegmentedKeyValueStorage storage;
  private final ArchiveHistoryReader reader;
  private final ArchiveTrieNodeWriter writer;
  private final Function<Hash, Optional<TrieLog>> trieLogs;
  private final ExecutorService storageTriePool;

  // Nodes this archiver wrote, by natural key. A load checks the hash, so an entry from a stale or
  // rolled back block is skipped.
  private final Cache<Bytes, Bytes> recentNodes;

  public TrieLogReplayArchiver(
      final SegmentedKeyValueStorage storage,
      final ArchiveNodeHistoryStore historyStore,
      final ArchiveTrieNodeWriter writer,
      final Function<Hash, Optional<TrieLog>> trieLogs,
      final ExecutorService storageTriePool,
      final long recentNodesCacheBytes) {
    this.storage = storage;
    this.reader = new ArchiveHistoryReader(historyStore);
    this.writer = writer;
    this.trieLogs = trieLogs;
    this.storageTriePool = storageTriePool;
    this.recentNodes =
        Caffeine.newBuilder()
            .maximumWeight(recentNodesCacheBytes)
            .<Bytes, Bytes>weigher((key, node) -> key.size() + node.size())
            .build();
  }

  /**
   * Archives {@code block}, which must be the child of {@code parent}, the last archived block.
   *
   * @throws UnarchivableBlockException if the trie log is missing or replays to the wrong root
   */
  public void archive(final BlockHeader parent, final BlockHeader block) {
    final TrieLog trieLog =
        trieLogs
            .apply(block.getBlockHash())
            .orElseThrow(
                () ->
                    new UnarchivableBlockException(
                        "no trie log for block " + block.getNumber() + " " + block.getBlockHash()));

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    final BlockReplay replay = new BlockReplay(block.getNumber(), tx);
    boolean committed = false;
    try {
      writer.beginBlock(tx, block);
      final Hash root = applyTrieLog(replay, trieLog, parent.getStateRoot());
      if (!root.equals(block.getStateRoot())) {
        throw new UnarchivableBlockException(
            String.format(
                "trie log for block %d replays to state root %s, expected %s",
                block.getNumber(), root, block.getStateRoot()));
      }
      writer.onBeforeCommit(tx);
      tx.commit();
      committed = true;
    } finally {
      if (!committed) {
        writer.onRollback(tx);
        tx.rollback();
      }
    }
  }

  @Override
  public void close() {
    storageTriePool.shutdownNow();
  }

  /** One block's replay: its transaction, and the nodes it read at the parent. */
  private record BlockReplay(
      long block, SegmentedKeyValueStorageTransaction tx, Map<Bytes, Bytes> priors) {
    BlockReplay(final long block, final SegmentedKeyValueStorageTransaction tx) {
      this(block, tx, new ConcurrentHashMap<>());
    }
  }

  private Optional<Bytes> load(
      final BlockReplay replay, final Bytes naturalKey, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    final Predicate<Bytes> hasNodeHash = node -> Hash.hash(node).getBytes().equals(nodeHash);
    final Optional<Bytes> node =
        Optional.ofNullable(recentNodes.getIfPresent(naturalKey))
            .filter(hasNodeHash)
            .or(() -> reader.nodeAt(naturalKey, replay.block() - 1).filter(hasNodeHash));
    node.ifPresent(n -> replay.priors().put(naturalKey, n));
    return node;
  }

  private void capture(
      final BlockReplay replay, final Bytes naturalKey, final Bytes location, final Bytes node) {
    final Bytes prior = replay.priors().get(naturalKey);
    if (node.equals(prior)) {
      return;
    }
    // a node not read this block has no prior and is written FULL
    writer.capture(naturalKey, location, node, prior, replay.tx());
    recentNodes.put(naturalKey, node);
  }

  private MerkleTrie<Bytes, Bytes> trie(
      final BlockReplay replay, final Function<Bytes, Bytes> keyOf, final Bytes32 root) {
    return new StoredMerklePatriciaTrie<>(
        (location, hash) -> load(replay, keyOf.apply(location), hash),
        root,
        Function.identity(),
        Function.identity());
  }

  private Hash applyTrieLog(
      final BlockReplay replay, final TrieLog trieLog, final Hash parentStateRoot) {
    final MerkleTrie<Bytes, Bytes> accountTrie =
        trie(replay, ArchiveNodeKey::account, Bytes32.wrap(parentStateRoot.getBytes()));
    final Map<Address, CompletableFuture<Bytes>> updated =
        updateStorageTries(replay, trieLog, accountTrie);
    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) -> {
              final AccountValue account = change.getUpdated();
              if (account == null) {
                accountTrie.remove(address.addressHash().getBytes());
              } else if (!change.isUnchanged()) {
                updated.putIfAbsent(
                    address,
                    CompletableFuture.completedFuture(
                        encodeAccount(account, account.getStorageRoot())));
              }
            });
    updated.forEach(
        (address, account) -> accountTrie.put(address.addressHash().getBytes(), account.join()));

    accountTrie.commit(
        (location, hash, value) ->
            capture(replay, ArchiveNodeKey.account(location), location, value));
    return Hash.wrap(accountTrie.getRootHash());
  }

  /** Starts each changed storage trie, giving its account encoded with the new storage root. */
  private Map<Address, CompletableFuture<Bytes>> updateStorageTries(
      final BlockReplay replay, final TrieLog trieLog, final MerkleTrie<Bytes, Bytes> accountTrie) {
    final Map<Address, CompletableFuture<Bytes>> updated = new HashMap<>();
    trieLog
        .getStorageChanges()
        .forEach(
            (address, slots) -> {
              final TrieLog.LogTuple<AccountValue> change =
                  trieLog.getAccountChanges().get(address);
              if (change != null && change.getUpdated() == null) {
                return; // deleted, so its storage trie isn't committed
              }
              // an account with only storage changes keeps its other fields from the parent
              final AccountValue account =
                  change == null ? readAccount(accountTrie, address) : change.getUpdated();
              final AccountValue prior = change == null ? account : change.getPrior();
              final Hash priorRoot = prior == null ? Hash.EMPTY_TRIE_HASH : prior.getStorageRoot();
              updated.put(
                  address,
                  CompletableFuture.supplyAsync(
                          () -> updateStorage(replay, address, priorRoot, slots), storageTriePool)
                      .thenApply(root -> encodeAccount(account, root)));
            });
    return updated;
  }

  private AccountValue readAccount(
      final MerkleTrie<Bytes, Bytes> accountTrie, final Address address) {
    return accountTrie
        .get(address.addressHash().getBytes())
        .map(rlp -> PatriciaTrieAccountValue.readFrom(RLP.input(rlp)))
        .orElseThrow(
            () -> new UnarchivableBlockException("storage change for missing account " + address));
  }

  private Bytes encodeAccount(final AccountValue fields, final Hash storageRoot) {
    final AccountValue account =
        new PatriciaTrieAccountValue(
            fields.getNonce(), fields.getBalance(), storageRoot, fields.getCodeHash());
    return RLP.encode(account::writeTo);
  }

  private Hash updateStorage(
      final BlockReplay replay,
      final Address address,
      final Hash priorRoot,
      final Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>> slots) {
    final Bytes accountHash = address.addressHash().getBytes();
    final MerkleTrie<Bytes, Bytes> trie =
        trie(
            replay,
            location -> ArchiveNodeKey.storage(accountHash, location),
            Bytes32.wrap(priorRoot.getBytes()));
    for (final var slot : slots.entrySet()) {
      if (slot.getValue().isUnchanged()) {
        continue;
      }
      final Bytes slotHash = slot.getKey().getSlotHash().getBytes();
      final UInt256 updated = slot.getValue().getUpdated();
      if (updated == null || updated.isZero()) {
        trie.remove(slotHash);
      } else {
        trie.put(slotHash, encodeTrieValue(updated));
      }
    }
    trie.commit(
        (location, hash, value) ->
            capture(replay, ArchiveNodeKey.storage(accountHash, location), location, value));
    return Hash.wrap(trie.getRootHash());
  }
}
