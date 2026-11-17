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
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore.ArchivedBlock;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Archives one block's trie nodes without a world state: it opens the account and storage tries at
 * the parent from the trie-node archive, applies the block's trie log, and captures the committed
 * nodes. The node read while applying the trie log is also the capture's prior.
 *
 * <p>Not thread safe: one archiver drives it. Owns {@code storageTriePool}.
 */
public class TrieNodeArchiveStep implements Closeable {

  /** Thrown when a block can't be archived and retrying won't help. */
  public static final class UnarchivableBlockException extends IllegalStateException {
    UnarchivableBlockException(final String message) {
      super(message);
    }
  }

  private final SegmentedKeyValueStorage storage;
  private final ArchiveNodeHistoryStore historyStore;
  private final ArchiveHistoryReader reader;
  private final ArchiveTrieNodeWriter writer;
  private final Function<Hash, Optional<TrieLog>> trieLogs;
  private final ExecutorService storageTriePool;

  // Nodes this step wrote, by natural key; only valid while it is the only writer.
  private final Cache<Bytes, Bytes> recentNodes;
  private Optional<ArchivedBlock> lastArchived = Optional.empty();

  public TrieNodeArchiveStep(
      final SegmentedKeyValueStorage storage,
      final ArchiveNodeHistoryStore historyStore,
      final ArchiveTrieNodeWriter writer,
      final Function<Hash, Optional<TrieLog>> trieLogs,
      final ExecutorService storageTriePool,
      final long recentNodesCacheBytes) {
    this.storage = storage;
    this.historyStore = historyStore;
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
   * @return false if the last archived block is no longer {@code parent}, so nothing was written
   * @throws UnarchivableBlockException if the trie log is missing or replays to the wrong root
   */
  public boolean archive(final BlockHeader parent, final BlockHeader block) {
    final ArchivedBlock parentBlock = new ArchivedBlock(parent.getNumber(), parent.getBlockHash());
    if (!isLastArchived(parentBlock)) {
      return false;
    }
    if (!lastArchived.equals(Optional.of(parentBlock))) {
      // inline capture wrote blocks since this step last ran
      recentNodes.invalidateAll();
    }
    final TrieLog trieLog =
        trieLogs
            .apply(block.getBlockHash())
            .orElseThrow(
                () ->
                    new UnarchivableBlockException(
                        "no trie log for block " + block.getNumber() + " " + block.getBlockHash()));

    final BlockReplay replay = new BlockReplay(block.getNumber());
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    boolean committed = false;
    try {
      writer.beginBlock(tx, block);
      final Hash root = replay.apply(trieLog, parent.getStateRoot(), tx);
      if (!root.equals(block.getStateRoot())) {
        throw new UnarchivableBlockException(
            String.format(
                "trie log for block %d replays to state root %s, expected %s",
                block.getNumber(), root, block.getStateRoot()));
      }
      writer.onBeforeCommit(tx);
      // the archiver is the only other writer; inline capture never writes a persisted block
      if (!isLastArchived(parentBlock)) {
        return false;
      }
      tx.commit();
      committed = true;
    } finally {
      if (!committed) {
        writer.onRollback(tx);
        tx.rollback();
      }
    }
    recentNodes.putAll(replay.written);
    lastArchived = Optional.of(new ArchivedBlock(block.getNumber(), block.getBlockHash()));
    return true;
  }

  @Override
  public void close() {
    storageTriePool.shutdownNow();
  }

  private boolean isLastArchived(final ArchivedBlock block) {
    return historyStore.getArchivedBlock().filter(block::equals).isPresent();
  }

  /** One block's replay: the nodes it read at the parent and the nodes it wrote. */
  private final class BlockReplay {
    private final long block;
    private final Map<Bytes, Bytes> priors = new ConcurrentHashMap<>();
    private final Map<Bytes, Bytes> written = new ConcurrentHashMap<>();

    BlockReplay(final long block) {
      this.block = block;
    }

    private Optional<Bytes> load(final Bytes naturalKey, final Bytes32 nodeHash) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
      }
      final Optional<Bytes> node =
          Optional.ofNullable(recentNodes.getIfPresent(naturalKey))
              .filter(n -> hashOf(n).equals(nodeHash))
              .or(() -> reader.nodeAt(naturalKey, block - 1))
              .filter(n -> hashOf(n).equals(nodeHash));
      node.ifPresent(n -> priors.put(naturalKey, n));
      return node;
    }

    private void capture(
        final Bytes naturalKey,
        final Bytes location,
        final Bytes node,
        final SegmentedKeyValueStorageTransaction tx) {
      final Bytes prior = priors.get(naturalKey);
      if (node.equals(prior)) {
        return;
      }
      // a node not read this block has no prior and is written FULL
      writer.capture(naturalKey, location, node, prior, tx);
      written.put(naturalKey, node);
    }

    private MerkleTrie<Bytes, Bytes> trie(final Function<Bytes, Bytes> keyOf, final Bytes32 root) {
      return new StoredMerklePatriciaTrie<>(
          (location, hash) -> load(keyOf.apply(location), hash),
          root,
          Function.identity(),
          Function.identity());
    }

    Hash apply(
        final TrieLog trieLog,
        final Hash parentStateRoot,
        final SegmentedKeyValueStorageTransaction tx) {
      final Map<Address, ? extends TrieLog.LogTuple<AccountValue>> accounts =
          trieLog.getAccountChanges();
      final Map<Address, ? extends Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>>>
          storageChanges = trieLog.getStorageChanges();
      final MerkleTrie<Bytes, Bytes> accountTrie =
          trie(ArchiveNodeKey::account, Bytes32.wrap(parentStateRoot.getBytes()));

      // An account with only storage changes keeps its other fields from the parent.
      final Map<Address, AccountFields> storageOnly = new HashMap<>();
      for (final Address address : storageChanges.keySet()) {
        if (!accounts.containsKey(address)) {
          storageOnly.put(
              address,
              accountTrie
                  .get(address.addressHash().getBytes())
                  .map(AccountFields::decode)
                  .orElseThrow(
                      () ->
                          new UnarchivableBlockException(
                              "storage change for missing account " + address)));
        }
      }

      final Map<Address, CompletableFuture<Hash>> storageRoots = new HashMap<>();
      for (final var entry : storageChanges.entrySet()) {
        final Address address = entry.getKey();
        final TrieLog.LogTuple<AccountValue> change = accounts.get(address);
        if (change != null && change.getUpdated() == null) {
          continue; // deleted, so its storage trie isn't committed
        }
        final Hash priorRoot =
            change != null
                ? Optional.ofNullable(change.getPrior())
                    .map(AccountValue::getStorageRoot)
                    .orElse(Hash.EMPTY_TRIE_HASH)
                : storageOnly.get(address).storageRoot();
        storageRoots.put(
            address,
            CompletableFuture.supplyAsync(
                () -> updateStorage(address, priorRoot, entry.getValue(), tx), storageTriePool));
      }

      final Set<Address> touched = new HashSet<>(accounts.keySet());
      touched.addAll(storageOnly.keySet());
      for (final Address address : touched) {
        final Bytes addressHash = address.addressHash().getBytes();
        final TrieLog.LogTuple<AccountValue> change = accounts.get(address);
        final CompletableFuture<Hash> storageRoot = storageRoots.get(address);
        if (change != null && change.getUpdated() == null) {
          accountTrie.remove(addressHash);
          continue;
        }
        if (change != null && change.isUnchanged() && storageRoot == null) {
          continue;
        }
        final AccountFields fields =
            change != null ? AccountFields.of(change.getUpdated()) : storageOnly.get(address);
        final Hash root = storageRoot != null ? join(storageRoot) : fields.storageRoot();
        accountTrie.put(addressHash, fields.withStorageRoot(root).encode());
      }

      accountTrie.commit(
          (location, hash, value) ->
              capture(ArchiveNodeKey.account(location), location, value, tx));
      return Hash.wrap(accountTrie.getRootHash());
    }

    private Hash updateStorage(
        final Address address,
        final Hash priorRoot,
        final Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>> slots,
        final SegmentedKeyValueStorageTransaction tx) {
      final Bytes accountHash = address.addressHash().getBytes();
      final MerkleTrie<Bytes, Bytes> trie =
          trie(
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
              capture(ArchiveNodeKey.storage(accountHash, location), location, value, tx));
      return Hash.wrap(trie.getRootHash());
    }
  }

  private static Bytes32 hashOf(final Bytes node) {
    return Bytes32.wrap(Hash.hash(node).getBytes());
  }

  private static Hash join(final CompletableFuture<Hash> future) {
    try {
      return future.join();
    } catch (final CompletionException e) {
      if (e.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      throw e;
    }
  }

  private record AccountFields(long nonce, UInt256 balance, Hash storageRoot, Hash codeHash) {
    static AccountFields of(final AccountValue value) {
      return new AccountFields(
          value.getNonce(),
          value.getBalance().toUInt256(),
          value.getStorageRoot(),
          value.getCodeHash());
    }

    static AccountFields decode(final Bytes rlp) {
      final RLPInput in = RLP.input(rlp);
      in.enterList();
      final long nonce = in.readLongScalar();
      final UInt256 balance = in.readUInt256Scalar();
      final Hash storageRoot = Hash.wrap(in.readBytes32());
      final Hash codeHash = Hash.wrap(in.readBytes32());
      in.leaveList();
      return new AccountFields(nonce, balance, storageRoot, codeHash);
    }

    AccountFields withStorageRoot(final Hash root) {
      return new AccountFields(nonce, balance, root, codeHash);
    }

    Bytes encode() {
      final BytesValueRLPOutput out = new BytesValueRLPOutput();
      out.startList();
      out.writeLongScalar(nonce);
      out.writeUInt256Scalar(balance);
      out.writeBytes(storageRoot.getBytes());
      out.writeBytes(codeHash.getBytes());
      out.endList();
      return out.encoded();
    }
  }
}
