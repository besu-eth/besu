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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView.encodeTrieValue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListIndex;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiBalWorldStateUpdater;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * BAL-driven {@link StateRootCommitter}: computes the state root <em>in the background</em> against
 * the parent world state while EVM execution runs on the main thread, then merges the result when
 * {@link #compute} is called.
 *
 * <h2>Why a background computation?</h2>
 *
 * <p>The standard (sequential) approach reads each account and storage slot from the Bonsai trie
 * <em>after</em> EVM execution completes, one by one. For large blocks this serialises trie I/O
 * behind the critical path. The BAL (Block Access List) is built during EVM execution and records
 * every account/storage write with its <em>final</em> value. Because the final values are known
 * ahead of time, this committer can start computing the new trie state on the <em>parent</em> world
 * state in a separate thread as soon as the block header (and BAL) are available — in practice
 * overlapping with EVM execution.
 *
 * <h2>Lifecycle</h2>
 *
 * <ol>
 *   <li><b>Construction</b> – a {@link CompletableFuture} is submitted to the common fork-join
 *       pool. It opens a read-only snapshot of the parent world state, then runs {@link
 *       BalComputation} to:
 *       <ul>
 *         <li>compute each changed account's storage trie root concurrently (pre-launched futures),
 *         <li>update the account trie, and
 *         <li>collect all resulting KV writes as a list of deferred {@link
 *             StateRootComputation.UpdaterWrite} lambdas.
 *       </ul>
 *   <li><b>{@link #compute}</b> – called by {@link
 *       org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState#persist}
 *       after EVM execution finishes. Blocks until the background future completes, then:
 *       <ul>
 *         <li>patches the storage roots back into the EVM accumulator so the trie-log records the
 *             correct per-account storage root, and
 *         <li>returns a {@link StateRootComputation} that carries the root hash and the deferred
 *             writes; the caller applies those writes to the KV storage updater.
 *       </ul>
 * </ol>
 *
 * <h2>Deferred writes pattern</h2>
 *
 * <p>Rather than writing directly to a {@code WorldStateKeyValueStorage.Updater}, each trie
 * mutation is captured as a {@link StateRootComputation.UpdaterWrite} lambda. The caller ({@code
 * PathBasedWorldState.persist}) applies them to the real updater in a single batch.
 *
 * <h2>Storage-root back-propagation</h2>
 *
 * <p>The EVM accumulator ({@link BonsaiWorldStateUpdateAccumulator}) holds mutable {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount} objects whose {@code
 * storageRoot} field defaults to the prior root. After the background computation resolves the new
 * storage roots, {@link #compute} writes them back into the accumulator so that the trie log
 * reflects the correct post-block storage root for each touched account.
 */
public final class BalStateRootCommitter implements StateRootCommitter {

  /**
   * Future that resolves once the background trie computation against the parent world state
   * finishes. Submitted immediately on construction so it can overlap with EVM execution.
   */
  private final CompletableFuture<BackgroundResult> backgroundComputation;

  /**
   * Opens the parent world state and starts computing the state root from {@code bal} in the
   * background.
   *
   * @param protocolContext used to look up the parent block header and open its world state
   * @param blockHeader header of the block being processed (used to find the parent hash)
   * @param bal the Block Access List produced during EVM execution of this block
   */
  public BalStateRootCommitter(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final BlockAccessList bal) {
    this.backgroundComputation =
        CompletableFuture.supplyAsync(
            () -> {
              final BlockAccessListIndex balIndex = new BlockAccessListIndex(bal);
              try (BonsaiWorldState parent =
                  openParentWorldState(protocolContext, blockHeader, balIndex)) {
                return runComputation(parent, bal);
              }
            });
  }

  @Override
  public void cancel() {
    backgroundComputation.cancel(true);
  }

  /**
   * Waits for the background computation to finish, patches storage roots into the EVM accumulator,
   * and returns the {@link StateRootComputation} carrying the root hash and deferred KV writes.
   *
   * <p>The BAL-computed root is the authoritative source. If it does not match the block header
   * state root, an {@link IllegalStateException} is thrown.
   */
  @Override
  public StateRootComputation compute(
      final MutableWorldState worldState, final BlockHeader blockHeader) {
    final BackgroundResult result = awaitBackgroundComputation(backgroundComputation);
    final BonsaiWorldStateUpdateAccumulator acc =
        (BonsaiWorldStateUpdateAccumulator) ((BonsaiWorldState) worldState).getAccumulator();
    result
        .storageRoots()
        .forEach(
            (address, newStorageRoot) -> {
              final var entry = acc.getAccountsToUpdate().get(address);
              if (entry != null && entry.getUpdated() != null) {
                entry.getUpdated().setStorageRoot(newStorageRoot);
              }
            });

    if (blockHeader != null && !result.root().equals(blockHeader.getStateRoot())) {
      throw new IllegalStateException(
          "BAL-computed root does not match block header state root: expected "
              + blockHeader.getStateRoot()
              + " but BAL computed "
              + result.root());
    }
    return StateRootComputation.pathBased(result.root(), result.writes());
  }

  private BackgroundResult runComputation(
      final BonsaiWorldState worldState, final BlockAccessList bal) {
    final List<BlockAccessListChanges.AccountFinalChanges> changes =
        BlockAccessListChanges.latestChanges(bal);
    if (changes.isEmpty()) {
      return new BackgroundResult(worldState.getWorldStateRootHash(), List.of(), Map.of());
    }
    final BonsaiBalWorldStateUpdater balUpdater = (BonsaiBalWorldStateUpdater) worldState.updater();
    return new BalComputation(worldState, changes, balUpdater).execute();
  }

  private BackgroundResult awaitBackgroundComputation(
      final CompletableFuture<BackgroundResult> future) {
    try {
      return future.join();
    } catch (final CancellationException e) {
      throw new IllegalStateException("Background BAL state root computation was cancelled", e);
    } catch (final CompletionException e) {
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new IllegalStateException("Background BAL state root computation failed", cause);
    }
  }

  private BonsaiWorldState openParentWorldState(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final BlockAccessListIndex balIndex) {
    final Hash parentHash = blockHeader.getParentHash();
    final BlockHeader parentHeader =
        protocolContext
            .getBlockchain()
            .getBlockHeader(parentHash)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Parent %s of block %s not found",
                            parentHash, blockHeader.getBlockHash())));
    final WorldStateQueryParams queryParams =
        WorldStateQueryParams.newBuilder()
            .withBlockHeader(parentHeader)
            .withShouldWorldStateUpdateHead(false)
            .withUpdaterFactory(
                (BonsaiWorldState ws) ->
                    new BonsaiBalWorldStateUpdater(balIndex, Long.MAX_VALUE, ws, EvmConfiguration.DEFAULT))
            .build();
    return (BonsaiWorldState)
        protocolContext.getWorldStateArchive().getWorldState(queryParams).orElseThrow();
  }

  private record BackgroundResult(
      Hash root, List<StateRootComputation.UpdaterWrite> writes, Map<Address, Hash> storageRoots) {}

  /**
   * Applies BAL changes to the parent world state's tries. Storage trie futures are launched
   * eagerly before the account loop so storage I/O overlaps with account-trie writes. Account
   * field values (nonce, balance, code hash) are read lazily through a {@link BonsaiBalWorldStateUpdater}
   * with {@code txIndex = Long.MAX_VALUE}, which serves them O(1) from the BAL index when present
   * and falls back to the parent DB only for fields not tracked in the BAL.
   */
  private static final class BalComputation {

    private final BonsaiWorldState worldState;
    private final List<BlockAccessListChanges.AccountFinalChanges> changes;

    /**
     * BAL-backed updater used for lazy field reads. {@code txIndex = Long.MAX_VALUE} makes all
     * BAL entries visible (post-all-transactions final state).
     */
    private final BonsaiBalWorldStateUpdater balUpdater;

    /** Thread-safe; populated concurrently by storage futures and account writes. */
    private final List<StateRootComputation.UpdaterWrite> writes =
        Collections.synchronizedList(new ArrayList<>());

    /** Populated during account resolution once storage futures complete. */
    private final Map<Address, Hash> storageRoots = new ConcurrentHashMap<>();

    /**
     * Futures for storage-trie updates, keyed by address. Launched eagerly so storage I/O
     * overlaps with the sequential account resolution loop.
     */
    private final Map<Address, CompletableFuture<Hash>> storageFutures = new ConcurrentHashMap<>();

    BalComputation(
        final BonsaiWorldState worldState,
        final List<BlockAccessListChanges.AccountFinalChanges> changes,
        final BonsaiBalWorldStateUpdater balUpdater) {
      this.worldState = worldState;
      this.changes = changes;
      this.balUpdater = balUpdater;
    }

    BackgroundResult execute() {
      final MerkleTrie<Bytes, Bytes> accountTrie = worldState.createAccountStateTrie();

      // Step 1: for every account with storage changes, launch a storage future eagerly so
      // storage I/O overlaps with step 2.
      changes.parallelStream()
          .filter(c -> !c.storageChanges().isEmpty())
          .forEach(
              c -> {
                final Hash accountHash = c.address().addressHash();
                storageFutures.put(
                    c.address(),
                    CompletableFuture.supplyAsync(() -> updateStorageTrie(accountHash, c)));
              });

      // Step 2: for each changed account, read final state via BalWorldUpdater and update the trie.
      for (final BlockAccessListChanges.AccountFinalChanges change : changes) {
        final Hash accountHash = change.address().addressHash();
        final Optional<Bytes> newRlp = resolveAccount(accountHash, change);
        if (newRlp.isPresent()) {
          accountTrie.put(accountHash.getBytes(), newRlp.get());
        } else {
          accountTrie.remove(accountHash.getBytes());
        }
      }

      // Step 3: commit the account trie.
      accountTrie.commit(
          (location, hash, value) ->
              writes.add(u -> u.putAccountStateTrieNode(location, hash, value)));
      return new BackgroundResult(Hash.wrap(accountTrie.getRootHash()), writes, storageRoots);
    }

    /**
     * Resolves the final on-chain account state for {@code change.address()} using the {@link
     * BonsaiBalWorldStateUpdater} for lazy nonce/balance/code-hash reads, and returns the RLP encoding to
     * store in the account trie (or empty to signal account deletion).
     */
    private Optional<Bytes> resolveAccount(
        final Hash accountHash, final BlockAccessListChanges.AccountFinalChanges change) {

      // Lazy reads via BalWorldUpdater: BAL-first (O(1)), DB-fallback only when not in BAL.
      final Account account = balUpdater.get(change.address());
      final long nonce = account != null ? account.getNonce() : 0L;
      final Wei balance = account != null ? account.getBalance() : Wei.ZERO;
      final Hash codeHash = account != null ? account.getCodeHash() : Hash.EMPTY;

      // Storage root: if there are no storage changes, read the prior root node hash from the
      // raw KV store (one lookup, same approach as updateStorageTrie).
      final Hash newStorageRoot;
      if (change.storageChanges().isEmpty()) {
        newStorageRoot =
            worldState
                .getWorldStateStorage()
                .getTrieNodeUnsafe(accountHash.getBytes())
                .map(Hash::hash)
                .orElse(Hash.EMPTY_TRIE_HASH);
      } else {
        // Join the pre-launched storage future; by now it is likely already complete.
        newStorageRoot = storageFutures.get(change.address()).join();
        storageRoots.put(change.address(), newStorageRoot);
      }

      applyCodeChanges(change.address(), accountHash, change);

      final PmtStateTrieAccountValue updated =
          new PmtStateTrieAccountValue(nonce, balance, newStorageRoot, codeHash);
      if (isAccountEmpty(updated)) {
        writes.add(updater -> updater.removeAccountInfoState(accountHash));
        return Optional.empty();
      } else {
        final Bytes encoded = RLP.encode(updated::writeTo);
        writes.add(updater -> updater.putAccountInfoState(accountHash, encoded));
        return Optional.of(encoded);
      }
    }

    private Hash updateStorageTrie(
        final Hash accountHash,
        final BlockAccessListChanges.AccountFinalChanges change) {

      // Read the storage trie root node directly (one raw KV lookup, no account RLP parsing).
      final Hash priorStorageRoot =
          worldState
              .getWorldStateStorage()
              .getTrieNodeUnsafe(accountHash.getBytes())
              .map(Hash::hash)
              .orElse(Hash.EMPTY_TRIE_HASH);

      final MerkleTrie<Bytes, Bytes> storageTrie =
          worldState.createTrie(
              (location, key) -> worldState.getStorageTrieNode(accountHash, location, key),
              Bytes32.wrap(priorStorageRoot.getBytes()));

      for (final BlockAccessListChanges.StorageFinalChange sc : change.storageChanges()) {
        final Hash slotHash = sc.slot().getSlotHash();
        final UInt256 value = sc.value();
        if (value.equals(UInt256.ZERO)) {
          writes.add(updater -> updater.removeStorageValueBySlotHash(accountHash, slotHash));
          storageTrie.remove(slotHash.getBytes());
        } else {
          writes.add(updater -> updater.putStorageValueBySlotHash(accountHash, slotHash, value));
          storageTrie.put(slotHash.getBytes(), encodeTrieValue(value));
        }
      }

      storageTrie.commit(
          (location, nodeHash, value) ->
              writes.add(u -> u.putAccountStorageTrieNode(accountHash, location, nodeHash, value)));
      return Hash.wrap(storageTrie.getRootHash());
    }

    /**
     * Emits KV writes for code changes. For code deletion, the prior account is loaded from the
     * parent world state to find the code hash to remove; this DB read is unavoidable since the
     * BAL does not store the prior code hash.
     */
    private void applyCodeChanges(
        final Address address,
        final Hash accountHash,
        final BlockAccessListChanges.AccountFinalChanges change) {
      change
          .code()
          .ifPresent(
              code -> {
                if (code.isEmpty()) {
                  // Code was cleared: load the parent account to find the prior code hash.
                  final Account parent = worldState.get(address);
                  if (parent != null && !Hash.EMPTY.equals(parent.getCodeHash())) {
                    final Hash priorCodeHash = parent.getCodeHash();
                    writes.add(updater -> updater.removeCode(accountHash, priorCodeHash));
                  }
                } else {
                  writes.add(updater -> updater.putCode(accountHash, Hash.hash(code), code));
                }
              });
    }

    private boolean isAccountEmpty(final PmtStateTrieAccountValue account) {
      return account.getNonce() == 0
          && account.getBalance().isZero()
          && Hash.EMPTY_TRIE_HASH.equals(account.getStorageRoot())
          && Hash.EMPTY.equals(account.getCodeHash());
    }
  }
}
