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

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldView.encodeTrieValue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.PatriciaAccountValue;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public final class BalStateRootCommitter implements StateRootCommitter {

  private final ProtocolContext protocolContext;
  private final BlockHeader blockHeader;
  private final BlockAccessListAccountLookup accountLookup;
  private final boolean storageFrozen;

  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  // Assigned by start(); null until then. start() must be called before compute()/cancel().
  private CompletableFuture<BackgroundResult> backgroundComputation;

  public BalStateRootCommitter(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final BlockAccessListAccountLookup accountLookup,
      final boolean storageFrozen) {
    this.protocolContext = protocolContext;
    this.blockHeader = blockHeader;
    this.accountLookup = accountLookup;
    this.storageFrozen = storageFrozen;
  }

  /**
   * Launches the background BAL state root computation and returns this committer.
   *
   * <p>Separated from the constructor so the background thread is not started during object
   * construction — the constructor only captures its arguments, and the asynchronous work (which
   * calls back into this instance) only begins once construction is complete and {@code start()} is
   * invoked. Must be called exactly once before {@link #compute} or {@link #cancel}.
   */
  public BalStateRootCommitter start() {
    this.backgroundComputation =
        CompletableFuture.supplyAsync(
            () -> {
              try (BonsaiWorldState parent =
                  openParentWorldState(protocolContext, blockHeader, accountLookup)) {
                return runComputation(parent, accountLookup, storageFrozen);
              }
            },
            BlockProcessingExecutors.stateRootExecutor());
    return this;
  }

  /** Cancels the background computation; {@link #compute} will throw if called afterwards. */
  @Override
  public void cancel() {
    cancelled.set(true);
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
      final MutableWorldState worldState,
      final BlockHeader blockHeader,
      final WorldUpdater worldUpdater) {
    final BackgroundResult result = awaitBackgroundComputation(backgroundComputation);
    final BonsaiWorldStateUpdateAccumulator accumulator =
        (BonsaiWorldStateUpdateAccumulator)
            Objects.requireNonNull(
                worldUpdater, "BAL state root committer requires a non-null WorldUpdater");
    result
        .storageRoots()
        .forEach(
            (address, newStorageRoot) -> {
              final var entry = accumulator.getAccountsToUpdate().get(address);
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
    return StateRootComputations.pathBased(result.root(), result.writes());
  }

  private BackgroundResult runComputation(
      final BonsaiWorldState worldState,
      final BlockAccessListAccountLookup accountLookup,
      final boolean storageFrozen) {
    if (accountLookup.isEmpty()) {
      return new BackgroundResult(worldState.getWorldStateRootHash(), List.of(), Map.of());
    }
    return new BalComputation(worldState, accountLookup, storageFrozen).execute();
  }

  private BackgroundResult awaitBackgroundComputation(
      final CompletableFuture<BackgroundResult> future) {
    try {
      final BackgroundResult result = future.get();
      if (cancelled.get()) {
        throw new IllegalStateException("Background BAL state root computation was cancelled");
      }
      return result;
    } catch (final CancellationException e) {
      throw new IllegalStateException("Background BAL state root computation was cancelled", e);
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new IllegalStateException("Background BAL state root computation failed", cause);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while waiting for background BAL state root computation", e);
    }
  }

  private BonsaiWorldState openParentWorldState(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final BlockAccessListAccountLookup accountLookup) {
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
            // The BAL overlay is attached for consistency with other BAL world-state callers, but
            // this computation path does not read through the world-state accumulator: it replays
            // the BAL directly onto tries/storage (see BalComputation), so the overlay is not
            // consumed here. Kept so the parent is opened the same way as for EVM execution.
            .withBalOverlay(new BlockAccessListOverlay(accountLookup, Long.MAX_VALUE))
            .build();
    final BonsaiWorldState worldState =
        (BonsaiWorldState)
            protocolContext.getWorldStateArchive().getWorldState(queryParams).orElseThrow();
    worldState.disableCacheMerkleTrieLoader();
    return worldState;
  }

  /**
   * Result of the background trie computation.
   *
   * @param root computed state root hash
   * @param writes deferred KV writes to apply at persist time (empty when storage is frozen)
   * @param storageRoots new per-account storage roots, patched into the EVM accumulator by {@link
   *     #compute}
   */
  private record BackgroundResult(
      Hash root,
      List<StateRootComputations.UpdaterWrite> writes,
      Map<Address, Hash> storageRoots) {}

  private static final class BalComputation {

    private final BonsaiWorldState worldState;
    private final BlockAccessListAccountLookup accountLookup;

    /** Strategy that persists deferred writes, or drops them when storage is frozen. */
    private final WriteSink sink;

    /** Lock-free queue; storage futures and account resolution may append concurrently. */
    private final ConcurrentLinkedQueue<StateRootComputations.UpdaterWrite> writes =
        new ConcurrentLinkedQueue<>();

    /** Populated during account resolution once storage futures complete. */
    private final Map<Address, Hash> storageRoots = new ConcurrentHashMap<>();

    /**
     * Futures for storage-trie updates, keyed by address. Launched eagerly so storage I/O overlaps
     * with the sequential account resolution loop.
     */
    private final Map<Address, CompletableFuture<Hash>> storageFutures = new ConcurrentHashMap<>();

    BalComputation(
        final BonsaiWorldState worldState,
        final BlockAccessListAccountLookup accountLookup,
        final boolean storageFrozen) {
      this.worldState = worldState;
      this.accountLookup = accountLookup;
      this.sink = storageFrozen ? new FrozenSink() : new PersistingSink(writes);
    }

    /**
     * Runs the three-phase BAL commit:
     *
     * <ol>
     *   <li>Launch storage-trie updates concurrently for accounts with storage changes.
     *   <li>Resolve each changed account in the account trie via {@code putDeferred}.
     *   <li>Commit the account trie; deferred writes are collected unless storage is frozen.
     * </ol>
     */
    BackgroundResult execute() {
      final MerkleTrie<Bytes, Bytes> accountTrie = worldState.createAccountStateTrie();

      // Step 1: for every account with storage changes, launch a storage future eagerly so
      // storage I/O overlaps with step 2.
      for (final BlockAccessList.AccountChanges changes : accountLookup.accountChanges()) {
        if (!changes.storageChanges().isEmpty()) {
          final Address address = changes.address();
          final Hash accountHash = address.addressHash();
          storageFutures.put(
              address,
              CompletableFuture.supplyAsync(
                  () -> updateStorageTrie(address, accountHash, changes),
                  BlockProcessingExecutors.storageTrieExecutor()));
        }
      }

      // Step 2: for each changed account, stage a deferred update — the trie passes the existing
      // leaf RLP.
      for (final BlockAccessList.AccountChanges changes : accountLookup.accountChanges()) {
        if (changes.hasAnyChange()) {
          final Address address = changes.address();
          final Hash accountHash = address.addressHash();
          accountTrie.putDeferred(
              accountHash.getBytes(),
              existingRlp -> resolveAccount(accountHash, address, changes, existingRlp));
        }
      }

      // Step 3: commit the account trie.
      sink.commitTrie(
          accountTrie,
          (location, hash, value) -> u -> u.putTrieNode(Optional.empty(), location, hash, value));
      return new BackgroundResult(
          Hash.wrap(accountTrie.getRootHash()), new ArrayList<>(writes), storageRoots);
    }

    private Optional<Bytes> resolveAccount(
        final Hash accountHash,
        final Address address,
        final BlockAccessList.AccountChanges changes,
        final Optional<Bytes> maybeRlp) {

      final PatriciaAccountValue priorAccount =
          maybeRlp.map(rlp -> PatriciaAccountValue.readFrom(RLP.input(rlp))).orElse(null);

      final long newNonce;
      if (changes.nonceChanges().isEmpty()) {
        newNonce = priorAccount != null ? priorAccount.getNonce() : 0L;
      } else {
        newNonce = changes.nonceChanges().getLast().newNonce();
      }

      final Wei newBalance;
      if (changes.balanceChanges().isEmpty()) {
        newBalance = priorAccount != null ? priorAccount.getBalance() : Wei.ZERO;
      } else {
        newBalance = changes.balanceChanges().getLast().postBalance();
      }

      final Hash newCodeHash;
      if (changes.codeChanges().isEmpty()) {
        newCodeHash = priorAccount != null ? priorAccount.getCodeHash() : Hash.EMPTY;
      } else {
        final BlockAccessList.CodeChange codeChange = changes.codeChanges().getLast();
        newCodeHash = Hash.hash(codeChange.newCode());
        if (!sink.isFrozen()) {
          if (codeChange.newCode().isEmpty()) {
            // Code was cleared: load the parent account to find the prior code hash.
            if (priorAccount != null && !Hash.EMPTY.equals(priorAccount.getCodeHash())) {
              final Hash priorCodeHash = priorAccount.getCodeHash();
              sink.removeCode(accountHash, priorCodeHash);
            }
          } else {
            sink.putCode(accountHash, newCodeHash, codeChange.newCode());
          }
        }
      }

      // Storage root: if there are no storage changes, parse the prior root from the existing
      // account RLP passed in by putDeferred (no separate KV lookup needed).
      final Hash newStorageRoot;
      if (changes.storageChanges().isEmpty()) {
        newStorageRoot =
            priorAccount != null ? priorAccount.getStorageRoot() : Hash.EMPTY_TRIE_HASH;
      } else {
        // Join the pre-launched storage future; by now it is likely already complete.
        newStorageRoot = storageFutures.get(address).join();
      }
      storageRoots.put(address, newStorageRoot);

      final PatriciaAccountValue updatedAccount =
          new PatriciaAccountValue(newNonce, newBalance, newStorageRoot, newCodeHash);
      if (isAccountEmpty(updatedAccount)) {
        sink.removeAccountInfoState(accountHash);
        return Optional.empty();
      } else {
        final Bytes encoded = RLP.encode(updatedAccount::writeTo);
        sink.putAccountInfoState(accountHash, encoded);
        return Optional.of(encoded);
      }
    }

    /**
     * Replays storage slot changes from the BAL on the parent storage trie and returns the new
     * storage root. When storage is frozen, the trie is updated in memory only and slot/trie-node
     * KV writes are not recorded.
     */
    private Hash updateStorageTrie(
        final Address address,
        final Hash accountHash,
        final BlockAccessList.AccountChanges accountChanges) {

      final Hash priorStorageRoot = priorStorageRoot(address);

      final MerkleTrie<Bytes, Bytes> storageTrie =
          worldState.createStorageTrie(accountHash, priorStorageRoot);

      for (final BlockAccessList.SlotChanges slotChanges : accountChanges.storageChanges()) {
        final Hash slotHash = slotChanges.slot().getSlotHash();
        final UInt256 rawValue = slotChanges.changes().getLast().newValue();
        final UInt256 value = rawValue == null ? UInt256.ZERO : rawValue;
        if (value.equals(UInt256.ZERO)) {
          sink.removeStorageValueBySlotHash(accountHash, slotHash);
          storageTrie.remove(slotHash.getBytes());
        } else {
          sink.putStorageValueBySlotHash(accountHash, slotHash, value);
          storageTrie.put(slotHash.getBytes(), encodeTrieValue(value));
        }
      }

      sink.commitTrie(
          storageTrie,
          (location, nodeHash, value) ->
              u -> u.putTrieNode(Optional.of(accountHash), location, nodeHash, value));
      return Hash.wrap(storageTrie.getRootHash());
    }

    private Hash priorStorageRoot(final Address address) {
      return worldState
          .getWorldStateStorage()
          .getAccount(address.addressHash())
          .map(rlp -> PatriciaAccountValue.readFrom(RLP.input(rlp)).getStorageRoot())
          .orElse(Hash.EMPTY_TRIE_HASH);
    }

    private boolean isAccountEmpty(final PatriciaAccountValue account) {
      return account.getNonce() == 0
          && account.getBalance().isZero()
          && Hash.EMPTY_TRIE_HASH.equals(account.getStorageRoot())
          && Hash.EMPTY.equals(account.getCodeHash());
    }
  }
}
