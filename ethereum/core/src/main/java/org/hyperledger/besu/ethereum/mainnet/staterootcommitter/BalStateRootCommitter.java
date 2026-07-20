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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAddressView;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;


public final class BalStateRootCommitter implements StateRootCommitter {

  private final CompletableFuture<BackgroundResult> backgroundComputation;


  public BalStateRootCommitter(
          final ProtocolContext protocolContext,
          final BlockHeader blockHeader,
          final BlockAccessListAddressView blockAccessListAddressView,
          final boolean storageFrozen) {
    this.backgroundComputation =
            CompletableFuture.supplyAsync(
                    () -> {
                      try (BonsaiWorldState parent =
                                   openParentWorldState(protocolContext, blockHeader, blockAccessListAddressView)) {
                        return runComputation(parent, blockAccessListAddressView, storageFrozen);
                      }
                    },
                    Executors.newSingleThreadScheduledExecutor());
  }

  /** Cancels the background computation; {@link #compute} will throw if called afterwards. */
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
          final MutableWorldState worldState, final BlockHeader blockHeader, final WorldUpdater worldUpdater) {
    final BackgroundResult result = awaitBackgroundComputation(backgroundComputation);
    final BonsaiWorldStateUpdateAccumulator accumulator = (BonsaiWorldStateUpdateAccumulator) worldUpdater;
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
    return StateRootComputation.pathBased(result.root(), result.writes());
  }

  private BackgroundResult runComputation(
          final BonsaiWorldState worldState, final BlockAccessListAddressView blockAccessListAddressView,
          final boolean storageFrozen) {
    if (blockAccessListAddressView.getAccountEntries().isEmpty()) {
      return new BackgroundResult(worldState.getWorldStateRootHash(), List.of(), Map.of());
    }
    return new BalComputation(worldState, blockAccessListAddressView, storageFrozen).execute();
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
          final BlockAccessListAddressView blockAccessListAddressView) {
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
                    .withBalOverlay(blockAccessListAddressView, Long.MAX_VALUE)
                    .build();
    return (BonsaiWorldState)
            protocolContext.getWorldStateArchive().getWorldState(queryParams).orElseThrow();
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
          Hash root, List<StateRootComputation.UpdaterWrite> writes, Map<Address, Hash> storageRoots) {}

  private static final class BalComputation {

    private final BonsaiWorldState worldState;
    private final BlockAccessListAddressView blockAccessListAddressView;


    /**
     * When {@code true}, trie updates are in-memory only; no deferred KV writes are collected.
     */
    private final boolean storageFrozen;

    /** Lock-free queue; storage futures and account resolution may append concurrently. */
    private final ConcurrentLinkedQueue<StateRootComputation.UpdaterWrite> writes =
        new ConcurrentLinkedQueue<>();

    /** Populated during account resolution once storage futures complete. */
    private final Map<Address, Hash> storageRoots = new ConcurrentHashMap<>();

    /**
     * Futures for storage-trie updates, keyed by address. Launched eagerly so storage I/O
     * overlaps with the sequential account resolution loop.
     */
    private final Map<Address, CompletableFuture<Hash>> storageFutures = new ConcurrentHashMap<>();

    BalComputation(
            final BonsaiWorldState worldState,
            final BlockAccessListAddressView blockAccessListAddressView,
            final boolean storageFrozen) {
      this.worldState = worldState;
      this.blockAccessListAddressView = blockAccessListAddressView;
      this.storageFrozen = storageFrozen;
    }

    /**
     * Runs the three-phase BAL commit:
     *
     * <ol>
     *   <li>Launch storage-trie updates concurrently for accounts with storage changes.
     *   <li>Resolve each changed account in the account trie via {@code putDeferred}.
     *   <li>Unless {@link #storageFrozen}, commit the account trie and collect deferred writes.
     * </ol>
     */
    BackgroundResult execute() {
      final MerkleTrie<Bytes, Bytes> accountTrie = worldState.createAccountStateTrie();

      // Step 1: for every account with storage changes, launch a storage future eagerly so
      // storage I/O overlaps with step 2.
        for (Map.Entry<Address, BlockAccessListAddressView.AccountEntry> entry : blockAccessListAddressView.getAccountEntries().entrySet()) {
            if (entry.getValue().hasStorageChanges()) {
                final Address address = entry.getKey();
                final Hash accountHash = entry.getValue().getAddressHash();
                storageFutures.put(
                        address,
                        CompletableFuture.supplyAsync(() -> updateStorageTrie(address, accountHash, blockAccessListAddressView)));
            }
        }

      // Step 2: for each changed account, stage a deferred update — the trie passes the existing leaf RLP.
      for (Map.Entry<Address, BlockAccessListAddressView.AccountEntry> entry : blockAccessListAddressView.getAccountEntries().entrySet()) {
            final Address address = entry.getKey();
            final Hash accountHash = entry.getValue().getAddressHash();
            accountTrie.putDeferred(
                    accountHash.getBytes(),
                    existingRlp -> resolveAccount(accountHash, address, entry.getValue(), existingRlp));
        }

      if(!storageFrozen) {
        // Step 3: commit the account trie.
        accountTrie.commit(
                (location, hash, value) ->
                        writes.add(u -> u.putAccountStateTrieNode(location, hash, value)));
      }
      return new BackgroundResult(
          Hash.wrap(accountTrie.getRootHash()), new ArrayList<>(writes), storageRoots);
    }

    private Optional<Bytes> resolveAccount(
            final Hash accountHash,
            final Address address,
            final BlockAccessListAddressView.AccountEntry changes,
            final Optional<Bytes> maybeRlp) {

      final PmtStateTrieAccountValue priorAccount = maybeRlp.map(rlp -> PmtStateTrieAccountValue.readFrom(RLP.input(rlp))).orElse(null);

      final long newNonce;
      if (!changes.hasNonceChanges()) {
        newNonce =
                priorAccount != null ? priorAccount.getNonce() : 0L;
      } else {
        newNonce = changes.nonceChanges().getLast().newNonce();
      }

      final Wei newBalance;
      if (!changes.hasBalanceChanges()) {
        newBalance =
                priorAccount != null ? priorAccount.getBalance() : Wei.ZERO;
      } else {
        newBalance = changes.balanceChanges().getLast().postBalance();
      }

      final Hash newCodeHash;
      if (!changes.hasCodeChange()) {
        newCodeHash =
                priorAccount != null ? priorAccount.getCodeHash() : Hash.EMPTY;
      } else {
        final BlockAccessList.CodeChange codeChange = changes.codeChanges().getLast();
        newCodeHash = Hash.hash(codeChange.newCode());
        if (!storageFrozen && codeChange.newCode().isEmpty()) {
          // Code was cleared: load the parent account to find the prior code hash.
          if (priorAccount != null && !Hash.EMPTY.equals(priorAccount.getCodeHash())) {
            final Hash priorCodeHash = priorAccount.getCodeHash();
            writes.add(updater -> updater.removeCode(accountHash, priorCodeHash));
          }
        } else {
          writes.add(updater -> updater.putCode(accountHash, newCodeHash, codeChange.newCode()));
        }
      }



      // Storage root: if there are no storage changes, parse the prior root from the existing
      // account RLP passed in by putDeferred (no separate KV lookup needed).
      final Hash newStorageRoot;
      if (!changes.hasStorageChanges()) {
        newStorageRoot =
                priorAccount !=null ? priorAccount.getStorageRoot() : Hash.EMPTY_TRIE_HASH;
      } else {
        // Join the pre-launched storage future; by now it is likely already complete.
        newStorageRoot = storageFutures.get(address).join();
      }
      storageRoots.put(address, newStorageRoot);

      final PmtStateTrieAccountValue updatedAccount =
              new PmtStateTrieAccountValue(newNonce, newBalance, newStorageRoot, newCodeHash);
      if (isAccountEmpty(updatedAccount)) {
        if(!storageFrozen){
          writes.add(updater -> updater.removeAccountInfoState(accountHash));
        }
        return Optional.empty();
      } else {
        final Bytes encoded = RLP.encode(updatedAccount::writeTo);
        if(!storageFrozen) {
          writes.add(updater -> updater.putAccountInfoState(accountHash, encoded));
        }
        return Optional.of(encoded);
      }
    }

    /**
     * Replays storage slot changes from the BAL on the parent storage trie and returns the new
     * storage root. When {@link #storageFrozen} is {@code true}, the trie is updated in memory only
     * and slot/trie-node KV writes are not recorded.
     */
    private Hash updateStorageTrie(
            final Address address, final Hash accountHash,
            final BlockAccessListAddressView changes) {

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

      for (final Map.Entry<StorageSlotKey, BlockAccessList.SlotChanges> sc : changes.getStorageEntries(address).entrySet()) {
        final Hash slotHash = sc.getKey().getSlotHash();
        final UInt256 value = sc.getValue().changes().getLast().newValue();
        if (value.equals(UInt256.ZERO)) {
          if(!storageFrozen) {
            writes.add(updater -> updater.removeStorageValueBySlotHash(accountHash, slotHash));
          }
          storageTrie.remove(slotHash.getBytes());
        } else {
          if(!storageFrozen) {
            writes.add(updater -> updater.putStorageValueBySlotHash(accountHash, slotHash, value));
          }
          storageTrie.put(slotHash.getBytes(), encodeTrieValue(value));
        }
      }

      if(!storageFrozen) {
        storageTrie.commit(
                (location, nodeHash, value) ->
                        writes.add(u -> u.putAccountStorageTrieNode(accountHash, location, nodeHash, value)));
      }
      return Hash.wrap(storageTrie.getRootHash());
    }

    private boolean isAccountEmpty(final PmtStateTrieAccountValue account) {
      return account.getNonce() == 0
              && account.getBalance().isZero()
              && Hash.EMPTY_TRIE_HASH.equals(account.getStorageRoot())
              && Hash.EMPTY.equals(account.getCodeHash());
    }
  }
}
