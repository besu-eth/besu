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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter.patricia;

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldView.encodeTrieValue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalStateRootCommitter;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootComputations;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.WriteSink;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/** Replays the BAL onto the Patricia account trie and the per-account storage tries. */
public final class PatriciaBalEngine implements BalStateRootCommitter.Engine {

  public static final PatriciaBalEngine INSTANCE = new PatriciaBalEngine();

  private PatriciaBalEngine() {}

  @Override
  public TrieBranchType getTrieBranchType() {
    return TrieBranchType.PATRICIA;
  }

  @Override
  public BalStateRootCommitter.Result compute(
      final BonsaiWorldState parent,
      final BlockAccessListAccountLookup accountLookup,
      final boolean storageFrozen) {
    return new Computation(parent, accountLookup, storageFrozen).execute();
  }

  private static final class Computation {

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

    Computation(
        final BonsaiWorldState worldState,
        final BlockAccessListAccountLookup accountLookup,
        final boolean storageFrozen) {
      this.worldState = worldState;
      this.accountLookup = accountLookup;
      this.sink = WriteSink.of(storageFrozen, writes);
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
    BalStateRootCommitter.Result execute() {
      final MerkleTrie<Bytes, Bytes> accountTrie =
          PatriciaTrieFactory.createAccountStateTrie(worldState);

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
          accountTrie, (location, hash, value) -> u -> u.putTrieNode(location, hash, value));
      return new BalStateRootCommitter.Result(
          StateRootComputations.pathBased(
              Hash.wrap(accountTrie.getRootHash()), new ArrayList<>(writes)),
          storageRoots);
    }

    private Optional<Bytes> resolveAccount(
        final Hash accountHash,
        final Address address,
        final BlockAccessList.AccountChanges changes,
        final Optional<Bytes> maybeRlp) {

      final PatriciaTrieAccountValue priorAccount =
          maybeRlp.map(rlp -> PatriciaTrieAccountValue.readFrom(RLP.input(rlp))).orElse(null);

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

      final PatriciaTrieAccountValue updatedAccount =
          new PatriciaTrieAccountValue(newNonce, newBalance, newStorageRoot, newCodeHash);
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
          PatriciaTrieFactory.createStorageTrie(worldState, accountHash, priorStorageRoot);

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
              u -> u.putTrieNode(TrieNodeKey.of(accountHash, location), nodeHash, value));
      return Hash.wrap(storageTrie.getRootHash());
    }

    private Hash priorStorageRoot(final Address address) {
      return worldState
          .getWorldStateStorage()
          .getAccount(address.addressHash())
          .map(rlp -> PatriciaTrieAccountValue.readFrom(RLP.input(rlp)).getStorageRoot())
          .orElse(Hash.EMPTY_TRIE_HASH);
    }

    private boolean isAccountEmpty(final PatriciaTrieAccountValue account) {
      return account.getNonce() == 0
          && account.getBalance().isZero()
          && Hash.EMPTY_TRIE_HASH.equals(account.getStorageRoot())
          && Hash.EMPTY.equals(account.getCodeHash());
    }
  }
}
