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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Bonsai path-based root for the flat (trie-disabled) mode: collects the deferred account, storage
 * and code writes directly, without building or walking any Merkle trie.
 *
 * <p>The state root cannot be recomputed without a trie, so the block header's state root is
 * returned (or {@link MerkleTrie#EMPTY_TRIE_NODE_HASH} when no header is available, e.g. a frozen
 * root recompute).
 */
public enum TrieDisabledStateRootCommitter implements StateRootCommitter {
  INSTANCE;

  @Override
  public StateRootComputation compute(
      final MutableWorldState mutableWorldState,
      final BlockHeader blockHeader,
      final WorldUpdater worldUpdater) {
    final BonsaiWorldStateUpdateAccumulator accumulator =
        (BonsaiWorldStateUpdateAccumulator)
            Objects.requireNonNull(
                worldUpdater, "Bonsai state root committers require a non-null WorldUpdater");
    final BonsaiWorldState bonsai = (BonsaiWorldState) mutableWorldState;
    final List<StateRootComputations.UpdaterWrite> writes = new ArrayList<>();
    new FlatComputation(bonsai, accumulator).executeInto(writes);
    final Hash root =
        blockHeader != null
            ? blockHeader.getStateRoot()
            : Hash.wrap(MerkleTrie.EMPTY_TRIE_NODE_HASH);
    return StateRootComputations.pathBased(root, writes);
  }

  private static final class FlatComputation {
    private final BonsaiWorldStateUpdateAccumulator worldStateUpdater;

    /** Lock-free queue; kept for symmetry with the default committer's sink wiring. */
    private final ConcurrentLinkedQueue<StateRootComputations.UpdaterWrite> writes =
        new ConcurrentLinkedQueue<>();

    /** Strategy that persists deferred writes, or drops them when storage is frozen. */
    private final WriteSink sink;

    FlatComputation(
        final BonsaiWorldState bonsai, final BonsaiWorldStateUpdateAccumulator worldStateUpdater) {
      this.worldStateUpdater = worldStateUpdater;
      this.sink = bonsai.isStorageFrozen() ? new FrozenSink() : new PersistingSink(writes);
    }

    void executeInto(final List<StateRootComputations.UpdaterWrite> writeSink) {
      if (!sink.isFrozen()) {
        collectCodeWrites();
      }
      collectStorageWrites();
      collectAccountWrites();
      writeSink.addAll(writes);
    }

    private void collectAccountWrites() {
      for (final Map.Entry<Address, BonsaiValue<BonsaiAccount>> accountUpdate :
          worldStateUpdater.getAccountsToUpdate().entrySet()) {
        final BonsaiValue<BonsaiAccount> accountValue = accountUpdate.getValue();
        final Hash addressHash = accountUpdate.getKey().addressHash();
        if (accountValue.getUpdated() == null) {
          sink.removeAccountInfoState(addressHash);
        } else {
          sink.putAccountInfoState(addressHash, accountValue.getUpdated().serializeAccount());
        }
      }
    }

    private void collectStorageWrites() {
      for (final Map.Entry<Address, StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>>>
          storageAccountUpdate : worldStateUpdater.getStorageToUpdate().entrySet()) {
        final Address address = storageAccountUpdate.getKey();
        if (!worldStateUpdater.getAccountsToUpdate().containsKey(address)) {
          continue;
        }
        final boolean accountDeleted =
            worldStateUpdater.getAccountsToUpdate().get(address).getUpdated() == null;
        if (accountDeleted && sink.isFrozen()) {
          continue;
        }
        final Hash updatedAddressHash = address.addressHash();
        for (final Map.Entry<StorageSlotKey, BonsaiValue<UInt256>> storageUpdate :
            storageAccountUpdate.getValue().entrySet()) {
          if (storageUpdate.getValue().isUnchanged()) {
            continue;
          }
          final Hash slotHash = storageUpdate.getKey().getSlotHash();
          final UInt256 updatedStorage = storageUpdate.getValue().getUpdated();
          if (updatedStorage == null || updatedStorage.equals(UInt256.ZERO)) {
            sink.removeStorageValueBySlotHash(updatedAddressHash, slotHash);
          } else {
            sink.putStorageValueBySlotHash(updatedAddressHash, slotHash, updatedStorage);
          }
        }
      }
    }

    private void collectCodeWrites() {
      for (final Map.Entry<Address, BonsaiValue<Bytes>> codeUpdate :
          worldStateUpdater.getCodeToUpdate().entrySet()) {
        final Bytes updatedCode = codeUpdate.getValue().getUpdated();
        final Hash accountHash = codeUpdate.getKey().addressHash();
        final Bytes priorCode = codeUpdate.getValue().getPrior();

        if (Objects.equals(priorCode, updatedCode)
            || (codeIsEmpty(priorCode) && codeIsEmpty(updatedCode))) {
          continue;
        }

        if (codeIsEmpty(updatedCode)) {
          final Hash priorCodeHash = Hash.hash(priorCode);
          sink.removeCode(accountHash, priorCodeHash);
        } else {
          final Hash codeHash = Hash.hash(updatedCode);
          sink.putCode(accountHash, codeHash, updatedCode);
        }
      }
    }

    private static boolean codeIsEmpty(final Bytes value) {
      return value == null || value.isEmpty();
    }
  }
}
