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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class BlockAccessListDataRequest extends SnapDataRequest {

  private final BlockHeader blockHeader;
  private Optional<BlockAccessList> blockAccessList = Optional.empty();

  public BlockAccessListDataRequest(final Hash rootHash, final BlockHeader blockHeader) {
    super(RequestType.BLOCK_ACCESS_LIST, rootHash);
    this.blockHeader = blockHeader;
  }

  public BlockHeader getBlockHeader() {
    return blockHeader;
  }

  public void setResponse(final BlockAccessList response) {
    blockAccessList = Optional.of(response);
  }

  public Optional<BlockAccessList> getBlockAccessList() {
    return blockAccessList;
  }

  @Override
  protected int doPersist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapWorldDownloadState downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {
    if (!(updater instanceof BonsaiWorldStateKeyValueStorage.Updater bonsaiUpdater)) {
      return 0;
    }

    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        worldStateStorageCoordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class);

    blockAccessList.ifPresent(
        bal -> {
          for (final var accountChanges : BlockAccessListChanges.latestChanges(bal)) {
            final Hash accountHash = Hash.hash(accountChanges.address().getBytes());

            final PmtStateTrieAccountValue currentValue =
                worldStateStorage
                    .getAccount(accountHash)
                    .map(raw -> PmtStateTrieAccountValue.readFrom(RLP.input(raw)))
                    .orElse(
                        new PmtStateTrieAccountValue(
                            0, Wei.ZERO, Hash.EMPTY_TRIE_HASH, Hash.EMPTY));

            final var updatedCode = accountChanges.code();
            final Hash updatedCodeHash =
                updatedCode.map(Hash::hash).orElse(currentValue.getCodeHash());
            updatedCode.ifPresent(
                code -> bonsaiUpdater.putCode(accountHash, updatedCodeHash, code));

            final Hash updatedStorageRoot =
                accountChanges.storageChanges().isEmpty()
                    ? currentValue.getStorageRoot()
                    : applyStorageChangesAndComputeUpdatedStorageRoot(
                        accountHash,
                        currentValue.getStorageRoot(),
                        accountChanges,
                        worldStateStorageCoordinator,
                        bonsaiUpdater);

            final PmtStateTrieAccountValue updatedValue =
                new PmtStateTrieAccountValue(
                    accountChanges.nonce().orElse(currentValue.getNonce()),
                    accountChanges.balance().orElse(currentValue.getBalance()),
                    updatedStorageRoot,
                    updatedCodeHash);
            bonsaiUpdater.putAccountInfoState(accountHash, RLP.encode(updatedValue::writeTo));
          }
        });

    return 0;
  }

  private Hash applyStorageChangesAndComputeUpdatedStorageRoot(
      final Hash accountHash,
      final Hash currentStorageRoot,
      final BlockAccessListChanges.AccountFinalChanges accountChanges,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final BonsaiWorldStateKeyValueStorage.Updater bonsaiUpdater) {
    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) ->
                worldStateStorageCoordinator.getAccountStorageTrieNode(accountHash, location, hash),
            Bytes32.wrap(currentStorageRoot.getBytes()),
            value -> value,
            value -> value);

    for (final var storageChange : accountChanges.storageChanges()) {
      final Hash slotHash = storageChange.slot().getSlotHash();
      final UInt256 value = storageChange.value();
      final Bytes slotKey = slotHash.getBytes();

      if (value.equals(UInt256.ZERO)) {
        bonsaiUpdater.removeStorageValueBySlotHash(accountHash, slotHash);
        storageTrie.remove(slotKey);
      } else {
        bonsaiUpdater.putStorageValueBySlotHash(accountHash, slotHash, value.toBytes());
        storageTrie.put(slotKey, PathBasedWorldView.encodeTrieValue(value.toBytes()));
      }
    }

    storageTrie.commit(
        (location, nodeHash, value) ->
            bonsaiUpdater.putAccountStorageTrieNode(accountHash, location, nodeHash, value));

    return Hash.wrap(storageTrie.getRootHash());
  }

  @Override
  public boolean isResponseReceived() {
    return blockAccessList.isPresent();
  }

  @Override
  public Stream<SnapDataRequest> getChildRequests(
      final SnapWorldDownloadState downloadState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState snapSyncState) {
    return Stream.empty();
  }
}
