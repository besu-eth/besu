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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2;

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType.ACCOUNT_RANGE;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.StackTrie.FlatDatabaseUpdater.noop;
import static org.hyperledger.besu.ethereum.trie.RangeManager.MAX_RANGE;
import static org.hyperledger.besu.ethereum.trie.RangeManager.MIN_RANGE;
import static org.hyperledger.besu.ethereum.trie.RangeManager.findNewBeginElementInRange;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.StackTrie;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.v2.SnapV2DataRequest;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Snap/2 account range data request. Commits all trie nodes including incomplete ones. */
public class SnapV2AccountRangeRequest extends SnapV2DataRequest {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2AccountRangeRequest.class);

  private final Bytes32 startKeyHash;
  private final Bytes32 endKeyHash;
  private final StackTrie stackTrie;
  private ResponseProofStatus responseProofStatus;

  public SnapV2AccountRangeRequest(
      final BlockHeader pivotBlockHeader, final Bytes32 startKeyHash, final Bytes32 endKeyHash) {
    super(ACCOUNT_RANGE, pivotBlockHeader, startKeyHash);
    this.startKeyHash = startKeyHash;
    this.endKeyHash = endKeyHash;
    this.responseProofStatus = ResponseProofStatus.PENDING;
    this.stackTrie = new StackTrie(pivotBlockHeader.getStateRoot(), startKeyHash);
  }

  @Override
  protected int doPersist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapRequestContext downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {

    final AtomicInteger nbNodesSaved = new AtomicInteger();
    final NodeUpdater nodeUpdater =
        (location, hash, value) -> {
          if (location.isEmpty()) {
            downloadState.setRootNodeData(value);
          }
          applyForStrategy(
              updater,
              onBonsai -> onBonsai.putAccountStateTrieNode(location, hash, value),
              onForest -> onForest.putAccountStateTrieNode(hash, value));
          nbNodesSaved.getAndIncrement();
        };

    final AtomicReference<StackTrie.FlatDatabaseUpdater> flatDatabaseUpdater =
        new AtomicReference<>(noop());

    worldStateStorageCoordinator.applyOnMatchingFlatModes(
        List.of(FlatDbMode.FULL, FlatDbMode.ARCHIVE),
        bonsaiWorldStateStorageStrategy -> {
          flatDatabaseUpdater.set(
              (key, value) ->
                  ((BonsaiWorldStateKeyValueStorage.Updater) updater)
                      .putAccountInfoState(Hash.wrap(key), value));
        });

    stackTrie.commit(flatDatabaseUpdater.get(), nodeUpdater, true);
    downloadState.getMetricsManager().notifyAccountsDownloaded(stackTrie.getElementsCount().get());
    return nbNodesSaved.get();
  }

  public void addResponse(
      final WorldStateProofProvider worldStateProofProvider,
      final NavigableMap<Bytes32, Bytes> accounts,
      final List<Bytes> proofs) {
    if (!accounts.isEmpty() || !proofs.isEmpty()) {
      if (!worldStateProofProvider.isValidRangeProof(
          startKeyHash, endKeyHash, Bytes32.wrap(getRootHash().getBytes()), proofs, accounts)) {
        responseProofStatus = ResponseProofStatus.INVALID;
      } else {
        stackTrie.addElement(startKeyHash, proofs, accounts);
        responseProofStatus = ResponseProofStatus.VALID;
        LOG.atDebug()
            .setMessage("{} accounts received during sync for account range {} {}")
            .addArgument(accounts.size())
            .addArgument(startKeyHash)
            .addArgument(endKeyHash)
            .log();
      }
    }
  }

  @Override
  public boolean isResponseReceived() {
    return responseProofStatus == ResponseProofStatus.VALID;
  }

  public boolean hasInvalidProof() {
    return responseProofStatus == ResponseProofStatus.INVALID;
  }

  @Override
  public Stream<SnapDataRequest> getChildRequests(
      final SnapRequestContext downloadState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState snapSyncState) {
    if (responseProofStatus != ResponseProofStatus.VALID) {
      return Stream.empty();
    }

    final List<SnapDataRequest> childRequests = new ArrayList<>();
    final StackTrie.TaskElement taskElement = stackTrie.getElement(startKeyHash);

    findNewBeginElementInRange(
            Bytes32.wrap(getRootHash().getBytes()),
            taskElement.proofs(),
            taskElement.keys(),
            endKeyHash)
        .ifPresentOrElse(
            missingRightElement -> {
              downloadState
                  .getMetricsManager()
                  .notifyRangeProgress(
                      SnapSyncMetricsManager.Step.DOWNLOAD, missingRightElement, endKeyHash);
              childRequests.add(
                  new SnapV2AccountRangeRequest(
                      getPivotBlockHeader(), missingRightElement, endKeyHash));
            },
            () ->
                downloadState
                    .getMetricsManager()
                    .notifyRangeProgress(
                        SnapSyncMetricsManager.Step.DOWNLOAD, endKeyHash, endKeyHash));

    for (Map.Entry<Bytes32, Bytes> account : taskElement.keys().entrySet()) {
      final PmtStateTrieAccountValue accountValue =
          PmtStateTrieAccountValue.readFrom(RLP.input(account.getValue()));
      if (!accountValue.getStorageRoot().equals(Hash.EMPTY_TRIE_HASH)) {
        childRequests.add(
            new SnapV2StorageRangeRequest(
                getPivotBlockHeader(),
                account.getKey(),
                Bytes32.wrap(accountValue.getStorageRoot().getBytes()),
                MIN_RANGE,
                MAX_RANGE,
                startKeyHash));
      }
      if (!accountValue.getCodeHash().equals(Hash.EMPTY)) {
        childRequests.add(
            new SnapV2BytecodeRequest(
                getPivotBlockHeader(),
                account.getKey(),
                Bytes32.wrap(accountValue.getCodeHash().getBytes()),
                startKeyHash));
      }
    }
    return childRequests.stream();
  }

  public Bytes32 getStartKeyHash() {
    return startKeyHash;
  }

  public Bytes32 getEndKeyHash() {
    return endKeyHash;
  }

  public NavigableMap<Bytes32, Bytes> getAccounts() {
    return stackTrie.getElement(startKeyHash).keys();
  }

  public SnapV2AccountRangeRequest retarget(final BlockHeader newPivotBlockHeader) {
    return new SnapV2AccountRangeRequest(newPivotBlockHeader, startKeyHash, endKeyHash);
  }

  @Override
  public void clear() {
    stackTrie.clear();
    responseProofStatus = ResponseProofStatus.PENDING;
  }
}
