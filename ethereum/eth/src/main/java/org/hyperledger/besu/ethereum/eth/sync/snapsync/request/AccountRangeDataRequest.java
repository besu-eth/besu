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

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType.ACCOUNT_RANGE;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager.Step.DOWNLOAD;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.StackTrie.FlatDatabaseUpdater.noop;
import static org.hyperledger.besu.ethereum.trie.RangeManager.MAX_RANGE;
import static org.hyperledger.besu.ethereum.trie.RangeManager.MIN_RANGE;
import static org.hyperledger.besu.ethereum.trie.RangeManager.findNewBeginElementInRange;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.StackTrie;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Returns a list of accounts and the merkle proofs of an entire range */
public class AccountRangeDataRequest extends SnapDataRequest {

  private static final Logger LOG = LoggerFactory.getLogger(AccountRangeDataRequest.class);

  protected final Bytes32 startKeyHash;
  protected final Bytes32 endKeyHash;
  protected final Optional<Bytes32> startStorageRange;
  protected final Optional<Bytes32> endStorageRange;

  protected final StackTrie stackTrie;
  private Optional<Boolean> isProofValid;

  protected AccountRangeDataRequest(
      final Hash rootHash,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash,
      final Optional<Bytes32> startStorageRange,
      final Optional<Bytes32> endStorageRange) {
    super(ACCOUNT_RANGE, rootHash);
    this.startKeyHash = startKeyHash;
    this.endKeyHash = endKeyHash;
    this.startStorageRange = startStorageRange;
    this.endStorageRange = endStorageRange;
    this.isProofValid = Optional.empty();
    this.stackTrie = new StackTrie(rootHash, startKeyHash);
    LOG.trace(
        "create get account range data request with root hash={} from {} to {}",
        rootHash,
        startKeyHash,
        endKeyHash);
  }

  protected AccountRangeDataRequest(
      final Hash rootHash, final Bytes32 startKeyHash, final Bytes32 endKeyHash) {
    this(rootHash, startKeyHash, endKeyHash, Optional.empty(), Optional.empty());
  }

  protected AccountRangeDataRequest(
      final Hash rootHash,
      final Hash accountHash,
      final Bytes32 startStorageRange,
      final Bytes32 endStorageRange) {
    this(
        rootHash,
        Bytes32.wrap(accountHash.getBytes()),
        Bytes32.wrap(accountHash.getBytes()),
        Optional.of(startStorageRange),
        Optional.of(endStorageRange));
  }

  @Override
  protected int doPersist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapWorldDownloadState downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {

    if (startStorageRange.isPresent() && endStorageRange.isPresent()) {
      // not store the new account if we just want to complete the account thanks to another
      // rootHash
      return 0;
    }

    // search incomplete nodes in the range
    final AtomicInteger nbNodesSaved = new AtomicInteger();
    final NodeUpdater nodeUpdater =
        (location, hash, value) -> {
          applyForStrategy(
              updater,
              onBonsai -> {
                onBonsai.putAccountStateTrieNode(location, hash, value);
              },
              onForest -> {
                onForest.putAccountStateTrieNode(hash, value);
              });
          nbNodesSaved.getAndIncrement();
        };

    final AtomicReference<StackTrie.FlatDatabaseUpdater> flatDatabaseUpdater =
        new AtomicReference<>(noop());

    // we have a flat DB only with Bonsai
    worldStateStorageCoordinator.applyOnMatchingFlatModes(
        List.of(FlatDbMode.FULL, FlatDbMode.ARCHIVE),
        bonsaiWorldStateStorageStrategy -> {
          flatDatabaseUpdater.set(
              (key, value) ->
                  ((BonsaiWorldStateKeyValueStorage.Updater) updater)
                      .putAccountInfoState(Hash.wrap(key), value));
        });

    stackTrie.commit(flatDatabaseUpdater.get(), nodeUpdater);

    downloadState.getMetricsManager().notifyAccountsDownloaded(stackTrie.getElementsCount().get());

    return nbNodesSaved.get();
  }

  public void addResponse(
      final WorldStateProofProvider worldStateProofProvider,
      final NavigableMap<Bytes32, Bytes> accounts,
      final ArrayDeque<Bytes> proofs) {
    if (!accounts.isEmpty() || !proofs.isEmpty()) {
      if (!worldStateProofProvider.isValidRangeProof(
          startKeyHash, endKeyHash, Bytes32.wrap(getRootHash().getBytes()), proofs, accounts)) {
        // this happens on repivot and on bad proofs
        LOG.atTrace()
            .setMessage("invalid range proof received for account range {} {}")
            .addArgument(accounts.firstKey())
            .addArgument(accounts.lastKey())
            .log();
        isProofValid = Optional.of(false);
      } else {
        stackTrie.addElement(startKeyHash, proofs, accounts);
        isProofValid = Optional.of(true);
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
    return isProofValid.orElse(false);
  }

  @Override
  public Stream<SnapDataRequest> getChildRequests(
      final SnapWorldDownloadState downloadState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState snapSyncState) {
    final List<SnapDataRequest> childRequests = new ArrayList<>();

    final StackTrie.TaskElement taskElement = stackTrie.getElement(startKeyHash);
    // new request is added if the response does not match all the requested range
    findNewBeginElementInRange(
            Bytes32.wrap(getRootHash().getBytes()),
            taskElement.proofs(),
            taskElement.keys(),
            endKeyHash)
        .ifPresentOrElse(
            missingRightElement -> {
              downloadState
                  .getMetricsManager()
                  .notifyRangeProgress(DOWNLOAD, missingRightElement, endKeyHash);
              childRequests.add(
                  createAccountRangeDataRequest(getRootHash(), missingRightElement, endKeyHash));
            },
            () ->
                downloadState
                    .getMetricsManager()
                    .notifyRangeProgress(DOWNLOAD, endKeyHash, endKeyHash));

    // find missing storages and code
    for (Map.Entry<Bytes32, Bytes> account : taskElement.keys().entrySet()) {
      final PmtStateTrieAccountValue accountValue =
          PmtStateTrieAccountValue.readFrom(RLP.input(account.getValue()));
      if (!accountValue.getStorageRoot().equals(Hash.EMPTY_TRIE_HASH)) {
        childRequests.add(
            createStorageRangeDataRequest(
                getRootHash(),
                account.getKey(),
                Bytes32.wrap(accountValue.getStorageRoot().getBytes()),
                startStorageRange.orElse(MIN_RANGE),
                endStorageRange.orElse(MAX_RANGE)));
      }
      if (!accountValue.getCodeHash().equals(Hash.EMPTY)) {
        childRequests.add(
            createBytecodeRequest(
                account.getKey(),
                getRootHash(),
                Bytes32.wrap(accountValue.getCodeHash().getBytes())));
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

  @VisibleForTesting
  public NavigableMap<Bytes32, Bytes> getAccounts() {
    return stackTrie.getElement(startKeyHash).keys();
  }

  @Override
  public void clear() {
    stackTrie.clear();
    isProofValid = Optional.of(false);
  }

  public Bytes serialize() {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeByte(getRequestType().getValue());
          out.writeBytes(getRootHash().getBytes());
          out.writeBytes(getStartKeyHash());
          out.writeBytes(getEndKeyHash());
          out.endList();
        });
  }

  public static AccountRangeDataRequest deserialize(final RLPInput in) {
    in.enterList();
    in.skipNext(); // skip request type
    final Hash rootHash = Hash.wrap(in.readBytes32());
    final Bytes32 startKeyHash = in.readBytes32();
    final Bytes32 endKeyHash = in.readBytes32();
    in.leaveList();
    return createAccountRangeDataRequest(rootHash, startKeyHash, endKeyHash);
  }
}
