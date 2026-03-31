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

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType.BYTECODES;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;
import static org.slf4j.LoggerFactory.getLogger;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;

/** Returns a list of bytecodes */
public class BytecodeRequest extends SnapDataRequest {

  private static final Logger LOG = getLogger(BytecodeRequest.class);

  private final Bytes32 accountHash;
  private final Bytes32 codeHash;

  private Bytes code;

  protected BytecodeRequest(
      final Hash rootHash, final Bytes32 accountHash, final Bytes32 codeHash) {
    super(BYTECODES, rootHash);
    LOG.trace("create get bytecode data request for {} with root hash={}", accountHash, rootHash);
    this.accountHash = accountHash;
    this.codeHash = codeHash;
    this.code = Bytes.EMPTY;
  }

  @Override
  protected int doPersist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapWorldDownloadState downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {

    applyForStrategy(
        updater,
        onBonsai -> {
          onBonsai.putCode(Hash.wrap(accountHash), Hash.wrap(codeHash), code);
        },
        onForest -> {
          onForest.putCode(codeHash, code);
        });
    downloadState.getMetricsManager().notifyCodeDownloaded();
    return possibleParent
        .map(
            trieNodeDataRequest ->
                trieNodeDataRequest.saveParent(
                        worldStateStorageCoordinator,
                        updater,
                        downloadState,
                        snapSyncState,
                        snapSyncConfiguration)
                    + 1)
        .orElse(1);
  }

  @Override
  public boolean isResponseReceived() {
    return !code.isEmpty();
  }

  @Override
  public Stream<SnapDataRequest> getChildRequests(
      final SnapWorldDownloadState downloadState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState snapSyncState) {
    return Stream.empty();
  }

  public Bytes32 getAccountHash() {
    return accountHash;
  }

  @Override
  public void clear() {
    setCode(Bytes.EMPTY);
  }

  public Bytes32 getCodeHash() {
    return codeHash;
  }

  public void setCode(final Bytes code) {
    this.code = code;
  }

  @Override
  public long getPriority() {
    return 0;
  }

  @Override
  public int getDepth() {
    return 0;
  }
}
