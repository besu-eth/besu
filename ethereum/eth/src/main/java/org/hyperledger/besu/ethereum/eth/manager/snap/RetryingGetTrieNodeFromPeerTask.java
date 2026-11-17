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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.SnapProtocolVersion;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.task.AbstractRetryingSwitchingPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.tuweni.bytes.Bytes;

public class RetryingGetTrieNodeFromPeerTask
    extends AbstractRetryingSwitchingPeerTask<Map<Bytes, Bytes>> {

  public static final int MAX_RETRIES = 4;

  private final EthContext ethContext;
  private final Map<Bytes, List<Bytes>> paths;
  private final BlockHeader blockHeader;
  private final MetricsSystem metricsSystem;

  private RetryingGetTrieNodeFromPeerTask(
      final EthContext ethContext,
      final Map<Bytes, List<Bytes>> paths,
      final BlockHeader blockHeader,
      final MetricsSystem metricsSystem) {
    super(ethContext, metricsSystem, Map::isEmpty, MAX_RETRIES);
    this.ethContext = ethContext;
    this.paths = paths;
    this.blockHeader = blockHeader;
    this.metricsSystem = metricsSystem;
  }

  public static EthTask<Map<Bytes, Bytes>> forTrieNodes(
      final EthContext ethContext,
      final Map<Bytes, List<Bytes>> paths,
      final BlockHeader blockHeader,
      final MetricsSystem metricsSystem) {
    return new RetryingGetTrieNodeFromPeerTask(ethContext, paths, blockHeader, metricsSystem);
  }

  @Override
  protected CompletableFuture<Map<Bytes, Bytes>> executeTaskOnCurrentPeer(final EthPeer peer) {
    final GetTrieNodeFromPeerTask task =
        GetTrieNodeFromPeerTask.forTrieNodes(ethContext, paths, blockHeader, metricsSystem);
    return executeSubTask(task::run)
        .thenApply(
            peerResult -> {
              result.complete(peerResult.getResult());
              return peerResult.getResult();
            });
  }

  // snap/2 has no trie node requests, so a peer that agreed on it disconnects on one
  @Override
  protected boolean isSuitablePeer(final EthPeerImmutableAttributes peer) {
    return peer.isServingSnap()
        && peer.ethPeer().getAgreedCapabilities().contains(SnapProtocolVersion.V1.getCapability());
  }
}
