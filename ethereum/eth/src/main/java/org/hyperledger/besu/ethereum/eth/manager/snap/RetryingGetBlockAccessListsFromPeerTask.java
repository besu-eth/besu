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
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.task.AbstractRetryingSwitchingPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RetryingGetBlockAccessListsFromPeerTask
    extends AbstractRetryingSwitchingPeerTask<List<BlockAccessList>> {

  public static final int MAX_RETRIES = 4;

  private final EthContext ethContext;
  private final List<BlockHeader> blockHeaders;
  private final MetricsSystem metricsSystem;

  private RetryingGetBlockAccessListsFromPeerTask(
      final EthContext ethContext,
      final List<BlockHeader> blockHeaders,
      final MetricsSystem metricsSystem) {
    super(ethContext, metricsSystem, List::isEmpty, MAX_RETRIES);
    this.ethContext = ethContext;
    this.blockHeaders = blockHeaders;
    this.metricsSystem = metricsSystem;
  }

  public static EthTask<List<BlockAccessList>> forBlockAccessLists(
      final EthContext ethContext,
      final List<BlockHeader> blockHeaders,
      final MetricsSystem metricsSystem) {
    return new RetryingGetBlockAccessListsFromPeerTask(ethContext, blockHeaders, metricsSystem);
  }

  @Override
  protected CompletableFuture<List<BlockAccessList>> executeTaskOnCurrentPeer(final EthPeer peer) {
    final GetBlockAccessListsFromPeerTask task =
        GetBlockAccessListsFromPeerTask.forBlockAccessLists(
            ethContext, blockHeaders, metricsSystem);
    return executeSubTask(task::run)
        .thenApply(
            peerResult -> {
              result.complete(peerResult.getResult());
              return peerResult.getResult();
            });
  }

  @Override
  protected boolean isSuitablePeer(final EthPeerImmutableAttributes peer) {
    return peer.isServingSnap()
        && peer.ethPeer().getAgreedCapabilities().contains(SnapProtocol.SNAP2);
  }
}
