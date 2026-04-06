/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.sync;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.tasks.CompleteBlocksWithPeerTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class DownloadBodiesStep
    implements Function<List<BlockHeader>, CompletableFuture<List<Block>>> {

  private final ProtocolSchedule protocolSchedule;
  private final EthContext ethContext;

  public DownloadBodiesStep(final ProtocolSchedule protocolSchedule, final EthContext ethContext) {
    this.protocolSchedule = protocolSchedule;
    this.ethContext = ethContext;
  }

  @Override
  public CompletableFuture<List<Block>> apply(final List<BlockHeader> blockHeaders) {
    return ethContext
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              final CompleteBlocksWithPeerTask completeBlocksWithPeerTask =
                  new CompleteBlocksWithPeerTask(
                      protocolSchedule, blockHeaders, ethContext.getPeerTaskExecutor());
              final List<Block> blocks = completeBlocksWithPeerTask.retrieveBlocksFromPeers();
              return CompletableFuture.completedFuture(blocks);
            });
  }
}
