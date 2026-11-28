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
package org.hyperledger.besu.ethereum.eth.sync.fullsync.era1prepipeline;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.ChainDownloader;
import org.hyperledger.besu.services.pipeline.Pipeline;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class FileImportChainDownloader implements ChainDownloader {

  private final FileImportPipelineFactory fileImportPipelineFactory;
  private final Blockchain blockchain;
  private final EthScheduler ethScheduler;

  private Optional<Pipeline<?>> pipeline = Optional.empty();

  public FileImportChainDownloader(
      final FileImportPipelineFactory fileImportPipelineFactory,
      final Blockchain blockchain,
      final EthScheduler ethScheduler) {
    this.fileImportPipelineFactory = fileImportPipelineFactory;
    this.blockchain = blockchain;
    this.ethScheduler = ethScheduler;
  }

  @Override
  public CompletableFuture<Void> start() {
    pipeline =
        Optional.of(
            fileImportPipelineFactory.createFileImportPipelineForCurrentBlockNumber(
                blockchain.getChainHeadBlockNumber()));
    return pipeline
        .map(
            (p) ->
                ethScheduler
                    .startPipeline(p)
                    .whenComplete((result, error) -> fileImportPipelineFactory.close()))
        .orElseThrow(
            () -> new RuntimeException("Failed to construct and start file import pipeline"));
  }

  @Override
  public void cancel() {
    pipeline.ifPresent((p) -> p.abort());
  }
}
