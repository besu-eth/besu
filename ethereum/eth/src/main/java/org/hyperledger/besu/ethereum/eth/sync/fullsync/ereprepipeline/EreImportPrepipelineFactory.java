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
package org.hyperledger.besu.ethereum.eth.sync.fullsync.ereprepipeline;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.SyncTerminationCondition;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.era1prepipeline.FileImportPipelineFactory;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

/** Imports Ere archives through the existing full block validation path. */
public class EreImportPrepipelineFactory implements FileImportPipelineFactory {
  private final MetricsSystem metrics;
  private final URI directory;
  private final int concurrency;
  private final ProtocolSchedule schedule;
  private final ProtocolContext context;
  private final EthContext ethContext;
  private final SyncTerminationCondition mergeTransitionReached;
  private final EreFileReader reader;

  public EreImportPrepipelineFactory(
      final MetricsSystem metrics,
      final URI directory,
      final int concurrency,
      final ProtocolSchedule schedule,
      final ProtocolContext context,
      final EthContext ethContext,
      final SyncTerminationCondition mergeTransitionReached) {
    this.metrics = metrics;
    this.directory = directory;
    this.concurrency = concurrency;
    this.schedule = schedule;
    this.context = context;
    this.ethContext = ethContext;
    this.mergeTransitionReached = mergeTransitionReached;
    this.reader = new EreFileReader(ScheduleBasedBlockHeaderFunctions.create(schedule));
  }

  @Override
  public Pipeline<URI> createFileImportPipelineForCurrentBlockNumber(final long head) {
    final var counter =
        metrics.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "ere_file_import_prepipeline_processed_total",
            "Number of entries processed by each Ere pipeline stage",
            "step",
            "action");
    final var importBlock =
        new EreImportBlockStep(
            schedule,
            context,
            ethContext,
            context.getBlockchain().getBlockHeader(head).orElseThrow(),
            mergeTransitionReached);
    return PipelineBuilder.createPipelineFrom(
            "Ere file source",
            new EreFileSource(directory, head),
            concurrency,
            counter,
            true,
            "Ere file import prepipeline")
        .thenProcessAsyncOrdered(
            "Stage Ere archive",
            uri ->
                ethContext
                    .getScheduler()
                    .scheduleServiceTask(
                        () -> {
                          try {
                            return CompletableFuture.completedFuture(reader.prepare(uri));
                          } catch (IOException e) {
                            return CompletableFuture.failedFuture(e);
                          }
                        }),
            concurrency)
        .andFinishWith(
            "Import Ere blocks",
            archive -> {
              try {
                reader.read(
                    archive,
                    importBlock.getLastImportedHeader(),
                    context.getBlockchain().getBlockHeader(0).orElseThrow().getHash(),
                    importBlock);
              } catch (IOException e) {
                throw new UncheckedIOException(
                    "Failed importing Ere archive for epoch "
                        + EreFileSource.metadata(archive.uri()).epoch(),
                    e);
              }
            });
  }

  @Override
  public void close() {
    reader.close();
  }
}
