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
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.FullImportBlockStep;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.SyncTerminationCondition;
import org.hyperledger.besu.ethereum.eth.sync.tasks.exceptions.InvalidBlockException;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;

import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes archive blocks in order without assigning post-Merge canonical forkchoice. */
final class EreImportBlockStep implements Consumer<Block> {
  private static final Logger LOG = LoggerFactory.getLogger(EreImportBlockStep.class);
  private final ProtocolSchedule schedule;
  private final ProtocolContext context;
  private final FullImportBlockStep preMergeImporter;
  private final SyncTerminationCondition mergeTransitionReached;
  private BlockHeader lastImportedHeader;

  EreImportBlockStep(
      final ProtocolSchedule schedule,
      final ProtocolContext context,
      final EthContext ethContext,
      final BlockHeader currentHead,
      final SyncTerminationCondition mergeTransitionReached) {
    this.schedule = schedule;
    this.context = context;
    this.lastImportedHeader = currentHead;
    this.mergeTransitionReached = mergeTransitionReached;
    // The file boundary, not network-sync TTD, terminates archive execution.
    preMergeImporter =
        new FullImportBlockStep(
            schedule,
            context,
            ethContext,
            SyncTerminationCondition.never(),
            HeaderValidationMode.FULL);
  }

  @Override
  public void accept(final Block block) {
    final BlockHeader header = block.getHeader();
    if (header.getNumber() != lastImportedHeader.getNumber() + 1
        || !header.getParentHash().equals(lastImportedHeader.getHash())) {
      throw InvalidBlockException.fromInvalidBlock(header);
    }
    final var blockchain = context.getBlockchain();
    final var storedBody = blockchain.getBlockBody(header.getHash());
    if (storedBody.isPresent() && !storedBody.get().equals(block.getBody())) {
      throw InvalidBlockException.fromInvalidBlock(header);
    }
    final var spec = schedule.getByBlockHeader(header);
    if (spec.isPoS() != header.getDifficulty().isZero()
        || (!spec.isPoS() && mergeTransitionReached.shouldStopDownload())) {
      throw InvalidBlockException.fromInvalidBlock(header);
    }
    if (!header.getDifficulty().isZero()) {
      preMergeImporter.accept(block);
    } else {
      // A previously executed payload can be reused only while its body and state are available.
      if (storedBody.isEmpty()
          || !context
              .getWorldStateArchive()
              .isWorldStateAvailable(header.getStateRoot(), header.getHash())) {
        final var result =
            spec.getBlockValidator()
                .validateAndProcessBlock(
                    context,
                    block,
                    HeaderValidationMode.FULL,
                    HeaderValidationMode.FULL,
                    Optional.empty(),
                    InclusionListTransactions.EMPTY,
                    false);
        if (!result.isSuccessful()) {
          throw InvalidBlockException.fromInvalidBlock(header);
        }
        final var outputs = result.getYield().orElseThrow();
        // Match Engine payload storage: only the consensus client's forkchoice selects a head.
        blockchain.storeBlock(block, outputs.getReceipts(), outputs.getBlockAccessList());
      }
      if (header.getNumber() % 200 == 0) {
        LOG.info("Processed Ere block {} ({})", header.getNumber(), header.getHash());
      }
    }
    lastImportedHeader = header;
  }

  BlockHeader getLastImportedHeader() {
    return lastImportedHeader;
  }
}
