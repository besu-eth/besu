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
package org.hyperledger.besu.consensus.merge;

import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardChain;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncAlgorithmFactory;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncContext;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.plugin.services.MetricsSystem;

/** The Transition backward sync context. */
public class TransitionBackwardSyncContext extends BackwardSyncContext {

  private final TransitionProtocolSchedule transitionProtocolSchedule;

  /**
   * Instantiates a new Transition backward sync context.
   *
   * @param protocolContext the protocol context
   * @param transitionProtocolSchedule the transition protocol schedule
   * @param metricsSystem the metrics system
   * @param ethContext the eth context
   * @param syncState the sync state
   * @param storageProvider the storage provider
   */
  public TransitionBackwardSyncContext(
      final ProtocolContext protocolContext,
      final TransitionProtocolSchedule transitionProtocolSchedule,
      final SynchronizerConfiguration synchronizerConfiguration,
      final MetricsSystem metricsSystem,
      final EthContext ethContext,
      final SyncState syncState,
      final StorageProvider storageProvider,
      final BackwardSyncAlgorithmFactory backwardSyncAlgorithmFactory) {
    super(
        protocolContext,
        transitionProtocolSchedule,
        synchronizerConfiguration,
        metricsSystem,
        ethContext,
        syncState,
        BackwardChain.from(
            storageProvider, ScheduleBasedBlockHeaderFunctions.create(transitionProtocolSchedule)),
        backwardSyncAlgorithmFactory);
    this.transitionProtocolSchedule = transitionProtocolSchedule;
  }

  /**
   * Choose the correct protocolSchedule and blockvalidator by block rather than number. This should
   * be used in the merge transition, specifically when the chain has not yet finalized.
   */
  @Override
  public BlockValidator getBlockValidatorForBlock(final Block block) {
    return transitionProtocolSchedule
        .getByBlockHeaderWithTransitionReorgHandling(block.getHeader())
        .getBlockValidator();
  }
}
