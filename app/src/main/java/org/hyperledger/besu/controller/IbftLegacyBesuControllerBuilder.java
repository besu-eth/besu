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
package org.hyperledger.besu.controller;

import org.hyperledger.besu.config.IbftLegacyConfigOptions;
import org.hyperledger.besu.consensus.common.EpochManager;
import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.consensus.common.validator.blockbased.BlockValidatorProvider;
import org.hyperledger.besu.consensus.ibftlegacy.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.ibftlegacy.IbftLegacyBlockInterface;
import org.hyperledger.besu.consensus.ibftlegacy.IbftProtocolSchedule;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.blockcreation.NoopMiningCoordinator;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Ibft legacy besu controller builder. */
public class IbftLegacyBesuControllerBuilder extends BesuControllerBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(IbftLegacyBesuControllerBuilder.class);
  private final BftBlockInterface blockInterface;

  /** Default constructor */
  public IbftLegacyBesuControllerBuilder() {
    LOG.warn(
        "IBFT1 is deprecated. This consensus configuration should be used only while migrating to another consensus mechanism.");
    this.blockInterface = new IbftLegacyBlockInterface(new IbftExtraDataCodec());
  }

  @Override
  protected MiningCoordinator createMiningCoordinator(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final TransactionPool transactionPool,
      final MiningConfiguration miningConfiguration,
      final SyncState syncState,
      final EthProtocolManager ethProtocolManager) {
    return new NoopMiningCoordinator();
  }

  @Override
  protected ProtocolSchedule createProtocolSchedule() {
    return IbftProtocolSchedule.create(
        genesisConfigOptions,
        dataStorageConfiguration.getRevertReasonEnabled(),
        evmConfiguration,
        metricsSystem);
  }

  @Override
  protected BftContext createConsensusContext(
      final Blockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final ProtocolSchedule protocolSchedule) {
    final IbftLegacyConfigOptions ibftConfig = genesisConfigOptions.getIbftLegacyConfigOptions();
    final EpochManager epochManager = new EpochManager(ibftConfig.getEpochLength());
    final ValidatorProvider validatorProvider =
        BlockValidatorProvider.nonForkingValidatorProvider(
            blockchain, epochManager, blockInterface);

    return new BftContext(validatorProvider, epochManager, blockInterface);
  }

  @Override
  protected PluginServiceFactory createAdditionalPluginServices(
      final Blockchain blockchain, final ProtocolContext protocolContext) {
    return new NoopPluginServiceFactory();
  }

  @Override
  protected void validateContext(final ProtocolContext context) {
    final BlockHeader genesisBlockHeader = context.getBlockchain().getGenesisBlock().getHeader();

    if (blockInterface.validatorsInBlock(genesisBlockHeader).isEmpty()) {
      LOG.warn("Genesis block contains no signers - chain will not progress.");
    }
  }
}
