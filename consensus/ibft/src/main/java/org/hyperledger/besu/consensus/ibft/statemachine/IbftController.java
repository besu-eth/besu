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
package org.hyperledger.besu.consensus.ibft.statemachine;

import org.hyperledger.besu.consensus.common.bft.Gossiper;
import org.hyperledger.besu.consensus.common.bft.MessageTracker;
import org.hyperledger.besu.consensus.common.bft.SynchronizerUpdater;
import org.hyperledger.besu.consensus.common.bft.statemachine.BaseBftController;
import org.hyperledger.besu.consensus.common.bft.statemachine.BaseBlockHeightManager;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftFinalState;
import org.hyperledger.besu.consensus.common.bft.statemachine.FutureMessageBuffer;
import org.hyperledger.besu.consensus.ibft.messagedata.CommitMessageData;
import org.hyperledger.besu.consensus.ibft.messagedata.IbftV2;
import org.hyperledger.besu.consensus.ibft.messagedata.PrepareMessageData;
import org.hyperledger.besu.consensus.ibft.messagedata.ProposalMessageData;
import org.hyperledger.besu.consensus.ibft.messagedata.RoundChangeMessageData;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

/** The Ibft controller. */
public class IbftController extends BaseBftController {

  private BaseIbftBlockHeightManager currentHeightManager;
  private final IbftBlockHeightManagerFactory ibftBlockHeightManagerFactory;

  /**
   * Instantiates a new Ibft controller.
   *
   * @param blockchain the blockchain
   * @param bftFinalState the bft final state
   * @param ibftBlockHeightManagerFactory the ibft block height manager factory
   * @param gossiper the gossiper
   * @param duplicateMessageTracker the duplicate message tracker
   * @param futureMessageBuffer the future message buffer
   * @param synchronizerUpdater the synchronizer updater
   */
  public IbftController(
      final Blockchain blockchain,
      final BftFinalState bftFinalState,
      final IbftBlockHeightManagerFactory ibftBlockHeightManagerFactory,
      final Gossiper gossiper,
      final MessageTracker duplicateMessageTracker,
      final FutureMessageBuffer<Message> futureMessageBuffer,
      final SynchronizerUpdater synchronizerUpdater) {

    super(
        blockchain,
        bftFinalState,
        gossiper,
        duplicateMessageTracker,
        futureMessageBuffer,
        synchronizerUpdater);
    this.ibftBlockHeightManagerFactory = ibftBlockHeightManagerFactory;
  }

  @Override
  protected void handleMessage(final Message message) {
    final MessageData messageData = message.getData();

    switch (messageData.getCode()) {
      case IbftV2.PROPOSAL:
        consumeMessage(
            message,
            ProposalMessageData.fromMessageData(messageData).decode(currentValidatorCount()),
            currentHeightManager::handleProposalPayload);
        break;

      case IbftV2.PREPARE:
        consumeMessage(
            message,
            PrepareMessageData.fromMessageData(messageData).decode(),
            currentHeightManager::handlePreparePayload);
        break;

      case IbftV2.COMMIT:
        consumeMessage(
            message,
            CommitMessageData.fromMessageData(messageData).decode(),
            currentHeightManager::handleCommitPayload);
        break;

      case IbftV2.ROUND_CHANGE:
        consumeMessage(
            message,
            RoundChangeMessageData.fromMessageData(messageData).decode(),
            currentHeightManager::handleRoundChangePayload);
        break;

      default:
        throw new IllegalArgumentException(
            String.format(
                "Received message with messageCode=%d does not conform to any recognised IBFT message structure",
                message.getData().getCode()));
    }
  }

  @Override
  protected void createNewHeightManager(final BlockHeader parentHeader) {
    currentHeightManager = ibftBlockHeightManagerFactory.create(parentHeader);
  }

  @Override
  protected BaseBlockHeightManager getCurrentHeightManager() {
    return currentHeightManager;
  }

  @Override
  protected void stopCurrentHeightManager(final BlockHeader parentHeader) {
    currentHeightManager = ibftBlockHeightManagerFactory.createNoOpBlockHeightManager(parentHeader);
  }
}
