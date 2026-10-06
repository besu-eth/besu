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
package org.hyperledger.besu.consensus.ibft.messagedata;

import org.hyperledger.besu.consensus.common.bft.messagedata.AbstractBftMessageData;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import org.apache.tuweni.bytes.Bytes;

/** The Proposal message data. */
public class ProposalMessageData extends AbstractBftMessageData {

  private static final int MESSAGE_CODE = IbftV2.PROPOSAL;

  private ProposalMessageData(final Bytes data) {
    super(data);
  }

  /**
   * From message data create proposal message data.
   *
   * @param messageData the message data
   * @return the proposal message data
   */
  public static ProposalMessageData fromMessageData(final MessageData messageData) {
    return fromMessageData(
        messageData, MESSAGE_CODE, ProposalMessageData.class, ProposalMessageData::new);
  }

  /**
   * Decode.
   *
   * @return the proposal
   */
  public Proposal decode() {
    return Proposal.decode(data);
  }

  /**
   * Decode, bounding signature recoveries to the maximum for the given validator-set size.
   *
   * @param validatorCount the current validator-set size
   * @return the proposal
   */
  public Proposal decode(final int validatorCount) {
    return Proposal.decode(data, validatorCount);
  }

  /**
   * Create proposal message data from proposal.
   *
   * @param proposal the proposal
   * @return the proposal message data
   */
  public static ProposalMessageData create(final Proposal proposal) {
    return new ProposalMessageData(proposal.encode());
  }

  @Override
  public int getCode() {
    return MESSAGE_CODE;
  }
}
