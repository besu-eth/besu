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
package org.hyperledger.besu.consensus.ibft.validation;

import static java.util.Optional.empty;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;

import org.junit.jupiter.api.Test;

public class RoundChangeMessageValidatorTest {

  private final RoundChangePayloadValidator payloadValidator =
      mock(RoundChangePayloadValidator.class);
  private final MessageFactory messageFactory = new MessageFactory(NodeKeyUtils.generate());
  private final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(1, 1);

  private final ProposalBlockConsistencyValidator proposalBlockConsistencyValidator =
      mock(ProposalBlockConsistencyValidator.class);
  private final BftBlockInterface bftBlockInterface =
      new BftBlockInterface(new IbftExtraDataCodec());

  private final RoundChangeMessageValidator validator =
      new RoundChangeMessageValidator(
          payloadValidator, proposalBlockConsistencyValidator, bftBlockInterface);

  @Test
  public void underlyingPayloadValidatorIsInvokedWithCorrectParameters() {
    final RoundChange message = messageFactory.createRoundChange(roundIdentifier, empty());

    validator.validateRoundChange(message);
    verify(payloadValidator, times(1)).validateRoundChange(message.getSignedPayload());
  }
}
