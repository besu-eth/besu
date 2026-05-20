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
package org.hyperledger.besu.consensus.qbft.core.validation;

import static com.google.common.collect.Iterables.toArray;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.consensus.qbft.core.validation.ValidationTestHelpers.createPreparePayloads;
import static org.hyperledger.besu.consensus.qbft.core.validation.ValidationTestHelpers.createPreparedCertificate;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundHelpers;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.QbftBlockTestFixture;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparedRoundMetadata;
import org.hyperledger.besu.consensus.qbft.core.payload.RoundChangePayload;
import org.hyperledger.besu.consensus.qbft.core.statemachine.PreparedCertificate;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator.ValidationResult;
import org.hyperledger.besu.consensus.qbft.core.types.QbftProtocolSchedule;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Hash;

import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RoundChangeMessageValidatorTest {

  @Mock private RoundChangePayloadValidator payloadValidator;
  @Mock private QbftProtocolSchedule protocolSchedule;
  @Mock private QbftBlockValidator blockValidator;
  @Mock private QbftBlockCodec blockEncoder;

  private RoundChangeMessageValidator messageValidator;
  private QbftNodeList validators;
  private static final int VALIDATOR_COUNT = 4;
  private static final int CHAIN_HEIGHT = 3;
  private final ConsensusRoundIdentifier targetRound =
      new ConsensusRoundIdentifier(CHAIN_HEIGHT, 3);
  private final ConsensusRoundIdentifier roundIdentifier =
      ConsensusRoundHelpers.createFrom(targetRound, 0, -1);

  @BeforeEach
  public void setup() {
    validators = QbftNodeList.createNodes(VALIDATOR_COUNT, blockEncoder);

    lenient().when(protocolSchedule.getBlockValidator(any())).thenReturn(blockValidator);
  }

  @Test
  public void roundChangeWithNoPiggyBackedDataIsValid() {
    when(payloadValidator.validate(any())).thenReturn(true);
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    for (int i = 0; i < VALIDATOR_COUNT; i++) {
      final RoundChange message =
          validators.getMessageFactory(i).createRoundChange(targetRound, Optional.empty());
      assertThat(messageValidator.validate(message)).isTrue();
    }
  }

  @Test
  public void roundChangeWithValidPiggyBackDataIsValid() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(true, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final PreparedCertificate prepCert =
        createPreparedCertificate(
            block, roundIdentifier, toArray(validators.getNodes(), QbftNode.class));

    final RoundChange message =
        validators.getMessageFactory(0).createRoundChange(targetRound, Optional.of(prepCert));
    assertThat(messageValidator.validate(message)).isTrue();
  }

  @Test
  public void roundChangeWithBlockRoundMismatchingPreparesIsValid() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(true, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final PreparedCertificate prepCert =
        createPreparedCertificate(
            block,
            ConsensusRoundHelpers.createFrom(roundIdentifier, 0, +1),
            toArray(validators.getNodes(), QbftNode.class));

    final RoundChange message =
        validators.getMessageFactory(0).createRoundChange(targetRound, Optional.of(prepCert));
    assertThat(messageValidator.validate(message)).isTrue();
  }

  @Test
  public void blockIsInvalidFailsValidation() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(false, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final PreparedCertificate prepCert =
        createPreparedCertificate(
            block, roundIdentifier, toArray(validators.getNodes(), QbftNode.class));

    final RoundChange message =
        validators.getMessageFactory(0).createRoundChange(targetRound, Optional.of(prepCert));
    assertThat(messageValidator.validate(message)).isFalse();
  }

  @Test
  public void invalidEmbeddedPayloadFailsValidation() {
    when(payloadValidator.validate(any())).thenReturn(false);
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final RoundChange message =
        validators.getMessageFactory(0).createRoundChange(targetRound, Optional.empty());
    assertThat(messageValidator.validate(message)).isFalse();
  }

  @Test
  public void insufficientPiggyBackedPrepareMessagesIsInvalid() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(true, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final PreparedCertificate prepCert =
        createPreparedCertificate(
            block, roundIdentifier, validators.getNode(0), validators.getNode(1));

    final RoundChange message =
        validators.getMessageFactory(0).createRoundChange(targetRound, Optional.of(prepCert));
    assertThat(messageValidator.validate(message)).isFalse();
  }

  @Test
  public void prepareFromNonValidatorFails() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(true, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftNode nonValidator = QbftNode.create(blockEncoder);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final PreparedCertificate prepCert =
        createPreparedCertificate(
            block, roundIdentifier, validators.getNode(0), validators.getNode(1), nonValidator);

    final RoundChange message =
        validators.getMessageFactory(0).createRoundChange(targetRound, Optional.of(prepCert));
    assertThat(messageValidator.validate(message)).isFalse();
  }

  @Test
  public void validationFailsIfPreparedMetadataContainsDifferentRoundToBlock() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(true, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final PreparedCertificate prepCert =
        new PreparedCertificate(
            block,
            validators.getNodes().stream()
                .map(
                    n ->
                        n.getMessageFactory()
                            .createPrepare(roundIdentifier, block.getHash())
                            .getSignedPayload())
                .collect(Collectors.toList()),
            roundIdentifier.getRoundNumber() - 1);

    final RoundChange message =
        validators.getMessageFactory(0).createRoundChange(targetRound, Optional.of(prepCert));
    assertThat(messageValidator.validate(message)).isFalse();
  }

  @Test
  public void validationFailsIfPreparesContainsDifferentRoundToBlock() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(true, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final PreparedCertificate prepCert =
        new PreparedCertificate(
            block,
            validators.getNodes().stream()
                .map(
                    n ->
                        n.getMessageFactory()
                            .createPrepare(
                                ConsensusRoundHelpers.createFrom(roundIdentifier, 0, -1),
                                block.getHash())
                            .getSignedPayload())
                .collect(Collectors.toList()),
            roundIdentifier.getRoundNumber());

    final RoundChange message =
        validators.getMessageFactory(0).createRoundChange(targetRound, Optional.of(prepCert));
    assertThat(messageValidator.validate(message)).isFalse();
  }

  @Test
  public void validationFailsIfPreparesContainsWrongHeight() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(true, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final PreparedCertificate prepCert =
        new PreparedCertificate(
            block,
            validators.getNodes().stream()
                .map(
                    n ->
                        n.getMessageFactory()
                            .createPrepare(
                                ConsensusRoundHelpers.createFrom(roundIdentifier, +1, 0),
                                block.getHash())
                            .getSignedPayload())
                .collect(Collectors.toList()),
            roundIdentifier.getRoundNumber());

    final RoundChange message =
        validators.getMessageFactory(0).createRoundChange(targetRound, Optional.of(prepCert));
    assertThat(messageValidator.validate(message)).isFalse();
  }

  @Test
  public void validationFailsIfPreparesHaveDuplicateAuthors() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(true, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final PreparedCertificate prepCert =
        createPreparedCertificate(
            block,
            roundIdentifier,
            validators.getNode(0),
            validators.getNode(1),
            validators.getNode(1));

    final RoundChange message =
        validators.getMessageFactory(0).createRoundChange(targetRound, Optional.of(prepCert));

    assertThat(messageValidator.validate(message)).isFalse();
  }

  @Test
  public void validationFailsIfBlockExistsButNotPreparedMetadata() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(true, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();

    final RoundChangePayload payload = new RoundChangePayload(targetRound, Optional.empty());
    final SECPSignature signature =
        validators
            .getNode(0)
            .getNodeKey()
            .sign(Bytes32.wrap(payload.hashForSignature().getBytes()));

    final RoundChange message =
        new RoundChange(
            SignedData.create(payload, signature),
            Optional.of(block),
            Optional.empty(),
            blockEncoder,
            emptyList());

    assertThat(messageValidator.validate(message)).isFalse();
  }

  @Test
  public void validationFailsIfBlockHashDoesNotMatchPreparedMetadata() {
    when(payloadValidator.validate(any())).thenReturn(true);
    when(blockValidator.validateBlock(any(), any()))
        .thenReturn(new ValidationResult(true, Optional.empty()));
    messageValidator =
        new RoundChangeMessageValidator(
            payloadValidator,
            BftHelpers.calculateRequiredValidatorQuorum(VALIDATOR_COUNT),
            CHAIN_HEIGHT,
            validators.getNodeAddresses(),
            protocolSchedule);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();

    final RoundChangePayload payload =
        new RoundChangePayload(
            targetRound,
            Optional.of(
                new PreparedRoundMetadata(
                    Hash.fromHexStringLenient("0x1"), roundIdentifier.getRoundNumber())));
    final SECPSignature signature =
        validators
            .getNode(0)
            .getNodeKey()
            .sign(Bytes32.wrap(payload.hashForSignature().getBytes()));

    final RoundChange message =
        new RoundChange(
            SignedData.create(payload, signature),
            Optional.of(block),
            Optional.empty(),
            blockEncoder,
            createPreparePayloads(
                roundIdentifier, block.getHash(), toArray(validators.getNodes(), QbftNode.class)));

    assertThat(messageValidator.validate(message)).isFalse();
  }
}
