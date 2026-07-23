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
package org.hyperledger.besu.consensus.clique.headervalidationrules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.clique.CliqueBlockInterface;
import org.hyperledger.besu.consensus.clique.CliqueContext;
import org.hyperledger.besu.consensus.clique.CliqueHelpers;
import org.hyperledger.besu.consensus.clique.TestHelpers;
import org.hyperledger.besu.consensus.common.EpochManager;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Util;

import java.util.List;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CliqueDifficultyValidationRuleTest {

  private final KeyPair proposerKeyPair = SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private final List<Address> validatorList = Lists.newArrayList();
  private ProtocolContext cliqueProtocolContext;
  private BlockHeaderTestFixture blockHeaderBuilder;
  private final CliqueBlockInterface blockInterface = new CliqueBlockInterface();

  @BeforeEach
  public void setup() {
    final Address localAddress = Util.publicKeyToAddress(proposerKeyPair.getPublicKey());
    validatorList.add(localAddress);
    validatorList.add(AddressHelpers.calculateAddressWithRespectTo(localAddress, 1));

    final ValidatorProvider validatorProvider = mock(ValidatorProvider.class);
    when(validatorProvider.getValidatorsAfterBlock(any())).thenReturn(validatorList);

    final CliqueContext cliqueContext =
        new CliqueContext(validatorProvider, new EpochManager(10), blockInterface);
    CliqueHelpers.setCliqueContext(cliqueContext);
    cliqueProtocolContext =
        new ProtocolContext.Builder().withConsensusContext(cliqueContext).build();
    blockHeaderBuilder = new BlockHeaderTestFixture();
  }

  @Test
  public void isTrueIfInTurnValidatorSuppliesDifficultyOfTwo() {
    final long IN_TURN_BLOCK_NUMBER = validatorList.size(); // i.e. proposer is 'in turn'
    final Difficulty REPORTED_DIFFICULTY = Difficulty.of(2);

    blockHeaderBuilder.number(IN_TURN_BLOCK_NUMBER - 1L);
    final BlockHeader parentHeader =
        TestHelpers.createCliqueSignedBlockHeader(
            blockHeaderBuilder, proposerKeyPair, validatorList);

    blockHeaderBuilder.number(IN_TURN_BLOCK_NUMBER).difficulty(REPORTED_DIFFICULTY);
    final BlockHeader newBlock =
        TestHelpers.createCliqueSignedBlockHeader(
            blockHeaderBuilder, proposerKeyPair, validatorList);

    final CliqueDifficultyValidationRule diffValidationRule = new CliqueDifficultyValidationRule();
    assertThat(diffValidationRule.validate(newBlock, parentHeader, cliqueProtocolContext)).isTrue();
  }

  @Test
  public void isTrueIfOutTurnValidatorSuppliesDifficultyOfOne() {
    final long OUT_OF_TURN_BLOCK_NUMBER = validatorList.size() - 1L;
    final Difficulty REPORTED_DIFFICULTY = Difficulty.ONE;

    blockHeaderBuilder.number(OUT_OF_TURN_BLOCK_NUMBER - 1L);
    final BlockHeader parentHeader =
        TestHelpers.createCliqueSignedBlockHeader(
            blockHeaderBuilder, proposerKeyPair, validatorList);

    blockHeaderBuilder.number(OUT_OF_TURN_BLOCK_NUMBER).difficulty(REPORTED_DIFFICULTY);
    final BlockHeader newBlock =
        TestHelpers.createCliqueSignedBlockHeader(
            blockHeaderBuilder, proposerKeyPair, validatorList);

    final CliqueDifficultyValidationRule diffValidationRule = new CliqueDifficultyValidationRule();
    assertThat(diffValidationRule.validate(newBlock, parentHeader, cliqueProtocolContext)).isTrue();
  }

  @Test
  public void isFalseIfOutTurnValidatorSuppliesDifficultyOfTwo() {
    final long OUT_OF_TURN_BLOCK_NUMBER = validatorList.size() - 1L;
    final Difficulty REPORTED_DIFFICULTY = Difficulty.of(2);

    blockHeaderBuilder.number(OUT_OF_TURN_BLOCK_NUMBER - 1L);
    final BlockHeader parentHeader =
        TestHelpers.createCliqueSignedBlockHeader(
            blockHeaderBuilder, proposerKeyPair, validatorList);

    blockHeaderBuilder.number(OUT_OF_TURN_BLOCK_NUMBER).difficulty(REPORTED_DIFFICULTY);
    final BlockHeader newBlock =
        TestHelpers.createCliqueSignedBlockHeader(
            blockHeaderBuilder, proposerKeyPair, validatorList);

    final CliqueDifficultyValidationRule diffValidationRule = new CliqueDifficultyValidationRule();
    assertThat(diffValidationRule.validate(newBlock, parentHeader, cliqueProtocolContext))
        .isFalse();
  }

  @Test
  public void isFalseIfInTurnValidatorSuppliesDifficultyOfOne() {
    final long IN_TURN_BLOCK_NUMBER = validatorList.size();
    final Difficulty REPORTED_DIFFICULTY = Difficulty.ONE;

    blockHeaderBuilder.number(IN_TURN_BLOCK_NUMBER - 1L);
    final BlockHeader parentHeader =
        TestHelpers.createCliqueSignedBlockHeader(
            blockHeaderBuilder, proposerKeyPair, validatorList);

    blockHeaderBuilder.number(IN_TURN_BLOCK_NUMBER).difficulty(REPORTED_DIFFICULTY);
    final BlockHeader newBlock =
        TestHelpers.createCliqueSignedBlockHeader(
            blockHeaderBuilder, proposerKeyPair, validatorList);

    final CliqueDifficultyValidationRule diffValidationRule = new CliqueDifficultyValidationRule();
    assertThat(diffValidationRule.validate(newBlock, parentHeader, cliqueProtocolContext))
        .isFalse();
  }
}
