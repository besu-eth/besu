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
package org.hyperledger.besu.consensus.qbft;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.consensus.common.bft.BftContextBuilder.setupContextWithBftExtraDataEncoder;

import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

public class QbftBlockHeaderValidationRulesetFactoryTest {

  private ProtocolContext protocolContext(final Collection<Address> validators) {
    return new ProtocolContext.Builder()
        .withConsensusContext(
            setupContextWithBftExtraDataEncoder(
                BftContext.class, validators, new QbftExtraDataCodec()))
        .build();
  }

  @Test
  public void bftValidateHeaderPasses() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2, proposerNodeKey, validators, parentHeader, Optional.empty())
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isTrue();
  }

  @Test
  public void bftValidateHeaderPassesWithBaseFee() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.of(Wei.ONE))
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2, proposerNodeKey, validators, parentHeader, Optional.of(Wei.ONE))
            .buildHeader();

    final BlockHeaderValidator validator =
        QbftBlockHeaderValidationRulesetFactory.blockHeaderValidator(
                Duration.ofSeconds(5), false, false, Optional.of(FeeMarket.london(1)))
            .build();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isTrue();
  }

  @Test
  public void bftValidateHeaderFailsWhenExtraDataDoesntContainValidatorList() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2, proposerNodeKey, emptyList(), parentHeader, Optional.empty())
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isFalse();
  }

  @Test
  public void bftValidateHeaderFailsOnCoinbaseData() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final Address nonProposerAddress =
        Util.publicKeyToAddress(NodeKeyUtils.generate().getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2, proposerNodeKey, validators, parentHeader, Optional.empty())
            .coinbase(nonProposerAddress)
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isFalse();
  }

  @Test
  public void bftValidateHeaderIgnoresNonceValue() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2,
                proposerNodeKey,
                validators,
                parentHeader,
                builder -> builder.nonce(3),
                Optional.empty())
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isTrue();
  }

  @Test
  public void bftValidateHeaderFailsOnTimestamp() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2, proposerNodeKey, validators, parentHeader, Optional.empty())
            .timestamp(100)
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isFalse();
  }

  @Test
  public void bftValidateHeaderFailsOnMixHash() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2,
                proposerNodeKey,
                validators,
                parentHeader,
                builder -> builder.mixHash(Hash.EMPTY_TRIE_HASH),
                Optional.empty())
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isFalse();
  }

  @Test
  public void bftValidateHeaderIgnoresOmmersValue() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2,
                proposerNodeKey,
                validators,
                parentHeader,
                builder -> builder.ommersHash(Hash.EMPTY_TRIE_HASH),
                Optional.empty())
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isTrue();
  }

  @Test
  public void bftValidateHeaderFailsOnDifficulty() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2, proposerNodeKey, validators, parentHeader, Optional.empty())
            .difficulty(Difficulty.of(5))
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isFalse();
  }

  @Test
  public void bftValidateHeaderFailsOnAncestor() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isFalse();
  }

  @Test
  public void bftValidateHeaderFailsOnGasUsage() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2, proposerNodeKey, validators, parentHeader, Optional.empty())
            .gasLimit(5_000)
            .gasUsed(6_000)
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isFalse();
  }

  @Test
  public void bftValidateHeaderFailsOnGasLimitRange() {
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2, proposerNodeKey, validators, parentHeader, Optional.empty())
            .gasLimit(4999)
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isFalse();
  }

  @Test
  public void bftValidateHeaderFailsOnGasLimitDeltaByDefault() {
    // Parent gas limit is the preset default (5000); the child jumps to 6000, far beyond the
    // permitted delta of parentGasLimit/1024. The gas-limit change is applied via the header
    // modifier so the commit seals are computed over the final header and remain valid, leaving
    // the gas-limit delta rule as the only failing rule.
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2,
                proposerNodeKey,
                validators,
                parentHeader,
                builder -> builder.gasLimit(6_000),
                Optional.empty())
            .buildHeader();

    final BlockHeaderValidator validator = getBlockHeaderValidator();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isFalse();
  }

  @Test
  public void bftValidateHeaderPassesOnGasLimitDeltaWhenGoQuorumCompatible() {
    // The same header that fails the gas-limit delta rule by default validates when goQuorum
    // compatibility mode is enabled, because that rule is not applied.
    final NodeKey proposerNodeKey = NodeKeyUtils.generate();
    final Address proposerAddress = Util.publicKeyToAddress(proposerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(proposerAddress);

    final BlockHeader parentHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                1, proposerNodeKey, validators, null, Optional.empty())
            .buildHeader();
    final BlockHeader blockHeader =
        QbftBlockHeaderUtils.createPresetHeaderBuilder(
                2,
                proposerNodeKey,
                validators,
                parentHeader,
                builder -> builder.gasLimit(6_000),
                Optional.empty())
            .buildHeader();

    final BlockHeaderValidator validator =
        QbftBlockHeaderValidationRulesetFactory.blockHeaderValidator(
                Duration.ofSeconds(5), false, true, Optional.empty())
            .build();

    assertThat(
            validator.validateHeader(
                blockHeader, parentHeader, protocolContext(validators), HeaderValidationMode.FULL))
        .isTrue();
  }

  public BlockHeaderValidator getBlockHeaderValidator() {
    return QbftBlockHeaderValidationRulesetFactory.blockHeaderValidator(
            Duration.ofSeconds(5), false, false, Optional.empty())
        .build();
  }
}
