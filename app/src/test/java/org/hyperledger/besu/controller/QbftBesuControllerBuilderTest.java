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
package org.hyperledger.besu.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.JsonQbftConfigOptions;
import org.hyperledger.besu.config.TransitionsConfigOptions;
import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.consensus.qbft.MutableQbftConfigOptions;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.validator.ForkingValidatorProvider;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.DefaultSynchronizer;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.List;
import java.util.function.Predicate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class QbftBesuControllerBuilderTest extends AbstractBftBesuControllerBuilderTest {

  @Captor private ArgumentCaptor<Predicate<Hash>> ownedByConsensus;

  @Override
  public void setupBftGenesisConfig() throws JsonProcessingException {

    // qbft prepForBuild setup
    lenient()
        .when(genesisConfigOptions.getQbftConfigOptions())
        .thenReturn(new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT));

    final var jsonTransitions =
        (ObjectNode)
            objectMapper.readTree(
                """
                                {"qbft": [
                                  {
                                            "block": 2,
                                            "blockperiodseconds": 2
                                  }
                                ]}
                                """);

    lenient()
        .when(genesisConfigOptions.getTransitions())
        .thenReturn(new TransitionsConfigOptions(jsonTransitions));

    lenient()
        .when(genesisConfig.getExtraData())
        .thenReturn(
            QbftExtraDataCodec.createGenesisExtraDataString(List.of(Address.fromHexString("1"))));
  }

  @Override
  protected BesuControllerBuilder createBftControllerBuilder() {
    // spied to check how build() configures the synchronizer
    return spy(new QbftBesuControllerBuilder());
  }

  @Override
  protected BlockHeaderFunctions getBlockHeaderFunctions() {
    return BftBlockHeaderFunctions.forOnchainBlock(new QbftExtraDataCodec());
  }

  @Test
  public void forkingValidatorProviderIsAvailableOnBftContext() {
    final BesuController besuController = bftBesuControllerBuilder.build();

    final ValidatorProvider validatorProvider =
        besuController
            .getProtocolContext()
            .getConsensusContext(BftContext.class)
            .getValidatorProvider();
    assertThat(validatorProvider).isInstanceOf(ForkingValidatorProvider.class);
  }

  @Test
  public void missingTransactionValidatorProviderThrowsError() {
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
    when(protocolContext.getBlockchain()).thenReturn(mock(MutableBlockchain.class));

    assertThatThrownBy(
            () ->
                bftBesuControllerBuilder.createAdditionalJsonRpcMethodFactory(
                    protocolContext, protocolSchedule, MiningConfiguration.newDefault()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("transactionValidatorProvider should have been initialised");
  }

  @Test
  public void buildConfiguresTheSynchronizer() {
    bftBesuControllerBuilder.build();

    verify(bftBesuControllerBuilder).configureSynchronizer(any());
  }

  @Test
  public void synchronizerLeavesBlocksClaimedByTheRoundToConsensus() {
    final QbftBesuControllerBuilder builder = new QbftBesuControllerBuilder();
    final DefaultSynchronizer synchronizer = mock(DefaultSynchronizer.class);
    final Hash claimed = Hash.hash(Bytes.of(1));

    builder.configureSynchronizer(synchronizer);

    // backed by the cache shared with the QBFT validator and importer adaptors
    verify(synchronizer).setConsensusOwnedBlockPredicate(ownedByConsensus.capture());
    builder.getValidatedBlockCache().claim(claimed, claimed, 1L);
    assertThat(ownedByConsensus.getValue().test(claimed)).isTrue();

    builder.getValidatedBlockCache().release(claimed, claimed);
    assertThat(ownedByConsensus.getValue().test(claimed)).isFalse();
    verify(synchronizer).onConsensusOwnershipReleased(claimed);
  }
}
