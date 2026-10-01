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
package org.hyperledger.besu.consensus.common.bft.queries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.NonBesuBlockHeader;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.plugin.services.query.BftQueryService;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BftQueryServiceImplTest {

  @Mock private Blockchain blockchain;

  @Mock private BftExtraDataCodec bftExtraDataCodec;

  @Mock private BftBlockInterface bftBlockInterface;

  @Mock private ValidatorProvider validatorProvider;

  private final List<NodeKey> validatorKeys =
      Lists.newArrayList(NodeKeyUtils.generate(), NodeKeyUtils.generate());

  private final List<NodeKey> signingKeys = Lists.newArrayList(validatorKeys.get(0));

  private BlockHeader blockHeader;

  @BeforeEach
  public void setup() {
    final BlockHeaderTestFixture blockHeaderTestFixture = new BlockHeaderTestFixture();
    blockHeaderTestFixture.number(1); // can't be genesis block (due to extradata serialisation)
    blockHeaderTestFixture.blockHeaderFunctions(
        BftBlockHeaderFunctions.forOnchainBlock(bftExtraDataCodec));

    blockHeader = blockHeaderTestFixture.buildHeader();
  }

  @Test
  public void roundNumberFromBlockIsReturned() {
    final BftQueryService service =
        new BftQueryServiceImpl(bftBlockInterface, blockchain, validatorProvider, null, null);
    final int roundNumberInBlock = 5;
    final BftExtraData extraData =
        new BftExtraData(Bytes.EMPTY, List.of(), Optional.empty(), roundNumberInBlock, List.of());
    when(bftBlockInterface.getExtraData(blockHeader)).thenReturn(extraData);

    assertThat(service.getRoundNumberFrom(blockHeader)).isEqualTo(roundNumberInBlock);
  }

  @Test
  public void getRoundNumberThrowsIfBlockIsNotOnTheChain() {
    final NonBesuBlockHeader header = new NonBesuBlockHeader(Hash.EMPTY, Bytes.EMPTY);

    final BftQueryService service =
        new BftQueryServiceImpl(
            new BftBlockInterface(bftExtraDataCodec), blockchain, validatorProvider, null, null);
    assertThatExceptionOfType(NoSuchElementException.class)
        .isThrownBy(() -> service.getRoundNumberFrom(header));
  }

  @Test
  public void getSignersReturnsAddressesOfSignersInBlock() {
    final BftQueryService service =
        new BftQueryServiceImpl(bftBlockInterface, blockchain, validatorProvider, null, null);

    final List<Address> signers =
        signingKeys.stream()
            .map(nodeKey -> Util.publicKeyToAddress(nodeKey.getPublicKey()))
            .collect(Collectors.toList());
    when(bftBlockInterface.getCommitters(any())).thenReturn(signers);

    assertThat(service.getSignersFrom(blockHeader)).containsExactlyElementsOf(signers);
  }

  @Test
  public void getSignersThrowsIfBlockIsNotOnTheChain() {
    final NonBesuBlockHeader header = new NonBesuBlockHeader(Hash.EMPTY, Bytes.EMPTY);

    final BftQueryService service =
        new BftQueryServiceImpl(bftBlockInterface, blockchain, validatorProvider, null, null);
    assertThatExceptionOfType(NoSuchElementException.class)
        .isThrownBy(() -> service.getSignersFrom(header));
  }

  @Test
  public void consensusMechanismNameReturnedIsSameAsThatPassedDuringCreation() {
    final BftQueryService service =
        new BftQueryServiceImpl(
            new BftBlockInterface(bftExtraDataCodec),
            blockchain,
            validatorProvider,
            null,
            "consensusMechanism");
    assertThat(service.getConsensusMechanismName()).isEqualTo("consensusMechanism");
  }

  @Test
  public void getValidatorsReturnsAddresses() {
    final BftQueryService service =
        new BftQueryServiceImpl(bftBlockInterface, blockchain, validatorProvider, null, null);

    final List<Address> validators =
        signingKeys.stream()
            .map(nodeKey -> Util.publicKeyToAddress(nodeKey.getPublicKey()))
            .collect(Collectors.toList());
    when(validatorProvider.getValidatorsAtHead()).thenReturn(validators);

    assertThat(service.getValidatorsForLatestBlock()).containsExactlyElementsOf(validators);
  }
}
