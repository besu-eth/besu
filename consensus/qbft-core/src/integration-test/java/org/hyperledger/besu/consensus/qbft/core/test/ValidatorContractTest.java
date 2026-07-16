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
package org.hyperledger.besu.consensus.qbft.core.test;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.BftFork;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.config.QbftFork;
import org.hyperledger.besu.config.QbftFork.VALIDATOR_SELECTION_MODE;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.events.BlockTimerExpiry;
import org.hyperledger.besu.consensus.common.bft.inttest.NodeParams;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.adaptor.AdaptorUtil;
import org.hyperledger.besu.consensus.qbft.core.support.RoundSpecificPeers;
import org.hyperledger.besu.consensus.qbft.core.support.TestContext;
import org.hyperledger.besu.consensus.qbft.core.support.TestContextBuilder;
import org.hyperledger.besu.consensus.qbft.core.support.ValidatorPeer;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.types.QbftNewChainHead;
import org.hyperledger.besu.consensus.qbft.core.types.QbftValidatorProvider;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.testutil.TestClock;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.io.Resources;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ValidatorContractTest {

  public static final Address NODE_ADDRESS =
      Address.fromHexString("0xeac51e3fe1afc9894f0dfeab8ceb471899b932df");
  public static final Bytes32 NODE_PRIVATE_KEY =
      Bytes32.fromHexString("0xa3bdf521b0f286a80918c4b67000dfd2a2bdef97e94d268016ef9ec86648eac3");

  private static final Address NODE_2_ADDRESS =
      Address.fromHexString("0xe98d92560fac3069ccff53ef348ded26a51d4b68");
  private static final Bytes32 NODE_2_PRIVATE_KEY =
      Bytes32.fromHexString("0xa3bdf521b0f286a80918c4b67000dfd2a2bdef97e94d268016ef9ec86648eac4");

  final Address NEW_VALIDATOR_CONTRACT_ADDRESS =
      Address.fromHexString("0x0000000000000000000000000000000000009999");

  private TestClock clock;
  private final QbftExtraDataCodec qbftExtraDataCodec = new QbftExtraDataCodec();

  @BeforeEach
  public void setup() {
    clock = new TestClock(Instant.EPOCH.plus(TestContextBuilder.BLOCK_TIMER_SEC, SECONDS));
  }

  @Test
  public void retrievesValidatorsFromValidatorContract() {
    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(0)
            .nodeParams(
                List.of(new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY))))
            .clock(TestClock.fixed())
            .genesisFile(Resources.getResource("genesis_validator_contract.json").getFile())
            .useValidatorContract(true)
            .buildAndStart();

    createNewBlockAsProposer(context, 1);

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);
    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).containsExactly(NODE_ADDRESS);
    assertThat(validatorProvider.getValidatorsForBlock(block1)).containsExactly(NODE_ADDRESS);
  }

  @Test
  public void retrievesValidatorsFromValidatorContract_LondonFork() {
    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(0)
            .nodeParams(
                List.of(new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY))))
            .clock(TestClock.fixed())
            .genesisFile(Resources.getResource("genesis_validator_contract_london.json").getFile())
            .useValidatorContract(true)
            .useLondonMilestone(true)
            .buildAndStart();

    createNewBlockAsProposer(context, 1);

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);
    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).containsExactly(NODE_ADDRESS);
    assertThat(validatorProvider.getValidatorsForBlock(block1)).containsExactly(NODE_ADDRESS);
  }

  @Test
  public void retrievesValidatorsFromValidatorContract_LondonFork_ZeroBaseFee() {
    // Using London on a free gas network
    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(0)
            .nodeParams(
                List.of(new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY))))
            .clock(TestClock.fixed())
            .genesisFile(Resources.getResource("genesis_validator_contract_london.json").getFile())
            .useValidatorContract(true)
            .useLondonMilestone(true)
            .useZeroBaseFee(true)
            .buildAndStart();

    createNewBlockAsProposer(context, 1);

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);
    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).containsExactly(NODE_ADDRESS);
    assertThat(validatorProvider.getValidatorsForBlock(block1)).containsExactly(NODE_ADDRESS);
  }

  @Test
  public void retrievesValidatorsFromValidatorContract_ShanghaiFork_ZeroBaseFee() {
    // Using Shanghai on a free gas network
    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(0)
            .nodeParams(
                List.of(new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY))))
            .clock(Clock.fixed(Instant.ofEpochSecond(1_672_531_210L), ZoneId.systemDefault()))
            .genesisFile(
                Resources.getResource("genesis_validator_contract_shanghai.json").getFile())
            .useValidatorContract(true)
            .useShanghaiMilestone(true)
            .useZeroBaseFee(true)
            .buildAndStart();

    createNewBlockAsProposerFixedTime(
        context, 1, 1_672_531_210L); // 10s ahead of genesis timestamp in
    // genesis_validator_contract_shanghai.json

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);
    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).containsExactly(NODE_ADDRESS);
    assertThat(validatorProvider.getValidatorsForBlock(block1)).containsExactly(NODE_ADDRESS);
  }

  @Test
  public void retrievesValidatorsFromValidatorContract_LondonFork_FixedBaseFee() {
    // Using London on a free gas network
    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(0)
            .nodeParams(
                List.of(new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY))))
            .clock(TestClock.fixed())
            .genesisFile(Resources.getResource("genesis_validator_contract_london.json").getFile())
            .useValidatorContract(true)
            .useLondonMilestone(true)
            .useFixedBaseFee(true)
            .buildAndStart();

    createNewBlockAsProposer(context, 1);

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);
    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).containsExactly(NODE_ADDRESS);
    assertThat(validatorProvider.getValidatorsForBlock(block1)).containsExactly(NODE_ADDRESS);
  }

  @Test
  public void retrievesValidatorsFromValidatorContract_ShanghaiFork_FixedBaseFee() {
    // Using Shanghai on a free gas network
    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(0)
            .nodeParams(
                List.of(new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY))))
            .clock(Clock.fixed(Instant.ofEpochSecond(1_672_531_210L), ZoneId.systemDefault()))
            .genesisFile(
                Resources.getResource("genesis_validator_contract_shanghai.json").getFile())
            .useValidatorContract(true)
            .useShanghaiMilestone(true)
            .useFixedBaseFee(true)
            .buildAndStart();

    createNewBlockAsProposerFixedTime(
        context, 1, 1_672_531_210L); // 10s ahead of genesis timestamp in
    // genesis_validator_contract_shanghai.json

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);
    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).containsExactly(NODE_ADDRESS);
    assertThat(validatorProvider.getValidatorsForBlock(block1)).containsExactly(NODE_ADDRESS);
  }

  @Test
  public void transitionsFromBlockHeaderModeToValidatorContractMode() {
    final List<QbftFork> qbftForks =
        List.of(createContractFork(1, TestContextBuilder.VALIDATOR_CONTRACT_ADDRESS));
    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(1)
            .nodeParams(
                List.of(
                    new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY)),
                    new NodeParams(NODE_2_ADDRESS, NodeKeyUtils.createFrom(NODE_2_PRIVATE_KEY))))
            .clock(TestClock.fixed())
            .genesisFile(
                Resources.getResource("genesis_migrating_validator_contract.json").getFile())
            .useValidatorContract(false)
            .qbftForks(qbftForks)
            .buildAndStart();

    // block 0 uses block header voting with 1 validator
    // block 1 onwards uses validator contract with 2 validators
    final List<Address> block0Addresses = List.of(NODE_ADDRESS);
    final List<Address> block1Addresses =
        Stream.of(NODE_ADDRESS, NODE_2_ADDRESS).sorted().collect(Collectors.toList());

    remotePeerProposesNewBlock(context, 1L);

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);

    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).isEqualTo(block0Addresses);
    assertThat(getExtraData(genesisBlock).getValidators()).containsExactly(NODE_ADDRESS);

    // contract block extra data cannot contain validators or vote
    assertThat(validatorProvider.getValidatorsForBlock(block1)).isEqualTo(block1Addresses);
    assertThat(getExtraData(block1).getValidators()).isEmpty();
    assertThat(getExtraData(block1).getVote()).isEmpty();
  }

  @Test
  public void transitionsFromValidatorContractModeToValidatorContractMode() {
    final List<QbftFork> qbftForks = List.of(createContractFork(1, NEW_VALIDATOR_CONTRACT_ADDRESS));

    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(1)
            .nodeParams(
                List.of(
                    new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY)),
                    new NodeParams(NODE_2_ADDRESS, NodeKeyUtils.createFrom(NODE_2_PRIVATE_KEY))))
            .clock(clock)
            .genesisFile(Resources.getResource("genesis_validator_contract.json").getFile())
            .useValidatorContract(true)
            .qbftForks(qbftForks)
            .buildAndStart();

    // block 0 uses validator contract with 1 validator
    // block 1 uses validator contract with 2 validators
    final List<Address> block0Addresses = List.of(NODE_ADDRESS);
    final List<Address> block1Addresses =
        Stream.of(NODE_ADDRESS, NODE_2_ADDRESS).sorted().collect(Collectors.toList());

    remotePeerProposesNewBlock(context, 1L);

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);

    // contract block extra data cannot contain validators or vote
    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).isEqualTo(block0Addresses);
    assertThat(getExtraData(genesisBlock).getValidators()).isEmpty();
    assertThat(getExtraData(genesisBlock).getVote()).isEmpty();

    // contract block extra data cannot contain validators or vote
    assertThat(validatorProvider.getValidatorsForBlock(block1)).isEqualTo(block1Addresses);
    assertThat(getExtraData(block1).getValidators()).isEmpty();
    assertThat(getExtraData(block1).getVote()).isEmpty();
  }

  @Test
  public void transitionsFromValidatorContractModeToBlockHeaderMode() {
    final List<QbftFork> qbftForks =
        List.of(createBlockHeaderFork(1, List.of(NODE_ADDRESS, NODE_2_ADDRESS)));
    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(0)
            .nodeParams(
                List.of(
                    new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY)),
                    new NodeParams(NODE_2_ADDRESS, NodeKeyUtils.createFrom(NODE_2_PRIVATE_KEY))))
            .clock(clock)
            .genesisFile(Resources.getResource("genesis_validator_contract.json").getFile())
            .useValidatorContract(true)
            .qbftForks(qbftForks)
            .buildAndStart();

    // block 0 uses validator contract with 1 validator
    // block 1 onwards uses block header voting with 2 overridden validators
    // block 2 uses block header voting with the cached validators
    final List<Address> block0Addresses = Stream.of(NODE_ADDRESS).collect(Collectors.toList());
    final List<Address> block1Addresses =
        Stream.of(NODE_ADDRESS, NODE_2_ADDRESS).sorted().collect(Collectors.toList());

    createNewBlockAsProposer(context, 1);
    clock.step(TestContextBuilder.BLOCK_TIMER_SEC, SECONDS);
    remotePeerProposesNewBlock(context, 2L);

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);
    final QbftBlockHeader block2 = context.getBlockHeader(2);

    // contract block extra data cannot contain validators or vote
    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).isEqualTo(block0Addresses);
    assertThat(getExtraData(genesisBlock).getValidators()).isEmpty();
    assertThat(getExtraData(genesisBlock).getVote()).isEmpty();

    // uses overridden validators
    assertThat(validatorProvider.getValidatorsForBlock(block1)).isEqualTo(block1Addresses);
    assertThat(getExtraData(block1).getValidators()).containsExactly(NODE_2_ADDRESS, NODE_ADDRESS);

    // uses cached validators
    assertThat(validatorProvider.getValidatorsForBlock(block2)).isEqualTo(block1Addresses);
    assertThat(getExtraData(block2).getValidators()).containsExactly(NODE_2_ADDRESS, NODE_ADDRESS);
  }

  @Test
  public void transitionsFromBlockHeaderModeToValidatorContractModeThenBack() {
    final List<QbftFork> qbftForks =
        List.of(
            createContractFork(1, TestContextBuilder.VALIDATOR_CONTRACT_ADDRESS),
            createBlockHeaderFork(2, List.of(NODE_ADDRESS, NODE_2_ADDRESS)));

    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(1)
            .nodeParams(
                List.of(
                    new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY)),
                    new NodeParams(NODE_2_ADDRESS, NodeKeyUtils.createFrom(NODE_2_PRIVATE_KEY))))
            .clock(clock)
            .genesisFile(
                Resources.getResource("genesis_migrating_validator_contract.json").getFile())
            .useValidatorContract(false)
            .qbftForks(qbftForks)
            .buildAndStart();

    // block 0 uses block header voting with 1 validator
    // block 1 uses validator contract with 2 validators
    // block 2 uses block header voting with 2 overridden validators
    // block 3 uses block header voting with cached validators
    final List<Address> block0Addresses = List.of(NODE_ADDRESS);
    final List<Address> block1Addresses =
        Stream.of(NODE_ADDRESS, NODE_2_ADDRESS).sorted().collect(Collectors.toList());

    remotePeerProposesNewBlock(context, 1L);
    clock.step(TestContextBuilder.BLOCK_TIMER_SEC, SECONDS);
    createNewBlockAsProposer(context, 2L);
    clock.step(TestContextBuilder.BLOCK_TIMER_SEC, SECONDS);
    remotePeerProposesNewBlock(context, 3L);

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);
    final QbftBlockHeader block2 = context.getBlockHeader(2);
    final QbftBlockHeader block3 = context.getBlockHeader(3);

    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).isEqualTo(block0Addresses);
    assertThat(getExtraData(genesisBlock).getValidators()).containsExactly(NODE_ADDRESS);

    // contract block extra data cannot contain validators or vote
    assertThat(validatorProvider.getValidatorsForBlock(block1)).isEqualTo(block1Addresses);
    assertThat(getExtraData(block1).getValidators()).isEmpty();
    assertThat(getExtraData(block1).getVote()).isEmpty();

    // uses overridden validators
    assertThat(validatorProvider.getValidatorsForBlock(block2)).isEqualTo(block1Addresses);
    assertThat(getExtraData(block2).getValidators()).containsExactly(NODE_2_ADDRESS, NODE_ADDRESS);

    // uses cached validators
    assertThat(validatorProvider.getValidatorsForBlock(block3)).isEqualTo(block1Addresses);
    assertThat(getExtraData(block3).getValidators()).containsExactly(NODE_2_ADDRESS, NODE_ADDRESS);
  }

  @Test
  public void transitionsFromValidatorContractModeToBlockHeaderModeThenBackToNewContract() {
    final List<QbftFork> qbftForks =
        List.of(
            createBlockHeaderFork(1, List.of(NODE_2_ADDRESS)),
            createContractFork(2, NEW_VALIDATOR_CONTRACT_ADDRESS));

    final TestContext context =
        new TestContextBuilder()
            .indexOfFirstLocallyProposedBlock(0)
            .nodeParams(
                List.of(
                    new NodeParams(NODE_ADDRESS, NodeKeyUtils.createFrom(NODE_PRIVATE_KEY)),
                    new NodeParams(NODE_2_ADDRESS, NodeKeyUtils.createFrom(NODE_2_PRIVATE_KEY))))
            .clock(clock)
            .genesisFile(Resources.getResource("genesis_validator_contract.json").getFile())
            .useValidatorContract(true)
            .qbftForks(qbftForks)
            .buildAndStart();

    // block 0 uses validator contract with 1 validator
    // block 1 uses block header voting with 1 overridden validator
    // block 2 uses validator contract with 2 validators
    final List<Address> block0Addresses = List.of(NODE_ADDRESS);
    final List<Address> block1Addresses = List.of(NODE_2_ADDRESS);
    final List<Address> block2Addresses =
        Stream.of(NODE_ADDRESS, NODE_2_ADDRESS).sorted().collect(Collectors.toList());

    createNewBlockAsProposer(context, 1L);
    clock.step(TestContextBuilder.BLOCK_TIMER_SEC, SECONDS);
    remotePeerProposesNewBlock(context, 2L);

    final QbftValidatorProvider validatorProvider = context.getValidatorProvider();
    final QbftBlockHeader genesisBlock = context.getBlockHeader(0);
    final QbftBlockHeader block1 = context.getBlockHeader(1);
    final QbftBlockHeader block2 = context.getBlockHeader(2);

    // contract block extra data cannot contain validators or vote
    assertThat(validatorProvider.getValidatorsForBlock(genesisBlock)).isEqualTo(block0Addresses);
    assertThat(getExtraData(genesisBlock).getValidators()).isEmpty();
    assertThat(getExtraData(genesisBlock).getVote()).isEmpty();

    assertThat(validatorProvider.getValidatorsForBlock(block1)).isEqualTo(block1Addresses);
    assertThat(getExtraData(block1).getValidators()).containsExactly(NODE_2_ADDRESS);

    // contract block extra data cannot contain validators or vote
    assertThat(validatorProvider.getValidatorsForBlock(block2)).isEqualTo(block2Addresses);
    assertThat(getExtraData(block2).getValidators()).isEmpty();
    assertThat(getExtraData(block2).getVote()).isEmpty();
  }

  private void createNewBlockAsProposer(final TestContext context, final long blockNumber) {
    ConsensusRoundIdentifier roundId = new ConsensusRoundIdentifier(blockNumber, 0);

    // trigger proposal
    context.getController().handleBlockTimerExpiry(new BlockTimerExpiry(roundId));

    // peers commit proposed block
    QbftBlock proposedBlock =
        context.createBlockForProposalFromChainHead(clock.instant().getEpochSecond());
    RoundSpecificPeers peers = context.roundSpecificPeers(roundId);
    peers.commitForNonProposing(roundId, proposedBlock);

    assertThat(context.getCurrentChainHeight()).isEqualTo(blockNumber);
    context
        .getController()
        .handleNewBlockEvent(new QbftNewChainHead(context.getBlockchain().getChainHeadHeader()));
  }

  private void createNewBlockAsProposerFixedTime(
      final TestContext context, final long blockNumber, final long timestamp) {
    ConsensusRoundIdentifier roundId = new ConsensusRoundIdentifier(blockNumber, 0);

    // trigger proposal
    context.getController().handleBlockTimerExpiry(new BlockTimerExpiry(roundId));

    // peers commit proposed block
    QbftBlock proposedBlock = context.createBlockForProposalFromChainHead(timestamp);
    RoundSpecificPeers peers = context.roundSpecificPeers(roundId);
    peers.commitForNonProposing(roundId, proposedBlock);

    assertThat(context.getCurrentChainHeight()).isEqualTo(blockNumber);
    context
        .getController()
        .handleNewBlockEvent(new QbftNewChainHead(context.getBlockchain().getChainHeadHeader()));
  }

  private void remotePeerProposesNewBlock(final TestContext context, final long blockNumber) {
    ConsensusRoundIdentifier roundId = new ConsensusRoundIdentifier(blockNumber, 0);

    RoundSpecificPeers peers = context.roundSpecificPeers(roundId);
    ValidatorPeer remoteProposer = peers.getProposer();
    final QbftBlock blockToPropose =
        context.createBlockForProposalFromChainHead(
            clock.instant().getEpochSecond(), remoteProposer.getNodeAddress());
    remoteProposer.injectProposal(roundId, blockToPropose);
    remoteProposer.injectCommit(roundId, blockToPropose);
    assertThat(context.getCurrentChainHeight()).isEqualTo(blockNumber);

    context
        .getController()
        .handleNewBlockEvent(new QbftNewChainHead(context.getBlockchain().getChainHeadHeader()));
  }

  private QbftFork createContractFork(final long block, final Address contractAddress) {
    return new QbftFork(
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                block,
                QbftFork.VALIDATOR_SELECTION_MODE_KEY,
                VALIDATOR_SELECTION_MODE.CONTRACT,
                QbftFork.VALIDATOR_CONTRACT_ADDRESS_KEY,
                contractAddress.getBytes().toHexString())));
  }

  private QbftFork createBlockHeaderFork(final long block, final List<Address> validators) {

    final List<TextNode> jsonValidators =
        validators.stream()
            .map(v -> TextNode.valueOf(v.getBytes().toHexString()))
            .collect(Collectors.toList());

    return new QbftFork(
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                block,
                QbftFork.VALIDATOR_SELECTION_MODE_KEY,
                VALIDATOR_SELECTION_MODE.BLOCKHEADER,
                BftFork.VALIDATORS_KEY,
                JsonUtil.getObjectMapper().createArrayNode().addAll(jsonValidators))));
  }

  private BftExtraData getExtraData(final QbftBlockHeader blockHeader) {
    return qbftExtraDataCodec.decode(AdaptorUtil.toBesuBlockHeader(blockHeader));
  }
}
