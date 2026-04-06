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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor.TransactionReceiptFactory;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.parallelization.MainnetParallelBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelTransactionPreprocessing;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BalStateRootCalculator;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Uint256;

@SuppressWarnings("rawtypes")
class AbstractBlockProcessorIntegrationTest {

  private static final String ACCOUNT_GENESIS_1 = "0x627306090abab3a6e1400e9345bc60c78a8bef57";
  private static final String ACCOUNT_GENESIS_2 = "0x7f2d653f56ea8de6ffa554c7a0cd4e03af79f3eb";

  private static final String ACCOUNT_2 = "0x0000000000000000000000000000000000000002";
  private static final String ACCOUNT_3 = "0x0000000000000000000000000000000000000003";
  private static final String ACCOUNT_4 = "0x0000000000000000000000000000000000000004";
  private static final String ACCOUNT_5 = "0x0000000000000000000000000000000000000005";
  private static final String ACCOUNT_6 = "0x0000000000000000000000000000000000000006";
  private static final String CONTRACT_ADDRESS = "0x00000000000000000000000000000000000fffff";

  private static final Address WITHDRAWAL_CONTRACT =
      Address.fromHexString("0x00a3ca265ebcb825b45f985a16cefb49958ce017");
  private static final Address CONSOLIDATION_CONTRACT =
      Address.fromHexString("0x00b42dbf2194e931e80326d950320f7d9dbeac02");

  private static final KeyPair ACCOUNT_GENESIS_1_KEYPAIR =
      generateKeyPair("c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3");

  private static final KeyPair ACCOUNT_GENESIS_2_KEYPAIR =
      generateKeyPair("fc5141e75bf622179f8eedada7fab3e2e6b3e3da8eb9df4f46d84df22df7430e");

  private static final Wei COINBASE_REWARD = Wei.of(2_000_000_000_000_000L);

  private ProtocolContext protocolContext;
  private WorldStateArchive worldStateArchive;
  private DefaultBlockchain blockchain;
  private Address coinbase;

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/mainnet/genesis-bp-it.json";

  @BeforeEach
  public void setUp() {
    final ExecutionContextTestFixture contextTestFixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    final BlockHeader blockHeader = new BlockHeaderTestFixture().number(0L).buildHeader();
    coinbase = blockHeader.getCoinbase();
    worldStateArchive = contextTestFixture.getStateArchive();
    protocolContext = contextTestFixture.getProtocolContext();
    blockchain = (DefaultBlockchain) contextTestFixture.getBlockchain();
  }

  private static Stream<Arguments> blockProcessors(
      final Wei coinbaseReward, final boolean skipRewards) {
    final ExecutionContextTestFixture contextTestFixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();

    final ProtocolSchedule protocolSchedule = contextTestFixture.getProtocolSchedule();
    final var protocolSpec =
        protocolSchedule.getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());

    final MainnetTransactionProcessor transactionProcessor = protocolSpec.getTransactionProcessor();
    final var receiptFactory = protocolSpec.getTransactionReceiptFactory();

    final BlockProcessor sequentialBlockProcessor =
        new MainnetBlockProcessor(
            transactionProcessor,
            receiptFactory,
            coinbaseReward,
            BlockHeader::getCoinbase,
            skipRewards,
            protocolSchedule,
            BalConfiguration.DEFAULT);

    final BlockProcessor parallelBlockProcessor =
        new MainnetParallelBlockProcessor(
            transactionProcessor,
            receiptFactory,
            coinbaseReward,
            BlockHeader::getCoinbase,
            skipRewards,
            protocolSchedule,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());

    return Stream.of(
        Arguments.of("sequential", sequentialBlockProcessor),
        Arguments.of("parallel", parallelBlockProcessor));
  }

  private static Stream<Arguments> blockProcessorProvider() {
    return blockProcessors(COINBASE_REWARD, false);
  }

  private static Stream<Arguments> blockProcessorProviderWithoutRewards() {
    return blockProcessors(Wei.ZERO, true);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProvider")
  void testBlockProcessingWithTransfers(
      final String ignoredName, final BlockProcessor blockProcessor) {
    processSimpleTransfers(blockProcessor);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProvider")
  void testProcessConflictedSimpleTransfersSameSender(
      final String ignoredName, final BlockProcessor blockProcessor) {
    processConflictedSimpleTransfersSameSender(blockProcessor);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProvider")
  void testProcessConflictedSimpleTransfersSameAddressReceiverAndSender(
      final String ignoredName, final BlockProcessor blockProcessor) {
    processConflictedSimpleTransfersSameAddressReceiverAndSender(blockProcessor);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProvider")
  void testProcessConflictedSimpleTransfersWithCoinbase(
      final String ignoredName, final BlockProcessor blockProcessor) {
    processConflictedSimpleTransfersWithCoinbase(blockProcessor);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProvider")
  void testProcessContractSlotUpdateThenReadTx(
      final String ignoredName, final BlockProcessor blockProcessor) {
    processContractSlotUpdateThenReadTx(blockProcessor);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProvider")
  void testProcessSlotReadThenUpdateTx(
      final String ignoredName, final BlockProcessor blockProcessor) {
    processSlotReadThenUpdateTx(blockProcessor);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProviderWithoutRewards")
  void blockAccessListStateRootMatchesAccumulatorForSimpleTransfers(
      final String ignoredName, final BlockProcessor blockProcessor) {
    Transaction transactionTransfer1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300000L, 0L, 5L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    Transaction transactionTransfer2 =
        createTransferTransaction(
            0, 2_000_000_000_000_000_000L, 300000L, 0L, 5L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);

    MutableWorldState worldState = worldStateArchive.getWorldState();
    Block blockWithTransactions =
        createBlockWithTransactions(
            "0x3e09cb932146fa6c57c1d41b444805a9547c7cc380505a0ee47ef8f561e9aeed",
            Wei.of(5),
            transactionTransfer1,
            transactionTransfer2);

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    assertTrue(blockProcessingResult.isSuccessful());
    assertBalComputesHeaderRoot(blockWithTransactions, blockProcessingResult);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProviderWithoutRewards")
  void blockAccessListStateRootMatchesAccumulatorForStorageAndReads(
      final String ignoredName, final BlockProcessor blockProcessor) {
    Address contractAddress = Address.fromHexStringStrict(CONTRACT_ADDRESS);

    Transaction setSlot1Transaction =
        createContractUpdateSlotTransaction(
            0, contractAddress, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(100));
    Transaction getSlot1Transaction =
        createContractUpdateSlotTransaction(
            0, contractAddress, "getSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty());
    Transaction setSlot3Transaction =
        createContractUpdateSlotTransaction(
            1, contractAddress, "setSlot2", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(200));
    Transaction setSlot4Transaction =
        createContractUpdateSlotTransaction(
            2, contractAddress, "setSlot3", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(300));

    MutableWorldState worldState = worldStateArchive.getWorldState();
    Block blockWithTransactions =
        createBlockWithTransactions(
            "0x811a7edeb2747665e5e5937310b30154386ef71cda786b46d13f38e0b3005c15",
            Wei.of(5),
            setSlot1Transaction,
            getSlot1Transaction,
            setSlot3Transaction,
            setSlot4Transaction);

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    assertTrue(blockProcessingResult.isSuccessful());
    assertBalComputesHeaderRoot(blockWithTransactions, blockProcessingResult);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProvider")
  void testProcessAccountReadThenUpdateTx(
      final String ignoredName, final BlockProcessor blockProcessor) {
    processAccountReadThenUpdateTx(blockProcessor);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProvider")
  void testProcessAccountUpdateThenReadTx(
      final String ignoredName, final BlockProcessor blockProcessor) {
    processAccountUpdateThenReadTx(blockProcessor);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProvider")
  void testProcessAccountReadThenUpdateTxWithTwoAccounts(
      final String ignoredName, final BlockProcessor blockProcessor) {
    processAccountReadThenUpdateTxWithTwoAccounts(blockProcessor);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("blockProcessorProvider")
  void testProcessAccountUpdateThenReadTeTxWithTwoAccounts(
      final String ignoredName, final BlockProcessor blockProcessor) {
    processAccountUpdateThenReadTxWithTwoAccounts(blockProcessor);
  }

  @Test
  void testProcessBlockZeroReward() {
    ExecutionContextTestFixture contextTestFixture =
        ExecutionContextTestFixture.builder(
                GenesisConfig.fromResource(
                    "/org/hyperledger/besu/ethereum/mainnet/genesis-bp-it.json"))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();

    MutableWorldState worldStateSequential = worldStateArchive.getWorldState();
    MutableWorldState worldStateParallel = contextTestFixture.getStateArchive().getWorldState();

    Transaction[] transactions = {
      createTransferTransaction(
          0, 1_000_000_000_000_000_000L, 300000L, 0L, 0L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR),
      createTransferTransaction(
          0, 2_000_000_000_000_000_000L, 300000L, 0L, 0L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR)
    };

    Block block =
        createBlockWithTransactions(
            "0xec1c11acf099066f93dbf23c57e91f5aec94ad80860874f5b54a24338fc57050",
            Wei.ZERO,
            transactions);

    ProtocolSchedule protocolSchedule = contextTestFixture.getProtocolSchedule();

    MainnetTransactionProcessor transactionProcessor =
        protocolSchedule.getByBlockHeader(block.getHeader()).getTransactionProcessor();
    TransactionReceiptFactory receiptFactory =
        protocolSchedule.getByBlockHeader(block.getHeader()).getTransactionReceiptFactory();

    MainnetBlockProcessor blockProcessor =
        new MainnetBlockProcessor(
            transactionProcessor,
            receiptFactory,
            Wei.ZERO,
            BlockHeader::getCoinbase,
            true,
            protocolSchedule,
            BalConfiguration.DEFAULT);

    BlockProcessingResult parallelResult =
        blockProcessor.processBlock(
            protocolContext,
            blockchain,
            worldStateParallel,
            block,
            new ParallelTransactionPreprocessing(
                transactionProcessor, Runnable::run, BalConfiguration.DEFAULT));

    BlockProcessingResult sequentialResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldStateSequential, block);

    assertTrue(sequentialResult.isSuccessful());
    assertTrue(parallelResult.isSuccessful());

    assertBalComputesHeaderRoot(block, sequentialResult);
    assertBalComputesHeaderRoot(block, parallelResult);

    assertThat(worldStateSequential.rootHash()).isEqualTo(worldStateParallel.rootHash());

    assertBlockAccessListAddresses(
        sequentialResult,
        coinbase,
        Address.fromHexStringStrict(ACCOUNT_2),
        Address.fromHexStringStrict(ACCOUNT_3),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT);

    assertBalanceMatchesWorldState(sequentialResult, Address.fromHexStringStrict(ACCOUNT_2));
    assertBalanceMatchesWorldState(sequentialResult, Address.fromHexStringStrict(ACCOUNT_3));
    assertBalanceMatchesWorldState(
        sequentialResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertBalanceMatchesWorldState(
        sequentialResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    // coinbase balance unchanged with zero reward

    assertNonceChange(sequentialResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1), 1L);
    assertNonceChange(sequentialResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2), 1L);

    assertBlockAccessListAddresses(
        parallelResult,
        coinbase,
        Address.fromHexStringStrict(ACCOUNT_2),
        Address.fromHexStringStrict(ACCOUNT_3),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT);

    assertBalanceMatchesWorldState(parallelResult, Address.fromHexStringStrict(ACCOUNT_2));
    assertBalanceMatchesWorldState(parallelResult, Address.fromHexStringStrict(ACCOUNT_3));
    assertBalanceMatchesWorldState(parallelResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertBalanceMatchesWorldState(parallelResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    // coinbase balance unchanged with zero reward

    assertNonceChange(parallelResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1), 1L);
    assertNonceChange(parallelResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2), 1L);
  }

  private void processSimpleTransfers(final BlockProcessor blockProcessor) {
    // Create two non conflicted transactions
    Transaction transactionTransfer1 = // ACCOUNT_GENESIS_1 -> ACCOUNT_2
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    Transaction transactionTransfer2 = // ACCOUNT_GENESIS_2 -> ACCOUNT_3
        createTransferTransaction(
            0, 2_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);

    MutableWorldState worldState = worldStateArchive.getWorldState();
    BonsaiAccount senderAccount1 = (BonsaiAccount) worldState.get(transactionTransfer1.getSender());
    BonsaiAccount senderAccount2 = (BonsaiAccount) worldState.get(transactionTransfer2.getSender());

    Block blockWithTransactions =
        createBlockWithTransactions(
            "0x1473bc3018da0e0e036ab6a11652a75753f8c06d6d63740fc89af2bc0b4d923b",
            Wei.of(5),
            transactionTransfer1,
            transactionTransfer2);

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    BonsaiAccount updatedSenderAccount1 =
        (BonsaiAccount) worldState.get(transactionTransfer1.getSender());
    BonsaiAccount updatedSenderAccount2 =
        (BonsaiAccount) worldState.get(transactionTransfer2.getSender());

    BonsaiAccount updatedAccount0x2 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_2));
    BonsaiAccount updatedAccount0x3 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_3));

    assertTrue(blockProcessingResult.isSuccessful());
    assertThat(updatedAccount0x2.getBalance()).isEqualTo(Wei.of(1_000_000_000_000_000_000L));
    assertThat(updatedAccount0x3.getBalance()).isEqualTo(Wei.of(2_000_000_000_000_000_000L));
    assertThat(updatedSenderAccount1.getBalance()).isLessThan(senderAccount1.getBalance());
    assertThat(updatedSenderAccount2.getBalance()).isLessThan(senderAccount2.getBalance());

    assertBlockAccessListAddresses(
        blockProcessingResult,
        Address.fromHexStringStrict(ACCOUNT_2),
        Address.fromHexStringStrict(ACCOUNT_3),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT,
        coinbase);

    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_2));
    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_3));
    assertBalanceMatchesWorldState(blockProcessingResult, transactionTransfer1.getSender());
    assertBalanceMatchesWorldState(blockProcessingResult, transactionTransfer2.getSender());
    assertBalanceMatchesWorldState(blockProcessingResult, coinbase);

    assertNonceChange(blockProcessingResult, transactionTransfer1.getSender(), 1L);
    assertNonceChange(blockProcessingResult, transactionTransfer2.getSender(), 1L);
  }

  private void processConflictedSimpleTransfersSameSender(final BlockProcessor blockProcessor) {
    // Create three transactions with the same sender
    Transaction transferTransaction1 = // ACCOUNT_GENESIS_1 -> ACCOUNT_4
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_4, ACCOUNT_GENESIS_1_KEYPAIR);
    Transaction transferTransaction2 = // ACCOUNT_GENESIS_1 -> ACCOUNT_5
        createTransferTransaction(
            1, 2_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_5, ACCOUNT_GENESIS_1_KEYPAIR);
    Transaction transferTransaction3 = // ACCOUNT_GENESIS_1 -> ACCOUNT_6
        createTransferTransaction(
            2, 3_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_6, ACCOUNT_GENESIS_1_KEYPAIR);

    MutableWorldState worldState = worldStateArchive.getWorldState();
    BonsaiAccount senderAccount = (BonsaiAccount) worldState.get(transferTransaction1.getSender());

    Block blockWithTransactions =
        createBlockWithTransactions(
            "0x06e86985eac65057e565f9ab44b825c4560dd1af50856a7ee3a5043fef039187",
            Wei.of(5),
            transferTransaction1,
            transferTransaction2,
            transferTransaction3);

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    BonsaiAccount updatedSenderAccount =
        (BonsaiAccount) worldState.get(transferTransaction1.getSender());

    BonsaiAccount updatedAccount0x4 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_4));
    BonsaiAccount updatedAccount0x5 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_5));
    BonsaiAccount updatedAccount0x6 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_6));

    assertTrue(blockProcessingResult.isSuccessful());
    assertThat(updatedAccount0x4.getBalance()).isEqualTo(Wei.of(1_000_000_000_000_000_000L));
    assertThat(updatedAccount0x5.getBalance()).isEqualTo(Wei.of(2_000_000_000_000_000_000L));
    assertThat(updatedAccount0x6.getBalance()).isEqualTo(Wei.of(3_000_000_000_000_000_000L));

    assertThat(updatedSenderAccount.getBalance()).isLessThan(senderAccount.getBalance());

    assertBlockAccessListAddresses(
        blockProcessingResult,
        Address.fromHexStringStrict(ACCOUNT_4),
        Address.fromHexStringStrict(ACCOUNT_5),
        Address.fromHexStringStrict(ACCOUNT_6),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT,
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        coinbase);

    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_4));
    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_5));
    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_6));
    assertBalanceMatchesWorldState(blockProcessingResult, transferTransaction1.getSender());
    assertBalanceMatchesWorldState(blockProcessingResult, coinbase);

    assertNonceChange(blockProcessingResult, transferTransaction1.getSender(), 3L);
  }

  private void processConflictedSimpleTransfersSameAddressReceiverAndSender(
      final BlockProcessor blockProcessor) {
    // Create conflicted transfer transactions
    Transaction transferTransaction1 =
        createTransferTransaction(
            0,
            1_000_000_000_000_000_000L,
            300000L,
            5L,
            7L,
            ACCOUNT_GENESIS_2,
            ACCOUNT_GENESIS_1_KEYPAIR); // ACCOUNT_GENESIS_1 -> ACCOUNT_GENESIS_2
    Transaction transferTransaction2 =
        createTransferTransaction(
            0,
            2_000_000_000_000_000_000L,
            300000L,
            5L,
            7L,
            ACCOUNT_2,
            ACCOUNT_GENESIS_2_KEYPAIR); // ACCOUNT_GENESIS_2 -> ACCOUNT_2

    MutableWorldState worldState = worldStateArchive.getWorldState();
    BonsaiAccount transferTransaction1Sender =
        (BonsaiAccount) worldState.get(transferTransaction1.getSender());

    Block blockWithTransactions =
        createBlockWithTransactions(
            "0xf2f6f0958f608eae7ed6b9b63a6711a8d37c7a9bf1efa45b9dff998fd40f4bd9",
            Wei.of(5),
            transferTransaction1,
            transferTransaction2);

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    BonsaiAccount updatedSenderAccount1 =
        (BonsaiAccount) worldState.get(transferTransaction1.getSender());
    BonsaiAccount updatedGenesisAccount1 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    BonsaiAccount updatedGenesisAccount2 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    BonsaiAccount updatedAccount0x2 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_2));

    assertTrue(blockProcessingResult.isSuccessful());
    assertThat(updatedGenesisAccount1.getBalance())
        .isEqualTo(
            Wei.of(
                UInt256.fromHexString(
                    ("0x00000000000000000000000000000000000000000000003627e8f7123739c1c8"))));
    assertThat(updatedGenesisAccount2.getBalance())
        .isEqualTo(
            Wei.of(
                UInt256.fromHexString(
                    ("0x00000000000000000000000000000000000000000000003627e8f7123739c024"))));
    assertThat(updatedAccount0x2.getBalance()).isEqualTo(Wei.of(2_000_000_000_000_000_000L));
    assertThat(updatedSenderAccount1.getBalance())
        .isLessThan(transferTransaction1Sender.getBalance());

    assertBlockAccessListAddresses(
        blockProcessingResult,
        Address.fromHexStringStrict(ACCOUNT_2),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT,
        coinbase);

    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_2));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    assertBalanceMatchesWorldState(blockProcessingResult, coinbase);

    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1), 1L);
    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2), 1L);
  }

  private void processConflictedSimpleTransfersWithCoinbase(final BlockProcessor blockProcessor) {
    // Create conflicted transactions using coinbase
    Transaction transferTransaction1 =
        createTransferTransaction(
            0,
            1_000_000_000_000_000_000L,
            300000L,
            5L,
            7L,
            ACCOUNT_2,
            ACCOUNT_GENESIS_1_KEYPAIR); // ACCOUNT_GENESIS_1 -> ACCOUNT_2
    Transaction transferTransaction2 =
        createTransferTransaction(
            0,
            2_000_000_000_000_000_000L,
            300000L,
            5L,
            7L,
            coinbase.getBytes().toHexString(),
            ACCOUNT_GENESIS_2_KEYPAIR); // ACCOUNT_GENESIS_2 -> COINBASE

    MutableWorldState worldState = worldStateArchive.getWorldState();
    BonsaiAccount transferTransaction1Sender =
        (BonsaiAccount) worldState.get(transferTransaction1.getSender());
    Block blockWithTransactions =
        createBlockWithTransactions(
            "0x1a4e7899fad518729994f78a991f8a21186007dcba653d96be8cc16027d25c00",
            Wei.of(5),
            transferTransaction1,
            transferTransaction2);

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    BonsaiAccount updatedSenderAccount1 =
        (BonsaiAccount) worldState.get(transferTransaction1.getSender());

    BonsaiAccount updatedAccount0x2 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_2));

    BonsaiAccount updatedCoinbase = (BonsaiAccount) worldState.get(coinbase);

    assertTrue(blockProcessingResult.isSuccessful());
    assertThat(updatedAccount0x2.getBalance()).isEqualTo(Wei.of(1_000_000_000_000_000_000L));
    assertThat(updatedCoinbase.getBalance())
        .isEqualTo(
            Wei.of(
                UInt256.fromHexString(
                    ("0x0000000000000000000000000000000000000000000000001bc8886498566008"))));

    assertThat(updatedSenderAccount1.getBalance())
        .isLessThan(transferTransaction1Sender.getBalance());

    assertBlockAccessListAddresses(
        blockProcessingResult,
        Address.fromHexStringStrict(ACCOUNT_2),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT,
        coinbase);

    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_2));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    assertBalanceMatchesWorldState(blockProcessingResult, coinbase);

    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1), 1L);
    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2), 1L);
  }

  void processContractSlotUpdateThenReadTx(final BlockProcessor blockProcessor) {
    Address contractAddress = Address.fromHexStringStrict(CONTRACT_ADDRESS);

    // create conflicted transactions on the same slot (update then read)
    Transaction setSlot1Transaction =
        createContractUpdateSlotTransaction(
            0, contractAddress, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(100));
    Transaction getSlot1Transaction =
        createContractUpdateSlotTransaction(
            0, contractAddress, "getSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty());
    Transaction setSlot3Transaction =
        createContractUpdateSlotTransaction(
            1, contractAddress, "setSlot2", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(200));
    Transaction setSlot4Transaction =
        createContractUpdateSlotTransaction(
            2, contractAddress, "setSlot3", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(300));

    Block blockWithTransactions =
        createBlockWithTransactions(
            "0x9e6593d2efd9e4c44345322bf7827e24e1eeb358ff56c243bb68298f26c4bc15",
            Wei.of(5),
            setSlot1Transaction,
            getSlot1Transaction,
            setSlot3Transaction,
            setSlot4Transaction);

    MutableWorldState worldState = worldStateArchive.getWorldState();
    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    assertTrue(blockProcessingResult.isSuccessful());

    // Verify the state
    assertContractStorage(worldState, contractAddress, 0, 100);
    assertContractStorage(worldState, contractAddress, 1, 200);
    assertContractStorage(worldState, contractAddress, 2, 300);

    assertBlockAccessListAddresses(
        blockProcessingResult,
        Address.fromHexStringStrict(CONTRACT_ADDRESS),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT,
        coinbase);

    // contract balance is unchanged so no balance changes recorded
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    assertBalanceMatchesWorldState(blockProcessingResult, coinbase);

    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1), 3L);
    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2), 1L);

    assertStorageWrite(blockProcessingResult, contractAddress, 0, 0, 100);
    assertStorageWrite(blockProcessingResult, contractAddress, 1, 2, 200);
    assertStorageWrite(blockProcessingResult, contractAddress, 2, 3, 300);
  }

  void processSlotReadThenUpdateTx(final BlockProcessor blockProcessor) {
    Address contractAddress = Address.fromHexStringStrict(CONTRACT_ADDRESS);

    Transaction getSlot1Transaction =
        createContractUpdateSlotTransaction(
            0, contractAddress, "getSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.empty());
    Transaction setSlot1Transaction =
        createContractUpdateSlotTransaction(
            0, contractAddress, "setSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.of(1000));
    Transaction setSlo2Transaction =
        createContractUpdateSlotTransaction(
            1, contractAddress, "setSlot2", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(2000));
    Transaction setSlot3Transaction =
        createContractUpdateSlotTransaction(
            2, contractAddress, "setSlot3", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(3000));

    Block blockWithTransactions =
        createBlockWithTransactions(
            "0xdb4e2dc94bbfa48bc713f908900abc327c28e7c219c720b4bdeab52ad8b88616",
            Wei.of(5),
            getSlot1Transaction,
            setSlot1Transaction,
            setSlo2Transaction,
            setSlot3Transaction);
    MutableWorldState worldState = worldStateArchive.getWorldState();

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    assertTrue(blockProcessingResult.isSuccessful());

    // Verify the state
    assertContractStorage(worldState, contractAddress, 0, 1000);
    assertContractStorage(worldState, contractAddress, 1, 2000);
    assertContractStorage(worldState, contractAddress, 2, 3000);

    assertBlockAccessListAddresses(
        blockProcessingResult,
        Address.fromHexStringStrict(CONTRACT_ADDRESS),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT,
        coinbase);

    // contract balance is unchanged so no balance changes recorded
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    assertBalanceMatchesWorldState(blockProcessingResult, coinbase);

    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1), 3L);
    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2), 1L);

    assertStorageWrite(blockProcessingResult, contractAddress, 0, 1, 1000);
    assertStorageWrite(blockProcessingResult, contractAddress, 1, 2, 2000);
    assertStorageWrite(blockProcessingResult, contractAddress, 2, 3, 3000);
  }

  void processAccountReadThenUpdateTx(final BlockProcessor blockProcessor) {
    Address contractAddress = Address.fromHexStringStrict(CONTRACT_ADDRESS);
    Transaction transactionTransfer = // ACCOUNT_GENESIS_1 -> CONTRACT_ADDRESS
        createTransferTransaction(
            0,
            1_000_000_000_000_000_000L,
            300000L,
            5L,
            7L,
            CONTRACT_ADDRESS,
            ACCOUNT_GENESIS_1_KEYPAIR);
    Transaction getcontractBalanceTransaction =
        createContractReadAccountTransaction(
            1, contractAddress, "getBalance", ACCOUNT_GENESIS_1_KEYPAIR, ACCOUNT_2);

    Transaction sendEthFromContractTransaction =
        createContractUpdateAccountTransaction(
            0,
            contractAddress,
            "transferTo",
            ACCOUNT_GENESIS_2_KEYPAIR,
            ACCOUNT_2,
            500_000_000_000_000_000L);

    Block blockWithTransactions =
        createBlockWithTransactions(
            "0x3912cef7496ffde5dc6822b2d268c582faeadcb261c66c80a333aca6f5da9de6",
            Wei.of(5),
            transactionTransfer,
            getcontractBalanceTransaction,
            sendEthFromContractTransaction);
    MutableWorldState worldState = worldStateArchive.getWorldState();

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    assertTrue(blockProcessingResult.isSuccessful());

    // Verify the state
    BonsaiAccount contractAccount = (BonsaiAccount) worldState.get(contractAddress);
    BonsaiAccount updatedAccount0x2 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_2));
    assertThat(contractAccount.getBalance()).isEqualTo(Wei.of(500_000_000_000_000_000L));
    assertThat(updatedAccount0x2.getBalance()).isEqualTo(Wei.of(500_000_000_000_000_000L));

    assertBlockAccessListAddresses(
        blockProcessingResult,
        Address.fromHexStringStrict(CONTRACT_ADDRESS),
        Address.fromHexStringStrict(ACCOUNT_2),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT,
        coinbase);

    assertBalanceMatchesWorldState(blockProcessingResult, contractAddress);
    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_2));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    assertBalanceMatchesWorldState(blockProcessingResult, coinbase);

    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1), 2L);
    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2), 1L);
  }

  void processAccountUpdateThenReadTx(final BlockProcessor blockProcessor) {
    Address contractAddress = Address.fromHexStringStrict(CONTRACT_ADDRESS);
    Transaction transactionTransfer =
        createTransferTransaction(
            0,
            1_000_000_000_000_000_000L,
            300000L,
            5L,
            7L,
            CONTRACT_ADDRESS,
            ACCOUNT_GENESIS_1_KEYPAIR);

    Transaction sendEthFromContractTransaction =
        createContractUpdateAccountTransaction(
            1,
            contractAddress,
            "transferTo",
            ACCOUNT_GENESIS_1_KEYPAIR,
            ACCOUNT_2,
            500_000_000_000_000_000L);

    Transaction getcontractBalanceTransaction =
        createContractReadAccountTransaction(
            0, contractAddress, "getBalance", ACCOUNT_GENESIS_2_KEYPAIR, ACCOUNT_2);

    Block blockWithTransactions =
        createBlockWithTransactions(
            "0xb5b3d7b39b66b3e829b038d6ba9b38a3597cedd87df504828336dedb5f3e799f",
            Wei.of(5),
            transactionTransfer,
            sendEthFromContractTransaction,
            getcontractBalanceTransaction);
    MutableWorldState worldState = worldStateArchive.getWorldState();

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);
    assertTrue(blockProcessingResult.isSuccessful());

    // Verify the state
    BonsaiAccount contractAccount = (BonsaiAccount) worldState.get(contractAddress);
    BonsaiAccount updatedAccount0x2 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_2));

    assertThat(contractAccount.getBalance()).isEqualTo(Wei.of(500_000_000_000_000_000L));
    assertThat(updatedAccount0x2.getBalance()).isEqualTo(Wei.of(500_000_000_000_000_000L));

    assertBlockAccessListAddresses(
        blockProcessingResult,
        Address.fromHexStringStrict(CONTRACT_ADDRESS),
        Address.fromHexStringStrict(ACCOUNT_2),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT,
        coinbase);

    assertBalanceMatchesWorldState(blockProcessingResult, contractAddress);
    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_2));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    assertBalanceMatchesWorldState(blockProcessingResult, coinbase);

    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1), 2L);
    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2), 1L);
  }

  void processAccountReadThenUpdateTxWithTwoAccounts(final BlockProcessor blockProcessor) {
    Address contractAddress = Address.fromHexStringStrict(CONTRACT_ADDRESS);
    Transaction transactionTransfer = // ACCOUNT_GENESIS_1 -> CONTRACT_ADDRESS
        createTransferTransaction(
            0,
            1_000_000_000_000_000_000L,
            300000L,
            5L,
            7L,
            CONTRACT_ADDRESS,
            ACCOUNT_GENESIS_1_KEYPAIR);
    Transaction getcontractBalanceTransaction =
        createContractReadAccountTransaction(
            1, contractAddress, "getBalance", ACCOUNT_GENESIS_1_KEYPAIR, ACCOUNT_2);

    Transaction sendEthFromContractTransaction =
        createContractUpdateAccountTransaction(
            0,
            contractAddress,
            "transferTo",
            ACCOUNT_GENESIS_2_KEYPAIR,
            ACCOUNT_3,
            500_000_000_000_000_000L);

    Block blockWithTransactions =
        createBlockWithTransactions(
            "0xf81c87537a0a166a48ede069bf50358163ae697d1005cbaa7e6083f22a60a2fb",
            Wei.of(5),
            transactionTransfer,
            getcontractBalanceTransaction,
            sendEthFromContractTransaction);
    MutableWorldState worldState = worldStateArchive.getWorldState();

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    assertTrue(blockProcessingResult.isSuccessful());

    // Verify the state
    BonsaiAccount contractAccount = (BonsaiAccount) worldState.get(contractAddress);
    BonsaiAccount updatedAccount0x3 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_3));
    assertThat(contractAccount.getBalance()).isEqualTo(Wei.of(500_000_000_000_000_000L));
    assertThat(updatedAccount0x3.getBalance()).isEqualTo(Wei.of(500_000_000_000_000_000L));

    assertBlockAccessListAddresses(
        blockProcessingResult,
        Address.fromHexStringStrict(ACCOUNT_2),
        Address.fromHexStringStrict(ACCOUNT_3),
        Address.fromHexStringStrict(CONTRACT_ADDRESS),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT,
        coinbase);

    assertBalanceMatchesWorldState(blockProcessingResult, contractAddress);
    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_3));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    assertBalanceMatchesWorldState(blockProcessingResult, coinbase);

    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1), 2L);
    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2), 1L);
  }

  void processAccountUpdateThenReadTxWithTwoAccounts(final BlockProcessor blockProcessor) {
    Address contractAddress = Address.fromHexStringStrict(CONTRACT_ADDRESS);
    Transaction transactionTransfer = // ACCOUNT_GENESIS_1 -> CONTRACT_ADDRESS
        createTransferTransaction(
            0,
            1_000_000_000_000_000_000L,
            300000L,
            5L,
            7L,
            CONTRACT_ADDRESS,
            ACCOUNT_GENESIS_1_KEYPAIR);

    Transaction sendEthFromContractTransaction =
        createContractUpdateAccountTransaction(
            0,
            contractAddress,
            "transferTo",
            ACCOUNT_GENESIS_2_KEYPAIR,
            ACCOUNT_3,
            500_000_000_000_000_000L);

    Transaction getcontractBalanceTransaction =
        createContractReadAccountTransaction(
            1, contractAddress, "getBalance", ACCOUNT_GENESIS_1_KEYPAIR, ACCOUNT_2);

    Block blockWithTransactions =
        createBlockWithTransactions(
            "0xf81c87537a0a166a48ede069bf50358163ae697d1005cbaa7e6083f22a60a2fb",
            Wei.of(5),
            transactionTransfer,
            sendEthFromContractTransaction,
            getcontractBalanceTransaction);
    MutableWorldState worldState = worldStateArchive.getWorldState();

    BlockProcessingResult blockProcessingResult =
        blockProcessor.processBlock(protocolContext, blockchain, worldState, blockWithTransactions);

    assertTrue(blockProcessingResult.isSuccessful());

    // Verify the state
    BonsaiAccount contractAccount = (BonsaiAccount) worldState.get(contractAddress);
    BonsaiAccount updatedAccount0x3 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_3));
    assertThat(contractAccount.getBalance()).isEqualTo(Wei.of(500_000_000_000_000_000L));
    assertThat(updatedAccount0x3.getBalance()).isEqualTo(Wei.of(500_000_000_000_000_000L));

    assertBlockAccessListAddresses(
        blockProcessingResult,
        Address.fromHexStringStrict(ACCOUNT_2),
        Address.fromHexStringStrict(ACCOUNT_3),
        Address.fromHexStringStrict(CONTRACT_ADDRESS),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2),
        WITHDRAWAL_CONTRACT,
        CONSOLIDATION_CONTRACT,
        coinbase);

    assertBalanceMatchesWorldState(blockProcessingResult, contractAddress);
    assertBalanceMatchesWorldState(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_3));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertBalanceMatchesWorldState(
        blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
    assertBalanceMatchesWorldState(blockProcessingResult, coinbase);

    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_1), 2L);
    assertNonceChange(blockProcessingResult, Address.fromHexStringStrict(ACCOUNT_GENESIS_2), 1L);
  }

  private static KeyPair generateKeyPair(final String privateKeyHex) {
    final KeyPair keyPair =
        SignatureAlgorithmFactory.getInstance()
            .createKeyPair(
                SECPPrivateKey.create(
                    Bytes32.fromHexString(privateKeyHex), SignatureAlgorithm.ALGORITHM));
    return keyPair;
  }

  private Transaction createContractUpdateSlotTransaction(
      final int nonce,
      final Address contractAddress,
      final String methodSignature,
      final KeyPair keyPair,
      final Optional<Integer> value) {
    Bytes payload = encodeFunctionCall(methodSignature, value);
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(5))
        .maxFeePerGas(Wei.of(7))
        .gasLimit(3000000L)
        .to(contractAddress)
        .value(Wei.ZERO)
        .payload(payload)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(keyPair);
  }

  private Transaction createContractReadAccountTransaction(
      final int nonce,
      final Address contractAddress,
      final String methodSignature,
      final KeyPair keyPair,
      final String address) {
    Bytes payload = encodeFunctionCall(methodSignature, address);
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(5))
        .maxFeePerGas(Wei.of(7))
        .gasLimit(3000000L)
        .to(contractAddress)
        .value(Wei.ZERO)
        .payload(payload)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(keyPair);
  }

  private Transaction createContractUpdateAccountTransaction(
      final int nonce,
      final Address contractAddress,
      final String methodSignature,
      final KeyPair keyPair,
      final String address,
      final long value) {
    Bytes payload = encodeFunctionCall(methodSignature, address, value);
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(5))
        .maxFeePerGas(Wei.of(7))
        .gasLimit(3000000L)
        .to(contractAddress)
        .value(Wei.ZERO)
        .payload(payload)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(keyPair);
  }

  private Bytes encodeFunctionCall(final String methodSignature, final Optional<Integer> value) {
    List<Type> inputParameters =
        value.isPresent() ? Arrays.<Type>asList(new Uint256(value.get())) : List.of();
    Function function = new Function(methodSignature, inputParameters, List.of());
    return Bytes.fromHexString(FunctionEncoder.encode(function));
  }

  private Bytes encodeFunctionCall(final String methodSignature, final String address) {
    List<Type> inputParameters = Arrays.<Type>asList(new org.web3j.abi.datatypes.Address(address));
    Function function = new Function(methodSignature, inputParameters, List.of());
    return Bytes.fromHexString(FunctionEncoder.encode(function));
  }

  private Bytes encodeFunctionCall(
      final String methodSignature, final String address, final long value) {
    List<Type> inputParameters =
        Arrays.<Type>asList(new org.web3j.abi.datatypes.Address(address), new Uint256(value));
    Function function = new Function(methodSignature, inputParameters, List.of());
    return Bytes.fromHexString(FunctionEncoder.encode(function));
  }

  private Block createBlockWithTransactions(
      final String stateRoot, final Wei baseFeePerGas, final Transaction... transactions) {
    final BlockHeader parentHeader = blockchain.getChainHeadHeader();
    BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .number(parentHeader.getNumber() + 1L)
            .parentHash(parentHeader.getHash())
            .stateRoot(Hash.fromHexString(stateRoot))
            .gasLimit(30_000_000L)
            .baseFeePerGas(baseFeePerGas)
            .buildHeader();
    BlockBody blockBody =
        new BlockBody(Arrays.asList(transactions), Collections.emptyList(), Optional.empty());
    return new Block(blockHeader, blockBody);
  }

  private void assertBlockAccessListAddresses(
      final BlockProcessingResult result, final Address... expectedAddresses) {
    final List<Address> expected =
        Arrays.stream(expectedAddresses)
            .sorted(Comparator.comparing(addr -> addr.getBytes().toHexString()))
            .toList();

    final BlockAccessList blockAccessList =
        result.getYield().orElseThrow().getBlockAccessList().orElseThrow();

    final List<Address> actual =
        blockAccessList.accountChanges().stream()
            .map(BlockAccessList.AccountChanges::address)
            .toList();

    assertThat(actual).containsExactlyElementsOf(expected);
  }

  private void assertContractStorage(
      final MutableWorldState worldState,
      final Address contractAddress,
      final int slot,
      final int expectedValue) {
    BonsaiAccount contractAccount = (BonsaiAccount) worldState.get(contractAddress);
    UInt256 actualValue = contractAccount.getStorageValue(UInt256.valueOf(slot));
    assertThat(actualValue).isEqualTo(UInt256.valueOf(expectedValue));
  }

  private Transaction createTransferTransaction(
      final long nonce,
      final long value,
      final long gasLimit,
      final long maxPriorityFeePerGas,
      final long maxFeePerGas,
      final String hexAddress,
      final KeyPair keyPair) {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(maxPriorityFeePerGas))
        .maxFeePerGas(Wei.of(maxFeePerGas))
        .gasLimit(gasLimit)
        .to(Address.fromHexStringStrict(hexAddress))
        .value(Wei.of(value))
        .payload(Bytes.EMPTY)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(keyPair);
  }

  private BlockAccessList.AccountChanges getAccountChanges(
      final BlockProcessingResult result, final Address address) {
    final BlockAccessList blockAccessList =
        result.getYield().orElseThrow().getBlockAccessList().orElseThrow();

    return blockAccessList.accountChanges().stream()
        .filter(ac -> ac.address().equals(address))
        .findFirst()
        .orElseThrow();
  }

  private void assertBalanceMatchesWorldState(
      final BlockProcessingResult result, final Address address) {
    final BlockAccessList.AccountChanges accountChanges = getAccountChanges(result, address);
    assertThat(accountChanges.balanceChanges()).isNotEmpty();

    final BlockAccessList.BalanceChange lastChange = accountChanges.balanceChanges().getLast();
    final Wei balanceFromAccessList = Wei.fromHexString(lastChange.postBalance().toHexString());

    final MutableWorldState worldState = worldStateArchive.getWorldState();
    final Wei actualBalance = ((BonsaiAccount) worldState.get(address)).getBalance();

    if (address.equals(coinbase)) {
      final Wei delta = actualBalance.subtract(balanceFromAccessList);
      // TODO: Should REALLY BALs contain coinbase reward?
      assertThat(delta.isZero() || delta.equals(COINBASE_REWARD)).isTrue();
    } else {
      assertThat(actualBalance).isEqualTo(balanceFromAccessList);
    }
  }

  private void assertNonceChange(
      final BlockProcessingResult result, final Address address, final long nonce) {
    final BlockAccessList.AccountChanges accountChanges = getAccountChanges(result, address);
    assertThat(accountChanges.nonceChanges()).isNotEmpty();
    assertThat(accountChanges.nonceChanges().getLast().newNonce()).isEqualTo(nonce);
  }

  private void assertStorageWrite(
      final BlockProcessingResult result,
      final Address address,
      final int slot,
      final int txIndex,
      final int newValue) {
    final BlockAccessList.AccountChanges accountChanges = getAccountChanges(result, address);
    final UInt256 slotKey = UInt256.valueOf(slot);

    final BlockAccessList.SlotChanges slotChanges =
        accountChanges.storageChanges().stream()
            .filter(sc -> sc.slot().getSlotKey().orElseThrow().equals(slotKey))
            .findFirst()
            .orElseThrow();

    assertThat(slotChanges.changes()).isNotEmpty();
    final BlockAccessList.StorageChange last = slotChanges.changes().getLast();
    assertThat(last.txIndex()).isEqualTo(txIndex + 1);
    assertThat(last.newValue()).isEqualTo(UInt256.valueOf(newValue));
  }

  private void assertBalComputesHeaderRoot(final Block block, final BlockProcessingResult result) {
    final Hash expectedRoot = block.getHeader().getStateRoot();

    final BlockAccessList blockAccessList =
        result.getYield().orElseThrow().getBlockAccessList().orElseThrow();

    final Hash computedRoot =
        BalStateRootCalculator.computeAsync(protocolContext, block.getHeader(), blockAccessList)
            .join()
            .root();

    assertThat(computedRoot).isEqualTo(expectedRoot);
  }
}
