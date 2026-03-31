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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie;

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
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview.BinTrieWorldState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BinTrieBlockProcessorIntegrationTest {

  private static final String ACCOUNT_2 = "0x0000000000000000000000000000000000000002";
  private static final String ACCOUNT_3 = "0x0000000000000000000000000000000000000003";

  private static final KeyPair ACCOUNT_GENESIS_1_KEYPAIR =
      generateKeyPair("c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3");

  private static final KeyPair ACCOUNT_GENESIS_2_KEYPAIR =
      generateKeyPair("fc5141e75bf622179f8eedada7fab3e2e6b3e3da8eb9df4f46d84df22df7430e");

  private WorldStateArchive worldStateArchive;
  private ProtocolSchedule protocolSchedule;

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/trie/pathbased/bintrie/genesis-bintrie-it.json";

  @BeforeEach
  public void setUp() {
    final ExecutionContextTestFixture contextTestFixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
            .dataStorageFormat(DataStorageFormat.BINTRIE)
            .build();
    worldStateArchive = contextTestFixture.getStateArchive();
    protocolSchedule = contextTestFixture.getProtocolSchedule();
  }

  @Test
  void testWorldStateIsBinTrie() {
    MutableWorldState worldState = worldStateArchive.getWorldState();
    assertThat(worldState).isInstanceOf(BinTrieWorldState.class);
  }

  @Test
  void testGenesisStateRootIsComputed() {
    MutableWorldState worldState = worldStateArchive.getWorldState();

    Hash rootHash = worldState.rootHash();
    assertThat(rootHash).isNotNull();
    assertThat(rootHash).isNotEqualTo(Hash.EMPTY_TRIE_HASH);
  }

  @Test
  void testGenesisAccountsExist() {
    MutableWorldState worldState = worldStateArchive.getWorldState();

    Account account1 =
        worldState.get(Address.fromHexString("0x627306090abab3a6e1400e9345bc60c78a8bef57"));
    Account account2 =
        worldState.get(Address.fromHexString("0x7f2d653f56ea8de6ffa554c7a0cd4e03af79f3eb"));

    assertThat(account1).isNotNull();
    assertThat(account2).isNotNull();
    assertThat(account1.getBalance().greaterThan(Wei.ZERO)).isTrue();
    assertThat(account2.getBalance().greaterThan(Wei.ZERO)).isTrue();
  }

  @Test
  void testSimpleTransferWithBinTrie() {
    final var protocolSpec =
        protocolSchedule.getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());

    final MainnetTransactionProcessor transactionProcessor = protocolSpec.getTransactionProcessor();

    Transaction transactionTransfer1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    Transaction transactionTransfer2 =
        createTransferTransaction(
            0, 2_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);

    MutableWorldState worldState = worldStateArchive.getWorldState();

    Account senderAccount1 = worldState.get(transactionTransfer1.getSender());
    Account senderAccount2 = worldState.get(transactionTransfer2.getSender());

    assertThat(senderAccount1).isNotNull();
    assertThat(senderAccount2).isNotNull();

    Wei initialBalance1 = senderAccount1.getBalance();
    Wei initialBalance2 = senderAccount2.getBalance();

    BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .number(1L)
            .gasLimit(30_000_000L)
            .baseFeePerGas(Wei.of(7))
            .buildHeader();

    var worldStateUpdater = worldState.updater();

    BlockHashLookup blockHashLookup = (frame, blockNumber) -> Hash.ZERO;

    TransactionProcessingResult result1 =
        transactionProcessor.processTransaction(
            worldStateUpdater,
            blockHeader,
            transactionTransfer1,
            blockHeader.getCoinbase(),
            OperationTracer.NO_TRACING,
            blockHashLookup,
            Wei.ZERO);

    assertTrue(result1.isSuccessful(), "Transfer 1 should be successful: " + result1);

    TransactionProcessingResult result2 =
        transactionProcessor.processTransaction(
            worldStateUpdater,
            blockHeader,
            transactionTransfer2,
            blockHeader.getCoinbase(),
            OperationTracer.NO_TRACING,
            blockHashLookup,
            Wei.ZERO);

    assertTrue(result2.isSuccessful(), "Transfer 2 should be successful: " + result2);

    worldStateUpdater.commit();
    worldState.persist(blockHeader);

    Account updatedSenderAccount1 = worldState.get(transactionTransfer1.getSender());
    Account updatedSenderAccount2 = worldState.get(transactionTransfer2.getSender());

    Account updatedAccount0x2 = worldState.get(Address.fromHexStringStrict(ACCOUNT_2));
    Account updatedAccount0x3 = worldState.get(Address.fromHexStringStrict(ACCOUNT_3));

    // Account 0x2 had initial balance of 1 Wei, plus transfer of 1 ETH
    assertThat(updatedAccount0x2.getBalance())
        .isEqualTo(Wei.of(1_000_000_000_000_000_000L).add(Wei.of(1)));
    // Account 0x3 had initial balance of 1 Wei, plus transfer of 2 ETH
    assertThat(updatedAccount0x3.getBalance())
        .isEqualTo(Wei.of(2_000_000_000_000_000_000L).add(Wei.of(1)));
    assertThat(updatedSenderAccount1.getBalance()).isLessThan(initialBalance1);
    assertThat(updatedSenderAccount2.getBalance()).isLessThan(initialBalance2);
  }

  @Test
  void testMultipleTransfersFromSameSender() {
    final var protocolSpec =
        protocolSchedule.getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());

    final MainnetTransactionProcessor transactionProcessor = protocolSpec.getTransactionProcessor();

    Transaction transferTransaction1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    Transaction transferTransaction2 =
        createTransferTransaction(
            1, 2_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_3, ACCOUNT_GENESIS_1_KEYPAIR);

    MutableWorldState worldState = worldStateArchive.getWorldState();

    Account senderAccount = worldState.get(transferTransaction1.getSender());
    assertThat(senderAccount).isNotNull();
    Wei initialBalance = senderAccount.getBalance();

    BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .number(1L)
            .gasLimit(30_000_000L)
            .baseFeePerGas(Wei.of(7))
            .buildHeader();

    var worldStateUpdater = worldState.updater();

    BlockHashLookup blockHashLookup = (frame, blockNumber) -> Hash.ZERO;

    TransactionProcessingResult result1 =
        transactionProcessor.processTransaction(
            worldStateUpdater,
            blockHeader,
            transferTransaction1,
            blockHeader.getCoinbase(),
            OperationTracer.NO_TRACING,
            blockHashLookup,
            Wei.ZERO);

    assertTrue(result1.isSuccessful());

    TransactionProcessingResult result2 =
        transactionProcessor.processTransaction(
            worldStateUpdater,
            blockHeader,
            transferTransaction2,
            blockHeader.getCoinbase(),
            OperationTracer.NO_TRACING,
            blockHashLookup,
            Wei.ZERO);

    assertTrue(result2.isSuccessful());

    worldStateUpdater.commit();
    worldState.persist(blockHeader);

    Account updatedSenderAccount = worldState.get(transferTransaction1.getSender());
    Account updatedAccount0x2 = worldState.get(Address.fromHexStringStrict(ACCOUNT_2));
    Account updatedAccount0x3 = worldState.get(Address.fromHexStringStrict(ACCOUNT_3));

    // Account 0x2 had initial balance of 1 Wei, plus transfer of 1 ETH
    assertThat(updatedAccount0x2.getBalance())
        .isEqualTo(Wei.of(1_000_000_000_000_000_000L).add(Wei.of(1)));
    // Account 0x3 had initial balance of 1 Wei, plus transfer of 2 ETH
    assertThat(updatedAccount0x3.getBalance())
        .isEqualTo(Wei.of(2_000_000_000_000_000_000L).add(Wei.of(1)));
    assertThat(updatedSenderAccount.getBalance()).isLessThan(initialBalance);
    assertThat(updatedSenderAccount.getNonce()).isEqualTo(2);
  }

  @Test
  void testTransactionProcessingChangesStateRoot() {
    final var protocolSpec =
        protocolSchedule.getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());

    final MainnetTransactionProcessor transactionProcessor = protocolSpec.getTransactionProcessor();

    Transaction transactionTransfer =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);

    MutableWorldState worldState = worldStateArchive.getWorldState();

    Hash initialRoot = worldState.rootHash();

    BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .number(1L)
            .gasLimit(30_000_000L)
            .baseFeePerGas(Wei.of(7))
            .buildHeader();

    var worldStateUpdater = worldState.updater();

    BlockHashLookup blockHashLookup = (frame, blockNumber) -> Hash.ZERO;

    TransactionProcessingResult result =
        transactionProcessor.processTransaction(
            worldStateUpdater,
            blockHeader,
            transactionTransfer,
            blockHeader.getCoinbase(),
            OperationTracer.NO_TRACING,
            blockHashLookup,
            Wei.ZERO);

    assertTrue(result.isSuccessful());

    worldStateUpdater.commit();
    worldState.persist(blockHeader);

    Hash newRoot = worldState.rootHash();

    assertThat(newRoot).isNotEqualTo(initialRoot);
  }

  private static KeyPair generateKeyPair(final String privateKeyHex) {
    return SignatureAlgorithmFactory.getInstance()
        .createKeyPair(
            SECPPrivateKey.create(
                Bytes32.fromHexString(privateKeyHex), SignatureAlgorithm.ALGORITHM));
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
        .chainId(BigInteger.valueOf(69420))
        .signAndBuild(keyPair);
  }
}
