/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.tuweni.bytes.Bytes32;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MempoolPrewarmerTest {

  // funded in the dev genesis
  private static final KeyPair SENDER =
      SignatureAlgorithmFactory.getInstance()
          .createKeyPair(
              SECPPrivateKey.create(
                  Bytes32.fromHexString(
                      "8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"),
                  "ECDSA"));
  private static final Address SENDER_ADDRESS =
      Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  private static final Address RECIPIENT =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");

  private final TransactionPool transactionPool = mock(TransactionPool.class);
  private final List<PendingTransaction> candidates = new CopyOnWriteArrayList<>();
  private final List<Map<PendingTransaction, TransactionSelectionResult>> selectionResults =
      new CopyOnWriteArrayList<>();
  private final StubMetricsSystem metricsSystem = new StubMetricsSystem();
  private ExecutionContextTestFixture fixture;
  private MempoolPrewarmer prewarmer;

  @BeforeEach
  void setUp() {
    fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource("/dev.json"))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    when(transactionPool.isEnabled()).thenReturn(true);
    doAnswer(
            invocation -> {
              final PendingTransactions.PendingTransactionsSelector selector =
                  invocation.getArgument(0);
              selectionResults.add(selector.evaluatePendingTransactions(List.copyOf(candidates)));
              return null;
            })
        .when(transactionPool)
        .selectTransactions(any());
    prewarmer =
        new MempoolPrewarmer(
            fixture.getProtocolContext(),
            fixture.getProtocolSchedule(),
            transactionPool,
            MiningConfiguration.newDefault(),
            2,
            4,
            metricsSystem);
  }

  @AfterEach
  void tearDown() {
    prewarmer.close();
  }

  @Test
  void executesThePoolCandidatesAndLeavesThePoolAsItIs() {
    final Transaction first = transfer(0);
    final Transaction second = transfer(1);
    candidates.add(new PendingTransaction.Remote(first));
    candidates.add(new PendingTransaction.Remote(second));

    prewarmer.start(head());

    awaitExecuted(2);
    assertThat(prewarmer.prewarmedTransactions()).contains(first.getHash(), second.getHash());
    assertThat(metricsSystem.getCounterValue("mempool_prewarm_transactions_total", "invalid"))
        .isZero();
    assertThat(selectionResults).isNotEmpty().allSatisfy(results -> assertThat(results).isEmpty());
  }

  @Test
  void leavesTheHeadStateAsItIs() {
    final MutableWorldState headState = fixture.getStateArchive().getWorldState();
    final Hash rootHash = headState.rootHash();
    final Wei balance = headState.get(SENDER_ADDRESS).getBalance();
    candidates.add(new PendingTransaction.Remote(transfer(0)));

    prewarmer.start(head());
    awaitExecuted(1);

    assertThat(headState.rootHash()).isEqualTo(rootHash);
    assertThat(headState.get(SENDER_ADDRESS).getBalance()).isEqualTo(balance);
    assertThat(headState.get(RECIPIENT)).isNull();
  }

  @Test
  void executesNothingOnceStopped() {
    prewarmer.start(head());
    prewarmer.stop(Optional.empty());
    candidates.add(new PendingTransaction.Remote(transfer(0)));

    // longer than several passes take
    Awaitility.await()
        .during(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .until(() -> executed() == 0);
    assertThat(prewarmer.prewarmedTransactions()).isEmpty();
  }

  @Test
  void countsTheTransactionsOfTheArrivingBlockThatWerePrewarmed() {
    final Transaction prewarmed = transfer(0);
    candidates.add(new PendingTransaction.Remote(prewarmed));
    prewarmer.start(head());
    awaitExecuted(1);

    prewarmer.stop(Optional.of(childBlock(prewarmed, transfer(1))));

    assertThat(blockTransactions("true")).isEqualTo(1);
    assertThat(blockTransactions("false")).isEqualTo(1);
  }

  @Test
  void countsNothingForABlockOnAnotherParent() {
    candidates.add(new PendingTransaction.Remote(transfer(0)));
    prewarmer.start(head());
    awaitExecuted(1);

    final BlockHeader otherParent =
        new BlockHeaderTestFixture().number(head().getNumber() + 1).buildHeader();
    prewarmer.stop(
        Optional.of(
            new Block(
                new BlockHeaderTestFixture()
                    .parentHash(otherParent.getHash())
                    .number(otherParent.getNumber() + 1)
                    .buildHeader(),
                new BlockBody(List.of(transfer(0)), List.of()))));

    assertThat(blockTransactions("true")).isZero();
    assertThat(blockTransactions("false")).isZero();
  }

  private BlockHeader head() {
    return fixture.getBlockchain().getChainHeadHeader();
  }

  private Block childBlock(final Transaction... transactions) {
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .parentHash(head().getHash())
            .number(head().getNumber() + 1)
            .buildHeader();
    return new Block(header, new BlockBody(List.of(transactions), List.of()));
  }

  private static Transaction transfer(final long nonce) {
    return new TransactionTestFixture()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .to(Optional.of(RECIPIENT))
        .value(Wei.ONE)
        .gasLimit(21_000)
        .maxFeePerGas(Optional.of(Wei.of(1_000_000_000_000L)))
        .maxPriorityFeePerGas(Optional.of(Wei.ONE))
        .createTransaction(SENDER);
  }

  private long executed() {
    return metricsSystem.getCounterValue("mempool_prewarm_transactions_total", "executed");
  }

  private long blockTransactions(final String prewarmed) {
    return metricsSystem.getCounterValue("mempool_prewarm_block_transactions_total", prewarmed);
  }

  private void awaitExecuted(final long transactions) {
    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> executed() >= transactions);
  }
}
