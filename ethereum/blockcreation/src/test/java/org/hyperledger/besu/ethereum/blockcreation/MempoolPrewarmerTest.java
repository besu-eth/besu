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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ConsensusContextFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionAddedListener;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.parallelization.PrewarmedTransactions;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes32;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

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
  private static final KeyPair OTHER_SENDER =
      SignatureAlgorithmFactory.getInstance()
          .createKeyPair(
              SECPPrivateKey.create(
                  Bytes32.fromHexString(
                      "c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3"),
                  "ECDSA"));
  private static final Address RECIPIENT =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");

  private final TransactionPool transactionPool = mock(TransactionPool.class);
  private final List<PendingTransaction> candidates = new CopyOnWriteArrayList<>();
  private final List<PendingTransactionAddedListener> poolListeners = new CopyOnWriteArrayList<>();
  private final List<Map<PendingTransaction, TransactionSelectionResult>> selectionResults =
      new CopyOnWriteArrayList<>();
  private final StubMetricsSystem metricsSystem = new StubMetricsSystem();
  private final AtomicLong now = new AtomicLong(System.currentTimeMillis());
  private final Clock clock =
      new Clock() {
        @Override
        public ZoneId getZone() {
          return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(final ZoneId zone) {
          return this;
        }

        @Override
        public Instant instant() {
          return Instant.ofEpochMilli(now.get());
        }
      };
  private ExecutionContextTestFixture fixture;
  private MempoolPrewarmer prewarmer;

  @BeforeEach
  void setUp() {
    when(transactionPool.isEnabled()).thenReturn(true);
    doAnswer(this::copyCandidates).when(transactionPool).selectTransactions(any());
    when(transactionPool.subscribePendingTransactions(any()))
        .thenAnswer(
            invocation -> {
              poolListeners.add(invocation.getArgument(0));
              return (long) poolListeners.size();
            });
    useGenesis(GenesisConfig.fromResource("/dev.json"), 4);
  }

  private Void copyCandidates(final InvocationOnMock invocation) {
    final PendingTransactions.PendingTransactionsSelector selector = invocation.getArgument(0);
    selectionResults.add(selector.evaluatePendingTransactions(List.copyOf(candidates)));
    return null;
  }

  private void useGenesis(final GenesisConfig genesis, final int gasLimitMultiplier) {
    if (prewarmer != null) {
      prewarmer.close();
    }
    fixture =
        ExecutionContextTestFixture.builder(genesis)
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    prewarmer =
        new MempoolPrewarmer(
            fixture.getProtocolContext(),
            fixture.getProtocolSchedule(),
            transactionPool,
            MiningConfiguration.newDefault(),
            2,
            gasLimitMultiplier,
            metricsSystem,
            clock);
  }

  private void useDevGenesisWith(final String gasLimit, final String baseFee, final int multiplier)
      throws IOException {
    useGenesis(
        GenesisConfig.fromConfig(
            devGenesis()
                .replace("\"gasLimit\": \"0x1fffffffffffff\"", "\"gasLimit\": \"" + gasLimit + "\"")
                .replace("\"baseFeePerGas\":\"0x0\"", "\"baseFeePerGas\":\"" + baseFee + "\"")),
        multiplier);
  }

  private static String devGenesis() throws IOException {
    return new String(
        GenesisConfig.class.getResourceAsStream("/dev.json").readAllBytes(),
        StandardCharsets.UTF_8);
  }

  @AfterEach
  void tearDown() {
    prewarmer.close();
  }

  @Test
  void executesThePoolCandidatesAndLeavesThePoolAsItIs() {
    final Transaction first = transfer(0);
    final Transaction second = transfer(1);
    addCandidate(first);
    addCandidate(second);

    prewarmer.start(head());

    awaitExecuted(2);
    assertThat(prewarmer.prewarmedTransactions()).contains(first.getHash(), second.getHash());
    assertThat(metricsSystem.getCounterValue("mempool_prewarm_transactions_total", "invalid"))
        .isZero();
    assertThat(selectionResults).isNotEmpty().allSatisfy(results -> assertThat(results).isEmpty());
  }

  @Test
  void keepsTheResultsForTheNextBlockToTake() {
    final PrewarmedTransactions prewarmed = new PrewarmedTransactions(new NoOpMetricsSystem());
    prewarmer.close();
    prewarmer =
        new MempoolPrewarmer(
            new ProtocolContext.Builder()
                .withBlockchain(fixture.getBlockchain())
                .withWorldStateArchive(fixture.getStateArchive())
                .withConsensusContext(new ConsensusContextFixture())
                .withPrewarmedTransactions(prewarmed)
                .build(),
            fixture.getProtocolSchedule(),
            transactionPool,
            MiningConfiguration.newDefault(),
            2,
            4,
            metricsSystem,
            clock);
    addCandidate(transfer(0));
    final BlockHeader next =
        new BlockHeaderTestFixture().parentHash(head().getHash()).buildHeader();

    prewarmer.start(head());

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> prewarmed.forBlock(next).isPresent());
  }

  @Test
  void keepsNoResultsWithoutReuse() {
    addCandidate(transfer(0));
    final BlockHeader next =
        new BlockHeaderTestFixture().parentHash(head().getHash()).buildHeader();

    prewarmer.start(head());

    awaitExecuted(1);
    assertThat(fixture.getProtocolContext().getPrewarmedTransactions().forBlock(next)).isEmpty();
  }

  @Test
  void preloadsTheTriePathsOfTheAccountsTheTransactionsChange() {
    addCandidate(transfer(0));

    prewarmer.start(head());

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> prewarmer.preloadedAccounts().containsAll(List.of(SENDER_ADDRESS, RECIPIENT)));
  }

  @Test
  void leavesTheHeadStateAsItIs() {
    final MutableWorldState headState = fixture.getStateArchive().getWorldState();
    final Hash rootHash = headState.rootHash();
    final Wei balance = headState.get(SENDER_ADDRESS).getBalance();
    addCandidate(transfer(0));

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
    addCandidate(transfer(0));

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
    addCandidate(prewarmed);
    prewarmer.start(head());
    awaitExecuted(1);

    final Block block = childBlock(prewarmed, transfer(1));
    prewarmer.stop(Optional.of(block));
    prewarmer.stop(Optional.of(block));

    assertThat(blockTransactions("true")).isEqualTo(1);
    assertThat(blockTransactions("false")).isEqualTo(1);
  }

  @Test
  void countsNothingForABlockArrivingAfterBlockBuilding() {
    addCandidate(transfer(0));
    prewarmer.start(head());
    awaitExecuted(1);
    prewarmer.stopForBlockBuilding();

    prewarmer.stop(Optional.of(childBlock(transfer(0))));

    assertThat(blockTransactions("true")).isZero();
    assertThat(blockTransactions("false")).isZero();
  }

  @Test
  void countsOnlyTheTransactionsThatRan() {
    final Transaction invalid = transfer(OTHER_SENDER, 0, 20_000, Wei.of(1_000_000_000_000L));
    addCandidate(invalid);
    prewarmer.start(head());
    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> invalid() == 1);

    prewarmer.stop(Optional.of(childBlock(invalid)));

    assertThat(blockTransactions("true")).isZero();
    assertThat(blockTransactions("false")).isEqualTo(1);
  }

  @Test
  void doesNotStartAgainForTheHeadABlockIsBuiltOn() {
    prewarmer.start(head());
    prewarmer.stopForBlockBuilding();
    addCandidate(transfer(0));

    prewarmer.start(head());

    // longer than several passes take
    Awaitility.await()
        .during(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .until(() -> executed() == 0);
  }

  @Test
  void startsAgainForTheSameHeadOnceTheNextSlotIsOver() {
    prewarmer.start(head());
    now.addAndGet(Duration.ofMinutes(1).toMillis());
    addCandidate(transfer(0));

    prewarmer.start(head());

    awaitExecuted(1);
  }

  @Test
  void stopsWithinAPassOnceTheGasBudgetIsUsed() throws IOException {
    // a budget of three transfers and a bit
    useDevGenesisWith("0x10000", "0x0", 1);
    for (int nonce = 0; nonce < 5; nonce++) {
      addCandidate(transfer(nonce));
    }

    prewarmer.start(head());

    awaitExecuted(4);
    Awaitility.await()
        .during(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .until(() -> executed() == 4);
  }

  @Test
  void leavesOutBlobTransactionsAndTransactionsBelowTheBaseFeeWithTheSendersLaterOnes()
      throws IOException {
    useDevGenesisWith("0x1fffffffffffff", "0x3B9ACA00", 4);
    final Transaction blob =
        new TransactionTestFixture()
            .type(TransactionType.BLOB)
            .nonce(0)
            .to(Optional.of(RECIPIENT))
            .maxFeePerGas(Optional.of(Wei.of(1_000_000_000_000L)))
            .maxPriorityFeePerGas(Optional.of(Wei.ONE))
            .versionedHashes(
                Optional.of(List.of(new VersionedHash(VersionedHash.SHA256_VERSION_ID, Hash.ZERO))))
            .createTransaction(SENDER);
    final Transaction underpriced = transfer(OTHER_SENDER, 0, 21_000, Wei.ONE);
    final Transaction warmable = transfer(OTHER_SENDER, 0, 21_000, Wei.of(1_000_000_000_000L));
    addCandidate(blob);
    addCandidate(transfer(1));
    addCandidate(underpriced);
    addCandidate(transfer(OTHER_SENDER, 1, 21_000, Wei.of(1_000_000_000_000L)));

    prewarmer.start(head());
    // longer than several passes take
    Awaitility.await()
        .during(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .until(() -> executed() == 0);
    candidates.clear();
    addCandidate(warmable);
    awaitExecuted(1);

    assertThat(prewarmer.prewarmedTransactions()).containsExactly(warmable.getHash());
    assertThat(invalid()).isZero();
  }

  @Test
  void stopsATransactionHalfwayOnceStopped() throws IOException {
    // calls the identity precompile in an endless loop
    final Address loop = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    useGenesis(
        GenesisConfig.fromConfig(
            devGenesis()
                .replace(
                    "\"alloc\": {",
                    "\"alloc\": {\""
                        + loop.toHexString()
                        + "\": {\"balance\": \"0x0\", \"code\": \"0x5b600060006000600060045afa50600056\"},")),
        4);
    addCandidate(
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .nonce(0)
            .to(Optional.of(loop))
            .gasLimit(1_000_000_000_000L)
            .maxFeePerGas(Optional.of(Wei.ONE))
            .maxPriorityFeePerGas(Optional.of(Wei.ONE))
            .createTransaction(SENDER));
    prewarmer.start(head());
    // long enough for the execution to start, far from the end of its gas
    Awaitility.await().pollDelay(Duration.ofMillis(500)).until(() -> true);

    prewarmer.stop(Optional.empty());

    // closing waits up to five seconds for the executions
    final long closing = System.nanoTime();
    prewarmer.close();
    assertThat(Duration.ofNanos(System.nanoTime() - closing)).isLessThan(Duration.ofSeconds(3));
    assertThat(executed()).isZero();
  }

  @Test
  void chargesTheGasBudgetOnceForATransactionWarmedAgain() throws IOException {
    // a budget of three transfers and a bit
    useDevGenesisWith("0x10000", "0x0", 1);
    addCandidate(transfer(0));
    addCandidate(transfer(1));
    prewarmer.start(head());
    awaitExecuted(2);

    // warmed again from the sender's first transaction
    addCandidate(transfer(2));
    awaitExecuted(5);
    addCandidate(transfer(OTHER_SENDER, 0, 21_000, Wei.of(1_000_000_000_000L)));

    awaitExecuted(6);
  }

  @Test
  void copiesThePoolAgainOnlyOnceATransactionArrives() {
    prewarmer.start(head());
    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> selectionResults.size() == 1);

    // the clock stands still, so no pass copies the pool for the promoted transactions
    Awaitility.await()
        .during(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .until(() -> selectionResults.size() == 1);
    addCandidate(transfer(0));

    awaitExecuted(1);
    assertThat(selectionResults).hasSize(2);
  }

  @Test
  void copiesThePoolAgainAfterASecondForTheTransactionsItPromotes() {
    final long slotMillis =
        fixture.getProtocolSchedule().getByBlockHeader(head()).getSlotDuration().toMillis();
    // in the middle of the slot after the head, a second before the warming ends
    now.set(TimeUnit.SECONDS.toMillis(head().getTimestamp()) + slotMillis / 2);
    prewarmer.start(head());
    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> selectionResults.size() == 1);
    // the pool does not announce the transactions it promotes
    candidates.add(new PendingTransaction.Remote(transfer(0)));

    now.addAndGet(1_000);

    awaitExecuted(1);
  }

  @Test
  void triesAgainInTheNextPassWhenCopyingThePoolFails() {
    doThrow(new IllegalStateException("pool"))
        .doAnswer(this::copyCandidates)
        .when(transactionPool)
        .selectTransactions(any());
    addCandidate(transfer(0));

    prewarmer.start(head());

    awaitExecuted(1);
  }

  @Test
  void stopsWhenTheHeadMovesWhileThePoolIsDisabled() {
    prewarmer.start(head());
    when(transactionPool.isEnabled()).thenReturn(false);

    prewarmer.start(head());
    addCandidate(transfer(0));

    // longer than several passes take
    Awaitility.await()
        .during(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .until(() -> executed() == 0);
  }

  @Test
  void preloadsNoTriePathsForABlockWithAnAccessList() throws IOException {
    final String json =
        new String(
            MempoolPrewarmerTest.class
                .getResourceAsStream("/block-transaction-selector/amsterdam-genesis.json")
                .readAllBytes(),
            StandardCharsets.UTF_8);
    useGenesis(
        GenesisConfig.fromConfig(
            json.replace("\"chainId\": 42", "\"chainId\": 1337")
                .replace(
                    "\"slotnumber\": \"0x42\"",
                    "\"slotnumber\": \"0x42\", \"alloc\": {\""
                        + SENDER_ADDRESS.toHexString()
                        + "\": {\"balance\": \"0xad78ebc5ac6200000\"}}")),
        4);
    addCandidate(transfer(SENDER, 0, 100_000, Wei.of(1_000_000_000_000L)));

    prewarmer.start(head());
    awaitExecuted(1);

    // longer than the execution's preloads take to start
    Awaitility.await()
        .during(Duration.ofMillis(500))
        .atMost(Duration.ofSeconds(3))
        .until(() -> prewarmer.preloadedAccounts().isEmpty());
  }

  @Test
  void countsNothingForABlockOnAnotherParent() {
    addCandidate(transfer(0));
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

  private void addCandidate(final Transaction transaction) {
    candidates.add(new PendingTransaction.Remote(transaction));
    poolListeners.forEach(listener -> listener.onTransactionAdded(transaction));
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
    return transfer(SENDER, nonce, 21_000, Wei.of(1_000_000_000_000L));
  }

  private static Transaction transfer(
      final KeyPair sender, final long nonce, final long gasLimit, final Wei maxFeePerGas) {
    return new TransactionTestFixture()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .to(Optional.of(RECIPIENT))
        .value(Wei.ONE)
        .gasLimit(gasLimit)
        .maxFeePerGas(Optional.of(maxFeePerGas))
        .maxPriorityFeePerGas(Optional.of(Wei.ONE))
        .createTransaction(sender);
  }

  private long executed() {
    return metricsSystem.getCounterValue("mempool_prewarm_transactions_total", "executed");
  }

  private long invalid() {
    return metricsSystem.getCounterValue("mempool_prewarm_transactions_total", "invalid");
  }

  private long blockTransactions(final String prewarmed) {
    return metricsSystem.getCounterValue("mempool_prewarm_block_transactions_total", prewarmed);
  }

  private void awaitExecuted(final long transactions) {
    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> executed() >= transactions);
  }
}
