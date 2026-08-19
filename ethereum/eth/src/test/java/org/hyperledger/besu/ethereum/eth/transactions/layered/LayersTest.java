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
package org.hyperledger.besu.ethereum.eth.transactions.layered;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.hyperledger.besu.datatypes.TransactionType.ACCESS_LIST;
import static org.hyperledger.besu.datatypes.TransactionType.BLOB;
import static org.hyperledger.besu.datatypes.TransactionType.DELEGATE_CODE;
import static org.hyperledger.besu.datatypes.TransactionType.EIP1559;
import static org.hyperledger.besu.datatypes.TransactionType.FRONTIER;
import static org.hyperledger.besu.ethereum.core.TransactionTestFixture.createSignedCodeDelegation;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason.INVALIDATED;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayersTest.AuthorityAndNonce.NO_DELEGATIONS;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayersTest.AuthorityAndNonce.delegation;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayersTest.AuthorityAndNonce.toCodeDelegations;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayersTest.Sender.S1;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayersTest.Sender.S2;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayersTest.Sender.S3;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayersTest.Sender.S4;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayersTest.Sender.SP1;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayersTest.Sender.SP2;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.TX_EVALUATION_TOO_LONG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolReplacementHandler;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class LayersTest extends BaseTransactionPoolTest {
  private static final int MAX_PRIO_TRANSACTIONS = 3;
  private static final int MAX_FUTURE_FOR_SENDER = 10;
  private static final Wei BASE_FEE = Wei.ONE;
  private static final Wei MIN_GAS_PRICE = BASE_FEE;
  private static final byte MIN_SCORE = 125;

  private static final TransactionPoolConfiguration DEFAULT_TX_POOL_CONFIG =
      ImmutableTransactionPoolConfiguration.builder()
          .maxPrioritizedTransactions(MAX_PRIO_TRANSACTIONS)
          .maxPrioritizedTransactionsByType(Map.of(BLOB, 1))
          .maxFutureBySender(MAX_FUTURE_FOR_SENDER)
          .minScore(MIN_SCORE)
          .pendingTransactionsLayerMaxCapacityBytes(
              new PendingTransaction.Remote(
                          new BaseTransactionPoolTest().createEIP1559Transaction(0, KEYS1, 1))
                      .memorySize()
                  * 3L)
          .build();

  private static final TransactionPoolConfiguration BLOB_TX_POOL_CONFIG =
      ImmutableTransactionPoolConfiguration.builder()
          .maxPrioritizedTransactions(MAX_PRIO_TRANSACTIONS)
          .maxPrioritizedTransactionsByType(Map.of(BLOB, 1))
          .maxFutureBySender(MAX_FUTURE_FOR_SENDER)
          .minScore(MIN_SCORE)
          .pendingTransactionsLayerMaxCapacityBytes(
              new PendingTransaction.Remote(
                          new BaseTransactionPoolTest().createEIP4844Transaction(0, KEYS1, 1, 1))
                      .memorySize()
                  * 3L)
          .build();

  @ParameterizedTest
  @MethodSource("providerAddTransactions")
  void addTransactions(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerAddTransactionsMultipleSenders")
  void addTransactionsMultipleSenders(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerRemoveTransactions")
  void removeTransactions(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerInterleavedAddRemoveTransactions")
  void interleavedAddRemoveTransactions(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerBlockAdded")
  void removeConfirmedTransactions(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerNextNonceForSender")
  void nextNonceForSender(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerSelectTransactions")
  void selectTransactions(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerReorg")
  void reorg(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerAsyncWorldStateUpdates")
  void asyncWorldStateUpdates(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerPrioritySenders")
  void prioritySenders(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerMaxPrioritizedByType")
  void maxPrioritizedByType(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerPenalized")
  void penalized(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerConfirmedEIP7702")
  void confirmedEIP7702(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerSenderBalance")
  void senderBalance(final Scenario scenario) {
    assertScenario(scenario);
  }

  @ParameterizedTest
  @MethodSource("providerPendingBySender")
  void pendingBySender(final Scenario scenario) {
    assertScenario(scenario);
  }

  private void assertScenario(final Scenario scenario) {
    scenario.run();
  }

  static Stream<Arguments> providerAddTransactions() {
    return Stream.of(
        Arguments.of(
            new Scenario("add first").addForSender(S1, 0).expectedPrioritizedForSender(S1, 0)),
        Arguments.of(
            new Scenario("add first sparse").addForSender(S1, 1).expectedSparseForSender(S1, 1)),
        Arguments.of(
            new Scenario("fill prioritized")
                .addForSender(S1, 0, 1, 2)
                .expectedPrioritizedForSender(S1, 0, 1, 2)),
        Arguments.of(
            new Scenario("fill prioritized reverse")
                .addForSender(S1, 2, 1, 0)
                .expectedPrioritizedForSender(S1, 0, 1, 2)),
        Arguments.of(
            new Scenario("fill prioritized mixed order 1 step by step")
                .addForSender(S1, 2)
                .expectedSparseForSender(S1, 2)
                .addForSender(S1, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 2)
                .addForSender(S1, 1)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedSparseForSenders()),
        Arguments.of(
            new Scenario("fill prioritized mixed order 2")
                .addForSender(S1, 0, 2, 1)
                .expectedPrioritizedForSender(S1, 0, 1, 2)),
        Arguments.of(
            new Scenario("overflow to ready")
                .addForSender(S1, 0, 1, 2, 3)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)),
        Arguments.of(
            new Scenario("overflow to ready reverse")
                .addForSender(S1, 3, 2, 1, 0)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)),
        Arguments.of(
            new Scenario("overflow to ready mixed order 1")
                .addForSender(S1, 3, 0, 2, 1)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)),
        Arguments.of(
            new Scenario("overflow to ready mixed order 2")
                .addForSender(S1, 0, 3, 1, 2)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)),
        Arguments.of(
            new Scenario("overflow to sparse")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6)),
        Arguments.of(
            new Scenario("overflow to sparse reverse")
                .addForSender(S1, 6, 5, 4, 3, 2, 1, 0)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                // 4,5,6 are evicted since max capacity of sparse layer is 3 txs
                .expectedReadyForSender(S1, 3)
                .expectedDroppedForSender(S1, 4, 5, 6)),
        Arguments.of(
            new Scenario("overflow to sparse mixed order 1")
                .addForSender(S1, 6, 0, 4, 1, 3, 2, 5)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6)),
        Arguments.of(
            new Scenario("overflow to sparse mixed order 2")
                .addForSender(S1, 0, 4, 6, 1, 5, 2, 3)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6)),
        Arguments.of(
            new Scenario("overflow to sparse mixed order 3")
                .addForSender(S1, 0, 1, 2, 3, 5, 6, 4)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6)),
        Arguments.of(
            new Scenario("nonce gap to sparse 1")
                .addForSender(S1, 0, 2)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 2)),
        Arguments.of(
            new Scenario("nonce gap to sparse 2")
                .addForSender(S1, 0, 1, 2, 3, 5)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)
                .expectedSparseForSender(S1, 5)),
        Arguments.of(
            new Scenario("fill sparse 1")
                .addForSender(S1, 2, 3, 5)
                .expectedSparseForSender(S1, 2, 3, 5)),
        Arguments.of(
            new Scenario("fill sparse 2")
                .addForSender(S1, 5, 3, 2)
                .expectedSparseForSender(S1, 2, 3, 5)),
        Arguments.of(
            new Scenario("overflow sparse 1")
                .addForSender(S1, 1, 2, 3, 4)
                .expectedSparseForSender(S1, 1, 2, 3)
                .expectedDroppedForSender(S1, 4)),
        Arguments.of(
            new Scenario("overflow sparse 2")
                .addForSender(S1, 4, 2, 3, 1)
                .expectedSparseForSender(S1, 1, 2, 3)
                .expectedDroppedForSender(S1, 4)),
        Arguments.of(
            new Scenario("overflow sparse 3")
                .addForSender(S1, 0, 4, 2, 3, 5)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 2, 3, 4)
                .expectedDroppedForSender(S1, 5)));
  }

  static Stream<Arguments> providerAddTransactionsMultipleSenders() {
    return Stream.of(
        Arguments.of(
            new Scenario("add first")
                .addForSenders(S1, 0, S2, 0)
                .expectedPrioritizedForSenders(S2, 0, S1, 0)),
        Arguments.of(
            new Scenario("add first sparse")
                .addForSenders(S1, 1, S2, 2)
                .expectedSparseForSenders(S2, 2, S1, 1)),
        Arguments.of(
            new Scenario("fill prioritized 1")
                .addForSender(S1, 0, 1, 2)
                .addForSender(S2, 0, 1, 2)
                .expectedPrioritizedForSender(S2, 0, 1, 2)
                .expectedReadyForSender(S1, 0, 1, 2)),
        Arguments.of(
            new Scenario("fill prioritized 2")
                .addForSender(S2, 0, 1, 2)
                .addForSender(S1, 0, 1, 2)
                .expectedPrioritizedForSender(S2, 0, 1, 2)
                .expectedReadyForSender(S1, 0, 1, 2)),
        Arguments.of(
            new Scenario("fill prioritized 3")
                .addForSenders(S1, 0, S2, 0, S1, 1, S2, 1, S1, 2, S2, 2)
                .expectedPrioritizedForSender(S2, 0, 1, 2)
                .expectedReadyForSender(S1, 0, 1, 2)),
        Arguments.of(
            new Scenario("fill prioritized mixed order")
                .addForSenders(S1, 2, S2, 1)
                .expectedPrioritizedForSenders()
                .expectedReadyForSenders()
                .expectedSparseForSenders(S2, 1, S1, 2)
                .addForSenders(S2, 2, S1, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedReadyForSenders()
                .expectedSparseForSenders(S2, 1, S2, 2, S1, 2)
                .addForSenders(S1, 1)
                .expectedPrioritizedForSenders(S1, 0, S1, 1, S1, 2)
                .expectedReadyForSenders()
                .expectedSparseForSenders(S2, 1, S2, 2)
                .addForSenders(S2, 0)
                // only S2[0] is prioritized because there is no space to try to fill gaps
                .expectedPrioritizedForSenders(S2, 0, S1, 0, S1, 1)
                .expectedReadyForSender(S1, 2)
                .expectedSparseForSenders(S2, 1, S2, 2)
                // confirm some S1 txs will promote S2 txs
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSenders(S2, 0, S2, 1, S1, 1)
                .expectedReadyForSenders(S2, 2, S1, 2)
                .expectedSparseForSenders()
                .confirmedForSenders(S1, 1)
                .expectedPrioritizedForSenders(S2, 0, S2, 1, S2, 2)
                .expectedReadyForSender(S1, 2)
                .expectedSparseForSenders()
                .confirmedForSenders(S2, 1)
                .expectedPrioritizedForSenders(S2, 2, S1, 2)
                .expectedReadyForSenders()
                .expectedSparseForSenders()),
        Arguments.of(
            new Scenario("overflow to ready 1")
                .addForSenders(S1, 0, S1, 1, S2, 0, S2, 1)
                .expectedPrioritizedForSenders(S2, 0, S2, 1, S1, 0)
                .expectedReadyForSender(S1, 1)),
        Arguments.of(
            new Scenario("overflow to ready 2")
                .addForSenders(S2, 0, S2, 1, S1, 0, S1, 1)
                .expectedPrioritizedForSenders(S2, 0, S2, 1, S1, 0)
                .expectedReadyForSender(S1, 1)),
        Arguments.of(
            new Scenario("overflow to ready 3")
                .addForSenders(S1, 0, S2, 0, S2, 1, S1, 1)
                .expectedPrioritizedForSenders(S2, 0, S2, 1, S1, 0)
                .expectedReadyForSender(S1, 1)),
        Arguments.of(
            new Scenario("overflow to ready reverse 1")
                .addForSenders(S1, 1, S1, 0, S2, 1, S2, 0)
                .expectedPrioritizedForSenders(S2, 0, S2, 1, S1, 0)
                .expectedReadyForSender(S1, 1)),
        Arguments.of(
            new Scenario("overflow to ready reverse 2")
                .addForSenders(S1, 1, S2, 1, S2, 0, S1, 0)
                .expectedPrioritizedForSenders(S2, 0, S2, 1, S1, 0)
                .expectedReadyForSender(S1, 1)),
        Arguments.of(
            new Scenario("overflow to ready mixed")
                .addForSenders(S2, 1, S1, 0, S1, 1, S2, 0)
                .expectedPrioritizedForSenders(S2, 0, S2, 1, S1, 0)
                .expectedReadyForSender(S1, 1)),
        Arguments.of(
            new Scenario("overflow to sparse")
                .addForSenders(S1, 0, S1, 1, S2, 0, S2, 1, S3, 0, S3, 1, S3, 2)
                .expectedPrioritizedForSender(S3, 0, 1, 2)
                .expectedReadyForSenders(S2, 0, S2, 1, S1, 0)
                .expectedSparseForSender(S1, 1)),
        Arguments.of(
            new Scenario("overflow to sparse reverse")
                .addForSenders(S3, 2, S3, 1, S3, 0, S2, 1, S2, 0, S1, 1, S1, 0)
                .expectedPrioritizedForSender(S3, 0, 1, 2)
                .expectedReadyForSenders(S2, 0, S2, 1, S1, 0)
                .expectedSparseForSender(S1, 1)),
        Arguments.of(
            new Scenario("overflow to sparse mixed")
                .addForSenders(S2, 0, S3, 2, S1, 1)
                .expectedPrioritizedForSender(S2, 0)
                .expectedReadyForSenders()
                .expectedSparseForSenders(S1, 1, S3, 2)
                .addForSenders(S2, 1)
                .expectedPrioritizedForSenders(S2, 0, S2, 1)
                .expectedReadyForSenders()
                .expectedSparseForSenders(S1, 1, S3, 2)
                .addForSenders(S3, 0)
                .expectedPrioritizedForSenders(S3, 0, S2, 0, S2, 1)
                .expectedReadyForSenders()
                .expectedSparseForSenders(S1, 1, S3, 2)
                .addForSenders(S1, 0)
                .expectedPrioritizedForSenders(S3, 0, S2, 0, S2, 1)
                .expectedReadyForSenders(S1, 0, S1, 1)
                .expectedSparseForSender(S3, 2)
                .addForSenders(S3, 1)
                // only S3[1] is prioritized because there is no space to try to fill gaps
                .expectedPrioritizedForSenders(S3, 0, S3, 1, S2, 0)
                .expectedReadyForSenders(S2, 1, S1, 0, S1, 1)
                .expectedSparseForSender(S3, 2)
                .addForSenders(S4, 0, S4, 1, S3, 3)
                .expectedPrioritizedForSenders(S4, 0, S4, 1, S3, 0)
                .expectedReadyForSenders(S3, 1, S2, 0, S2, 1)
                .expectedSparseForSenders(S1, 0, S1, 1, S3, 2)
                // ToDo: non optimal discard, worth to improve?
                .expectedDroppedForSender(S3, 3)),
        Arguments.of(
            new Scenario("replacement cross layer")
                .addForSenders(S2, 0, S3, 2, S1, 1, S2, 1, S3, 0, S1, 0, S3, 1)
                // only S3[1] is prioritized because there is no space to try to fill gaps
                .expectedPrioritizedForSenders(S3, 0, S3, 1, S2, 0)
                .expectedReadyForSenders(S2, 1, S1, 0, S1, 1)
                .expectedSparseForSender(S3, 2)
                .replaceForSenders(S3, 2) // added in prioritized, but replacement in sparse
                .expectedPrioritizedForSenders(S3, 0, S3, 1, S3, 2)
                .expectedReadyForSenders(S2, 0, S2, 1, S1, 0)
                .expectedSparseForSender(S1, 1)));
  }

  static Stream<Arguments> providerRemoveTransactions() {
    return Stream.of(
        // when expected*ForSender(s) is not present, by default there is a check that the layers
        // are empty
        Arguments.of(new Scenario("remove not existing").removeForSender(S1, 0)),
        Arguments.of(new Scenario("add/remove first").addForSender(S1, 0).removeForSender(S1, 0)),
        Arguments.of(
            new Scenario("add/remove first sparse").addForSender(S1, 1).removeForSender(S1, 1)),
        Arguments.of(
            new Scenario("fill/remove prioritized 1")
                .addForSender(S1, 0, 1, 2)
                .removeForSender(S1, 0, 1, 2)),
        Arguments.of(
            new Scenario("fill/remove prioritized 2")
                .addForSender(S1, 0, 1, 2)
                .removeForSender(S1, 2, 1, 0)),
        Arguments.of(
            new Scenario("fill/remove prioritized reverse 1")
                .addForSender(S1, 2, 1, 0)
                .removeForSender(S1, 0, 1, 2)),
        Arguments.of(
            new Scenario("fill/remove prioritized reverse 2")
                .addForSender(S1, 2, 1, 0)
                .removeForSender(S1, 2, 1, 0)),
        Arguments.of(
            new Scenario("fill/remove first prioritized")
                .addForSender(S1, 0, 1, 2)
                .removeForSender(S1, 0)
                .expectedSparseForSender(S1, 1, 2)),
        Arguments.of(
            new Scenario("fill/remove last prioritized")
                .addForSender(S1, 0, 1, 2)
                .removeForSender(S1, 2)
                .expectedPrioritizedForSender(S1, 0, 1)),
        Arguments.of(
            new Scenario("fill/remove middle prioritized")
                .addForSender(S1, 0, 1, 2)
                .removeForSender(S1, 1)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 2)),
        Arguments.of(
            new Scenario("overflow to ready then remove 1")
                .addForSender(S1, 0, 1, 2, 3)
                .removeForSender(S1, 2)
                .expectedPrioritizedForSender(S1, 0, 1)
                .expectedSparseForSender(S1, 3)),
        Arguments.of(
            new Scenario("overflow to ready then remove 2")
                .addForSender(S1, 0, 1, 2, 3, 4)
                .removeForSender(S1, 4)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)),
        Arguments.of(
            new Scenario("overflow to ready then remove 3")
                .addForSender(S1, 0, 1, 2, 3, 4, 5)
                .removeForSender(S1, 4)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)
                .expectedSparseForSender(S1, 5)),
        Arguments.of(
            new Scenario("overflow to ready then remove 4")
                .addForSender(S1, 0, 1, 2, 3, 4, 5)
                .removeForSender(S1, 3)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedSparseForSender(S1, 4, 5)),
        Arguments.of(
            new Scenario("overflow to ready then remove 5")
                .addForSender(S1, 0, 1, 2, 3, 4, 5)
                .removeForSender(S1, 0)
                .expectedSparseForSender(S1, 1, 2, 3)
                .expectedDroppedForSender(S1, 4, 5)),
        Arguments.of(
            new Scenario("overflow to sparse then remove 1")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6)
                .removeForSender(S1, 6)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)),
        Arguments.of(
            new Scenario("overflow to sparse then remove 2")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6)
                .removeForSender(S1, 5)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4)
                .expectedSparseForSender(S1, 6)),
        Arguments.of(
            new Scenario("overflow to sparse then remove 3")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6)
                .removeForSender(S1, 2)
                .expectedPrioritizedForSender(S1, 0, 1)
                .expectedSparseForSender(S1, 3, 4, 5)
                .expectedDroppedForSender(S1, 6)),
        Arguments.of(
            new Scenario("overflow to sparse then remove 4")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .removeForSender(S1, 7)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6, 8)),
        Arguments.of(
            new Scenario("overflow to sparse then remove 5")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .removeForSender(S1, 6)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 7, 8)),
        Arguments.of(
            new Scenario("overflow to sparse then remove 6")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .removeForSender(S1, 6)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 7, 8)),
        Arguments.of(
            new Scenario("overflow to sparse then remove 7")
                .setAccountNonce(S1, 1)
                .addForSender(S1, 1, 2, 3, 4, 5, 8)
                .removeForSender(S1, 2)
                .expectedPrioritizedForSender(S1, 1)
                .expectedSparseForSender(S1, 3, 4, 5)
                .expectedDroppedForSender(S1, 8)));
  }

  static Stream<Arguments> providerInterleavedAddRemoveTransactions() {
    return Stream.of(
        Arguments.of(
            new Scenario("interleaved add/remove 1")
                .addForSender(S1, 0)
                .removeForSender(S1, 0)
                .addForSender(S1, 0)
                .expectedPrioritizedForSender(S1, 0)),
        Arguments.of(
            new Scenario("interleaved add/remove 2")
                .addForSender(S1, 0)
                .removeForSender(S1, 0)
                .addForSender(S1, 1)
                .expectedSparseForSender(S1, 1)),
        Arguments.of(
            new Scenario("interleaved add/remove 3")
                .addForSender(S1, 0)
                .removeForSender(S1, 0)
                .addForSender(S1, 1, 0)
                .expectedPrioritizedForSender(S1, 0, 1)));
  }

  static Stream<Arguments> providerBlockAdded() {
    return Stream.of(
        Arguments.of(new Scenario("confirmed not exist").confirmedForSenders(S1, 0)),
        Arguments.of(
            new Scenario("confirmed below existing lower nonce")
                .addForSender(S1, 1)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSender(S1, 1)),
        Arguments.of(
            new Scenario("confirmed only one existing")
                .addForSender(S1, 0)
                .confirmedForSenders(S1, 0)),
        Arguments.of(
            new Scenario("confirmed above existing")
                .addForSender(S1, 0)
                .confirmedForSenders(S1, 1)),
        Arguments.of(
            new Scenario("confirmed some existing 1")
                .addForSender(S1, 0, 1)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSender(S1, 1)),
        Arguments.of(
            new Scenario("confirmed some existing 2")
                .addForSender(S1, 0, 1, 2)
                .confirmedForSenders(S1, 1)
                .expectedPrioritizedForSender(S1, 2)),
        Arguments.of(
            new Scenario("overflow to ready and confirmed some existing 1")
                .addForSender(S1, 0, 1, 2, 3)
                .confirmedForSenders(S1, 3)),
        Arguments.of(
            new Scenario("overflow to ready and confirmed some existing 2")
                .addForSender(S1, 0, 1, 2, 3)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSender(S1, 1, 2, 3)),
        Arguments.of(
            new Scenario("overflow to ready and confirmed some existing 3")
                .addForSender(S1, 0, 1, 2, 3, 4, 5)
                .confirmedForSenders(S1, 1)
                .expectedPrioritizedForSender(S1, 2, 3, 4)
                .expectedReadyForSender(S1, 5)),
        Arguments.of(
            new Scenario("overflow to ready and confirmed all existing")
                .addForSender(S1, 0, 1, 2, 3, 4, 5)
                .confirmedForSenders(S1, 5)),
        Arguments.of(
            new Scenario("overflow to ready and confirmed above highest nonce")
                .addForSender(S1, 0, 1, 2, 3, 4, 5)
                .confirmedForSenders(S1, 6)),
        Arguments.of(
            new Scenario("overflow to sparse and confirmed some existing 1")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSender(S1, 1, 2, 3)
                .expectedReadyForSender(S1, 4, 5, 6)),
        Arguments.of(
            new Scenario("overflow to sparse and confirmed some existing 2")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6)
                .confirmedForSenders(S1, 2)
                .expectedPrioritizedForSender(S1, 3, 4, 5)
                .expectedReadyForSender(S1, 6)),
        Arguments.of(
            new Scenario("overflow to sparse and confirmed some existing 3")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6)
                .confirmedForSenders(S1, 3)
                .expectedPrioritizedForSender(S1, 4, 5, 6)),
        Arguments.of(
            new Scenario("overflow to sparse and confirmed some existing 4")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSender(S1, 1, 2, 3)
                .expectedReadyForSender(S1, 4, 5, 6)
                .expectedSparseForSender(S1, 7, 8)),
        Arguments.of(
            new Scenario("overflow to sparse and confirmed some existing with gap")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 7, 8)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSender(S1, 1, 2, 3)
                .expectedReadyForSender(S1, 4, 5)
                .expectedSparseForSender(S1, 7, 8)),
        Arguments.of(
            new Scenario("overflow to sparse and confirmed all w/o gap")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .confirmedForSenders(S1, 8)),
        Arguments.of(
            new Scenario("overflow to sparse and confirmed all with gap")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 7, 8)
                .confirmedForSenders(S1, 8)),
        Arguments.of(
            new Scenario("overflow to sparse and confirmed all before gap 1")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 7, 8)
                .confirmedForSenders(S1, 5)
                .expectedSparseForSender(S1, 7, 8)),
        Arguments.of(
            new Scenario("overflow to sparse and confirmed all before gap 2")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 7, 8)
                .confirmedForSenders(S1, 6)
                .expectedPrioritizedForSender(S1, 7, 8)));
  }

  static Stream<Arguments> providerNextNonceForSender() {
    return Stream.of(
        Arguments.of(new Scenario("not exist").expectedNextNonceForSenders(S1, null)),
        Arguments.of(
            new Scenario("first transaction")
                .addForSender(S1, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedNextNonceForSenders(S1, 1)),
        Arguments.of(
            new Scenario("fill prioritized")
                .addForSender(S1, 0, 1, 2)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedNextNonceForSenders(S1, 3)),
        Arguments.of(
            new Scenario("reverse fill prioritized")
                .addForSender(S1, 2, 1, 0)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedNextNonceForSenders(S1, 3)),
        Arguments.of(
            new Scenario("reverse fill prioritized 2")
                .addForSender(S1, 3, 2, 1, 0)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)
                .expectedNextNonceForSenders(S1, 4)),
        Arguments.of(
            new Scenario("overflow to ready")
                .addForSender(S1, 0, 1, 2, 3)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)
                .expectedNextNonceForSenders(S1, 4)),
        Arguments.of(
            new Scenario("fill ready")
                .addForSender(S1, 0, 1, 2, 3, 4, 5)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedNextNonceForSenders(S1, 6)),
        Arguments.of(
            new Scenario("overflow to sparse")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6)
                .expectedNextNonceForSenders(S1, 7)),
        Arguments.of(
            new Scenario("fill sparse")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6, 7, 8)
                .expectedNextNonceForSenders(S1, 9)),
        Arguments.of(
            new Scenario("drop transaction")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6, 7, 8)
                .expectedDroppedForSender(S1, 9)
                .expectedNextNonceForSenders(S1, 9)),
        Arguments.of(
            new Scenario("first with gap")
                .addForSender(S1, 1)
                .expectedSparseForSender(S1, 1)
                .expectedNextNonceForSenders(S1, null)),
        Arguments.of(
            new Scenario("sequence with gap 1")
                .addForSender(S1, 0, 2)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 2)
                .expectedNextNonceForSenders(S1, 1)),
        Arguments.of(
            new Scenario("sequence with gap 2")
                .addForSender(S1, 0, 1, 2, 4)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedSparseForSender(S1, 4)
                .expectedNextNonceForSenders(S1, 3)),
        Arguments.of(
            new Scenario("sequence with gap 3")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 7)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 7)
                .expectedNextNonceForSenders(S1, 6)),
        Arguments.of(
            new Scenario("sequence with gap 4")
                .addForSender(S1, 0, 1, 2, 3, 4, 5, 6, 8)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6, 8)
                .expectedNextNonceForSenders(S1, 7)),
        Arguments.of(
            new Scenario("out of order sequence 1")
                .addForSender(S1, 2, 0, 1)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedNextNonceForSenders(S1, 3)),
        Arguments.of(
            new Scenario("out of order sequence 2")
                .addForSender(S1, 2, 0, 4, 3, 1)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4)
                .expectedNextNonceForSenders(S1, 5)),
        Arguments.of(
            new Scenario("out of order sequence with gap 1")
                .addForSender(S1, 2, 1)
                .expectedSparseForSender(S1, 1, 2)
                .expectedNextNonceForSenders(S1, null)),
        Arguments.of(
            new Scenario("out of order sequence with gap 2")
                .addForSender(S1, 2, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 2)
                .expectedNextNonceForSenders(S1, 1)),
        Arguments.of(
            new Scenario("out of order sequence with gap 3")
                .addForSender(S1, 2, 0, 1, 4)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedSparseForSender(S1, 4)
                .expectedNextNonceForSenders(S1, 3)),
        Arguments.of(
            new Scenario("out of order sequence with gap 4")
                .addForSender(S1, 2, 0, 4, 1, 6, 3)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4)
                .expectedSparseForSender(S1, 6)
                .expectedNextNonceForSenders(S1, 5)),
        Arguments.of(
            new Scenario("no gap and confirmed 1")
                .addForSender(S1, 0, 1, 2)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSender(S1, 1, 2)
                .expectedNextNonceForSenders(S1, 3)),
        Arguments.of(
            new Scenario("all confirmed 1")
                .addForSender(S1, 0, 1, 2)
                .confirmedForSenders(S1, 2)
                .expectedNextNonceForSenders(S1, null)),
        Arguments.of(
            new Scenario("all confirmed 2")
                .addForSender(S1, 3, 0, 4, 1, 5, 2)
                .confirmedForSenders(S1, 8)
                .expectedNextNonceForSenders(S1, null)),
        Arguments.of(
            new Scenario("all confirmed 3")
                .addForSender(S1, 3, 0, 4, 1, 2, 5, 6, 7)
                .confirmedForSenders(S1, 8)
                .expectedNextNonceForSenders(S1, null)),
        Arguments.of(
            new Scenario("all confirmed step by step 1")
                .addForSender(S1, 3)
                .expectedSparseForSender(S1, 3)
                .expectedNextNonceForSenders(S1, null)
                .addForSender(S1, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 3)
                .expectedNextNonceForSenders(S1, 1)
                .addForSender(S1, 4)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 3, 4)
                .expectedNextNonceForSenders(S1, 1)
                .addForSender(S1, 1)
                .expectedPrioritizedForSender(S1, 0, 1)
                .expectedSparseForSender(S1, 3, 4)
                .expectedNextNonceForSenders(S1, 2)
                .addForSender(S1, 2)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4)
                .expectedNextNonceForSenders(S1, 5)
                .addForSender(S1, 5)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedNextNonceForSenders(S1, 6)
                .addForSender(S1, 6)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6)
                .expectedNextNonceForSenders(S1, 7)
                .addForSender(S1, 7)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6, 7)
                .expectedNextNonceForSenders(S1, 8)
                .addForSender(S1, 8)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6, 7, 8)
                .expectedNextNonceForSenders(S1, 9)
                .addForSender(S1, 9)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6, 7, 8)
                .expectedDroppedForSender(S1, 9)
                .expectedNextNonceForSenders(S1, 9)
                .confirmedForSenders(S1, 9)
                .expectedPrioritizedForSenders()
                .expectedReadyForSenders()
                .expectedSparseForSenders()
                .expectedNextNonceForSenders(S1, null)));
  }

  static Stream<Arguments> providerSelectTransactions() {
    return Stream.of(
        Arguments.of(new Scenario("no transactions").expectedSelectedTransactions()),
        Arguments.of(
            new Scenario("first transaction")
                .addForSender(S1, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSelectedTransactions(S1, 0)),
        Arguments.of(
            new Scenario("fill prioritized")
                .addForSender(S1, 0, 1, 2)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)),
        Arguments.of(
            new Scenario("reverse fill prioritized")
                .addForSender(S1, 2, 1, 0)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)),
        Arguments.of(
            new Scenario("reverse fill prioritized 2")
                .addForSender(S1, 3, 2, 1, 0)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)),
        Arguments.of(
            new Scenario("overflow to ready")
                .addForSender(S1, 0, 1, 2, 3)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)),
        Arguments.of(
            new Scenario("first with gap")
                .addForSender(S1, 1)
                .expectedSparseForSender(S1, 1)
                .expectedSelectedTransactions()),
        Arguments.of(
            new Scenario("sequence with gap 1")
                .addForSender(S1, 0, 2)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 2)
                .expectedSelectedTransactions(S1, 0)),
        Arguments.of(
            new Scenario("sequence with gap 2")
                .addForSender(S1, 0, 1, 2, 4)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedSparseForSender(S1, 4)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)),
        Arguments.of(
            new Scenario("out of order sequence 1")
                .addForSender(S1, 2, 0, 1)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)),
        Arguments.of(
            new Scenario("out of order sequence 2")
                .addForSender(S1, 2, 0, 4, 3, 1)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)),
        Arguments.of(
            new Scenario("out of order sequence with gap 1")
                .addForSender(S1, 2, 1)
                .expectedSparseForSender(S1, 1, 2)
                .expectedSelectedTransactions()),
        Arguments.of(
            new Scenario("out of order sequence with gap 2")
                .addForSender(S1, 2, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 2)
                .expectedSelectedTransactions(S1, 0)),
        Arguments.of(
            new Scenario("no gap and confirmed 1")
                .addForSender(S1, 0, 1, 2)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSender(S1, 1, 2)
                .expectedSelectedTransactions(S1, 1, S1, 2)),
        Arguments.of(
            new Scenario("all confirmed 1")
                .addForSender(S1, 0, 1, 2)
                .confirmedForSenders(S1, 2)
                .expectedSelectedTransactions()),
        Arguments.of(
            new Scenario("all confirmed step by step 1")
                .addForSender(S1, 3)
                .expectedSparseForSender(S1, 3)
                .expectedSelectedTransactions()
                .addForSender(S1, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 3)
                .expectedSelectedTransactions(S1, 0)
                .addForSender(S1, 4)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 3, 4)
                .expectedSelectedTransactions(S1, 0)
                .addForSender(S1, 1)
                .expectedPrioritizedForSender(S1, 0, 1)
                .expectedSparseForSender(S1, 3, 4)
                .expectedSelectedTransactions(S1, 0, S1, 1)
                .addForSender(S1, 2)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)
                .addForSender(S1, 5)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)
                .addForSender(S1, 6)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)
                .addForSender(S1, 7)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6, 7)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)
                .addForSender(S1, 8)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6, 7, 8)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)
                .addForSender(S1, 9)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .expectedSparseForSender(S1, 6, 7, 8)
                .expectedDroppedForSender(S1, 9)
                .expectedSelectedTransactions(S1, 0, S1, 1, S1, 2)
                .confirmedForSenders(S1, 9)
                .expectedPrioritizedForSenders()
                .expectedReadyForSenders()
                .expectedSparseForSenders()
                .expectedSelectedTransactions()));
  }

  static Stream<Arguments> providerReorg() {
    return Stream.of(
        Arguments.of(
            new Scenario("reorg")
                .setAccountNonce(S1, 0)
                .addForSender(S1, 0, 1, 2)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .confirmedForSenders(S1, 1)
                .expectedPrioritizedForSender(S1, 2)
                .expectedNextNonceForSenders(S1, 3)
                .addForSender(S1, 3)
                .expectedPrioritizedForSender(S1, 2, 3)
                .reorgForSenders(S1, 0) // rewind nonce due to reorg
                .addForSender(S1, 0, 1) // re-add reorged txs
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)));
  }

  static Stream<Arguments> providerAsyncWorldStateUpdates() {
    return Stream.of(
        Arguments.of(
            new Scenario("nonce forward just before confirmed block is processed")
                .setAccountNonce(S1, 0)
                .addForSender(S1, 0, 1, 2, 8, 9)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedSparseForSender(S1, 8, 9)
                .expectedNextNonceForSenders(S1, 3)
                // block has been imported and world state already has the new nonce,
                // but block has not yet been processed by txpool
                .setAccountNonce(S1, 5)
                .addForSender(S1, 7)
                .expectedPrioritizedForSenders()
                .expectedSparseForSender(S1, 7, 8, 9)));
  }

  static Stream<Arguments> providerPrioritySenders() {
    return Stream.of(
        Arguments.of(
            new Scenario("priority first same fee")
                .addForSenders(S1, 0, SP1, 0)
                .expectedPrioritizedForSenders(SP1, 0, S1, 0)),
        Arguments.of(
            new Scenario("priority first lower fee")
                .addForSenders(S2, 0, SP1, 0)
                .expectedPrioritizedForSenders(SP1, 0, S2, 0)),
        Arguments.of(
            new Scenario("priority first higher fee")
                .addForSenders(S1, 0, SP2, 0)
                .expectedPrioritizedForSenders(SP2, 0, S1, 0)),
        Arguments.of(
            new Scenario("same priority order by fee")
                .addForSenders(SP1, 0, SP2, 0)
                .expectedPrioritizedForSenders(SP2, 0, SP1, 0)),
        Arguments.of(
            new Scenario("same priority order by fee")
                .addForSenders(SP2, 0, SP1, 0)
                .expectedPrioritizedForSenders(SP2, 0, SP1, 0)),
        Arguments.of(
            new Scenario("priority first overflow to ready")
                .addForSender(S2, 0, 1, 2)
                .expectedPrioritizedForSender(S2, 0, 1, 2)
                .addForSender(SP1, 0)
                .expectedPrioritizedForSenders(SP1, 0, S2, 0, S2, 1)
                .expectedReadyForSender(S2, 2)),
        Arguments.of(
            new Scenario("priority first overflow to ready 2")
                .addForSender(S2, 0, 1, 2)
                .expectedPrioritizedForSender(S2, 0, 1, 2)
                .addForSender(SP1, 0, 1, 2)
                .expectedPrioritizedForSender(SP1, 0, 1, 2)
                .expectedReadyForSender(S2, 0, 1, 2)),
        Arguments.of(
            new Scenario("multiple priority senders first overflow to ready")
                .addForSender(S2, 0, 1, 2)
                .expectedPrioritizedForSender(S2, 0, 1, 2)
                .addForSenders(SP2, 0, SP1, 0)
                .expectedPrioritizedForSenders(SP2, 0, SP1, 0, S2, 0)
                .expectedReadyForSender(S2, 1, 2)),
        Arguments.of(
            new Scenario("priority with initial gap")
                .addForSender(S2, 0)
                .expectedPrioritizedForSender(S2, 0)
                .addForSender(SP1, 1) // initial gap
                .expectedPrioritizedForSender(S2, 0)
                .expectedSparseForSender(SP1, 1)
                .addForSender(SP1, 0) // fill gap
                .expectedPrioritizedForSenders(SP1, 0, SP1, 1, S2, 0)
                .expectedSparseForSenders()),
        Arguments.of(
            new Scenario("priority with initial gap overflow to ready")
                .addForSender(S2, 0, 1)
                .expectedPrioritizedForSender(S2, 0, 1)
                .addForSender(SP1, 1) // initial gap
                .expectedSparseForSender(SP1, 1)
                .addForSender(SP1, 0) // fill gap
                .expectedPrioritizedForSenders(SP1, 0, SP1, 1, S2, 0)
                .expectedReadyForSender(S2, 1)
                .expectedSparseForSenders()),
        Arguments.of(
            new Scenario("priority with initial gap overflow to ready when prioritized is full")
                .addForSender(S2, 0, 1, 2)
                .expectedPrioritizedForSender(S2, 0, 1, 2)
                .addForSender(SP1, 1) // initial gap
                .expectedSparseForSender(SP1, 1)
                .addForSender(SP1, 0) // fill gap, but there is not enough space to promote
                .expectedPrioritizedForSenders(SP1, 0, S2, 0, S2, 1)
                .expectedReadyForSender(S2, 2)
                .expectedSparseForSender(SP1, 1)
                .confirmedForSenders(
                    SP1, 0) // asap there is new space the priority tx is promoted first
                .expectedPrioritizedForSenders(SP1, 1, S2, 0, S2, 1)
                .expectedReadyForSender(S2, 2)
                .expectedSparseForSenders()),
        Arguments.of(
            new Scenario("overflow to ready promote priority first")
                .addForSender(SP2, 0, 1, 2)
                .expectedPrioritizedForSender(SP2, 0, 1, 2)
                .addForSender(S2, 0)
                .expectedReadyForSender(S2, 0)
                .addForSender(SP1, 0)
                .expectedReadyForSenders(SP1, 0, S2, 0)
                .confirmedForSenders(
                    SP2, 0) // asap there is new space the priority tx is promoted first
                .expectedPrioritizedForSenders(SP2, 1, SP2, 2, SP1, 0)
                .expectedReadyForSender(S2, 0)),
        Arguments.of(
            new Scenario("priority first overflow to sparse")
                .addForSender(SP2, 0, 1, 2)
                .addForSender(S3, 0)
                .expectedPrioritizedForSender(SP2, 0, 1, 2)
                .expectedReadyForSender(S3, 0)
                .addForSender(SP1, 0, 1, 2)
                .expectedPrioritizedForSender(SP2, 0, 1, 2)
                .expectedReadyForSender(SP1, 0, 1, 2)
                .expectedSparseForSender(S3, 0)),
        Arguments.of(
            new Scenario("priority first overflow to sparse 2")
                .addForSender(S2, 0, 1, 2)
                .addForSender(S3, 0, 1, 2)
                .expectedPrioritizedForSender(S3, 0, 1, 2)
                .expectedReadyForSender(S2, 0, 1, 2)
                .addForSender(SP1, 0)
                .expectedPrioritizedForSenders(SP1, 0, S3, 0, S3, 1)
                .expectedReadyForSenders(S3, 2, S2, 0, S2, 1)
                .expectedSparseForSender(S2, 2)),
        Arguments.of(
            new Scenario("overflow to sparse promote priority first")
                .addForSender(SP2, 0, 1, 2, 3, 4, 5)
                .expectedPrioritizedForSender(SP2, 0, 1, 2)
                .expectedReadyForSender(SP2, 3, 4, 5)
                .addForSender(S3, 0)
                .expectedSparseForSender(S3, 0)
                .addForSender(SP1, 0)
                .expectedSparseForSenders(SP1, 0, S3, 0)
                .confirmedForSenders(SP2, 0)
                .expectedPrioritizedForSender(SP2, 1, 2, 3)
                .expectedReadyForSenders(SP2, 4, SP2, 5, SP1, 0)
                .expectedSparseForSender(S3, 0)),
        Arguments.of(
            new Scenario("discard priority as last")
                .addForSender(SP2, 0, 1, 2, 3, 4, 5)
                .expectedPrioritizedForSender(SP2, 0, 1, 2)
                .expectedReadyForSender(SP2, 3, 4, 5)
                .addForSender(S3, 0)
                .expectedSparseForSender(S3, 0)
                .addForSender(SP1, 0, 1, 2)
                .expectedSparseForSender(SP1, 0, 1, 2)
                .expectedDroppedForSender(S3, 0)));
  }

  static Stream<Arguments> providerMaxPrioritizedByType() {
    return Stream.of(
        Arguments.of(
            new Scenario("first blob tx is prioritized", BLOB_TX_POOL_CONFIG)
                .addForSender(S1, BLOB, 0)
                .expectedPrioritizedForSender(S1, 0)),
        Arguments.of(
            new Scenario("multiple senders only first blob tx is prioritized", BLOB_TX_POOL_CONFIG)
                .addForSender(S1, BLOB, 0)
                .addForSender(S2, BLOB, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedReadyForSender(S2, 0)),
        Arguments.of(
            new Scenario("same sender following blob txs are moved to ready", BLOB_TX_POOL_CONFIG)
                .addForSender(S1, BLOB, 0, 1, 2)
                .expectedPrioritizedForSender(S1, 0)
                .expectedReadyForSender(S1, 1, 2)),
        Arguments.of(
            new Scenario("promoting txs respect prioritized count limit", BLOB_TX_POOL_CONFIG)
                .addForSender(S1, BLOB, 0, 1, 2)
                .expectedPrioritizedForSender(S1, 0)
                .expectedReadyForSender(S1, 1, 2)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSender(S1, 1)
                .expectedReadyForSender(S1, 2)),
        Arguments.of(
            new Scenario("filling gaps respect prioritized count limit", BLOB_TX_POOL_CONFIG)
                .addForSender(S1, BLOB, 1)
                .expectedSparseForSender(S1, 1)
                .addForSender(S1, BLOB, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 1)),
        Arguments.of(
            new Scenario("promoting to ready is unbounded", BLOB_TX_POOL_CONFIG)
                .addForSender(S1, BLOB, 0, 1, 2, 3, 4, 5, 6)
                .expectedPrioritizedForSender(S1, 0)
                .expectedReadyForSender(S1, 1, 2, 3)
                .expectedSparseForSender(S1, 4, 5, 6)
                .confirmedForSenders(S1, 3)
                .expectedPrioritizedForSender(S1, 4)
                .expectedReadyForSender(S1, 5, 6)
                .expectedSparseForSenders()));
  }

  static Stream<Arguments> providerPenalized() {
    return Stream.of(
        Arguments.of(
            new Scenario("single sender, single tx")
                .addForSender(S1, 0)
                .expectedPrioritizedForSender(S1, 0)
                .penalizeForSender(S1, 0)
                .expectedPrioritizedForSender(S1, 0)),
        Arguments.of(
            new Scenario("single sender penalize last")
                .addForSender(S1, 0, 1)
                .expectedPrioritizedForSender(S1, 0, 1)
                .penalizeForSender(S1, 1)
                .expectedPrioritizedForSender(S1, 0, 1)),
        Arguments.of(
            new Scenario("single sender penalize first")
                .addForSender(S1, 0, 1)
                .expectedPrioritizedForSender(S1, 0, 1)
                .penalizeForSender(S1, 0, 1)
                // even if 0 has less score it is always the first for the sender
                // since otherwise there is a nonce gap
                .expectedPrioritizedForSender(S1, 0, 1)),
        Arguments.of(
            new Scenario("multiple senders, penalize top")
                .addForSenders(S1, 0, S2, 0)
                // remember S2 pays more fees
                .expectedPrioritizedForSenders(S2, 0, S1, 0)
                .penalizeForSender(S2, 0)
                .expectedPrioritizedForSenders(S1, 0, S2, 0)),
        Arguments.of(
            new Scenario("multiple senders, penalize bottom")
                .addForSenders(S1, 0, S2, 0)
                .expectedPrioritizedForSenders(S2, 0, S1, 0)
                .penalizeForSender(S1, 0)
                .expectedPrioritizedForSenders(S2, 0, S1, 0)),
        Arguments.of(
            new Scenario("multiple senders, penalize middle")
                .addForSenders(S1, 0, S2, 0, S3, 0)
                .expectedPrioritizedForSenders(S3, 0, S2, 0, S1, 0)
                .penalizeForSender(S2, 0)
                .expectedPrioritizedForSenders(S3, 0, S1, 0, S2, 0)),
        Arguments.of(
            new Scenario("single sender, promote from ready")
                .addForSender(S1, 0, 1, 2, 3, 4, 5)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3, 4, 5)
                .penalizeForSender(S1, 3)
                .confirmedForSenders(S1, 0)
                // even if penalized 3 is promoted to avoid nonce gap
                .expectedPrioritizedForSender(S1, 1, 2, 3)
                .expectedReadyForSender(S1, 4, 5)),
        Arguments.of(
            new Scenario("multiple senders, overflow to ready")
                .addForSenders(S1, 0, S2, 0, S3, 0)
                .expectedPrioritizedForSenders(S3, 0, S2, 0, S1, 0)
                .expectedReadyForSenders()
                .penalizeForSender(S3, 0)
                .addForSender(S1, 1)
                .expectedPrioritizedForSenders(S2, 0, S1, 0, S1, 1)
                // S3(0) is demoted to ready even if it is paying more fees,
                // since has a lower score
                .expectedReadyForSender(S3, 0)),
        Arguments.of(
            new Scenario("multiple senders, overflow to sparse")
                .addForSenders(S1, 0, S2, 0, S3, 0, S1, 1, S2, 1, S3, 1)
                .expectedPrioritizedForSenders(S3, 0, S3, 1, S2, 0)
                .expectedReadyForSenders(S2, 1, S1, 0, S1, 1)
                .penalizeForSender(S2, 1)
                .addForSender(S2, 2)
                .expectedReadyForSenders(S1, 0, S1, 1, S2, 1)
                .expectedSparseForSender(S2, 2)),
        Arguments.of(
            new Scenario("remove below min score")
                .addForSender(S1, 0) // score 127
                .expectedPrioritizedForSender(S1, 0)
                .penalizeForSender(S1, 0) // score 126
                .expectedPrioritizedForSender(S1, 0)
                .penalizeForSender(S1, 0) // score 125
                .expectedPrioritizedForSender(S1, 0)
                .penalizeForSender(S1, 0) // score 124, removed since decreased score < MIN_SCORE
                .expectedPrioritizedForSenders()));
  }

  static Stream<Arguments> providerConfirmedEIP7702() {
    return Stream.of(
        Arguments.of(
            new Scenario("code delegation tx only")
                .addForSender(S1, delegation(S2, 0), 0)
                .expectedPrioritizedForSender(S1, 0)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSenders()),
        Arguments.of(
            new Scenario("confirmed delegation over plain tx")
                .addForSender(S2, 0)
                .addForSender(S1, delegation(S2, 0), 0)
                .expectedPrioritizedForSenders(S2, 0, S1, 0)
                .confirmedForSenders(S1, 0)
                // confirming the code delegation tx updates the nonce for S2, so his conflicting
                // plain tx is removed
                .expectedPrioritizedForSenders()),
        Arguments.of(
            new Scenario("confirmed plain tx over delegation")
                .addForSender(S2, 0)
                .addForSender(S1, delegation(S2, 0), 0)
                .expectedPrioritizedForSenders(S2, 0, S1, 0)
                .confirmedForSenders(S2, 0)
                // verify the code delegation for S2 is still there, of course that delegation will
                // fail,
                // but is it not possible to remove it from the list
                .expectedPrioritizedForSender(S1, 0)),
        Arguments.of(
            new Scenario("self code delegation")
                .addForSender(S1, delegation(S1, 1), 0)
                .expectedPrioritizedForSender(S1, 0)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSenders()),
        Arguments.of(
            new Scenario("self code delegation and plain tx")
                .addForSender(S1, delegation(S1, 1), 0)
                .addForSender(S1, 1)
                .expectedPrioritizedForSender(S1, 0, 1)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSenders()),
        Arguments.of(
            new Scenario("self code delegation and plain tx in sparse")
                .addForSender(S1, delegation(S1, 1), 0)
                .addForSender(S1, 2)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 2)
                .confirmedForSenders(S1, 0)
                .expectedPrioritizedForSender(S1, 2)
                .expectedSparseForSenders()));
  }

  static Stream<Arguments> providerSenderBalance() {
    return Stream.of(
        Arguments.of(
            new Scenario("not enough balance not added to prioritized")
                .hasSenderEnoughBalance(S1, false)
                .addForSender(S1, 0)
                .expectedReadyForSender(S1, 0)),
        Arguments.of(
            new Scenario("first enough balance then not")
                .addForSender(S1, 0)
                .expectedPrioritizedForSender(S1, 0)
                // sender hasn't enough balance for 2nd tx, so it goes to ready
                .hasSenderEnoughBalance(S1, false)
                .addForSender(S1, 1)
                .expectedReadyForSender(S1, 1)),
        Arguments.of(
            new Scenario("first no balance then yes")
                // sender hasn't enough balance for tx, so it goes to ready
                .hasSenderEnoughBalance(S1, false)
                .addForSender(S1, 0)
                .expectedReadyForSender(S1, 0)
                // now sender has balance and new block is imported and the tx is promoted
                .hasSenderEnoughBalance(S1, true)
                .confirmedForSenders()
                .expectedPrioritizedForSender(S1, 0)
                .expectedReadyForSenders()));
  }

  static Stream<Arguments> providerPendingBySender() {
    return Stream.of(
        Arguments.of(new Scenario("no txs for sender").expectedAllForSender(S1)),
        Arguments.of(
            new Scenario("only one in prio")
                .addForSender(S1, 0)
                .expectedPrioritizedForSender(S1, 0)
                .expectedAllForSender(S1, 0)),
        Arguments.of(
            new Scenario("only one in ready")
                .hasSenderEnoughBalance(S1, false)
                .addForSender(S1, 0)
                .expectedReadyForSender(S1, 0)
                .expectedAllForSender(S1, 0)),
        Arguments.of(
            new Scenario("only one in sparse")
                .addForSender(S1, 1)
                .expectedSparseForSender(S1, 1)
                .expectedAllForSender(S1, 1)),
        Arguments.of(
            new Scenario("only one in prio and sparse")
                .addForSender(S1, 0, 3)
                .expectedPrioritizedForSender(S1, 0)
                .expectedSparseForSender(S1, 3)
                .expectedAllForSender(S1, 0, 3)),
        Arguments.of(
            new Scenario("something in all layers")
                .addForSender(S1, 0, 1, 2, 3, 6)
                .expectedPrioritizedForSender(S1, 0, 1, 2)
                .expectedReadyForSender(S1, 3)
                .expectedSparseForSender(S1, 6)
                .expectedAllForSender(S1, 0, 1, 2, 3, 6)));
  }

  private static BlockHeader mockBlockHeader() {
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(BASE_FEE));
    return blockHeader;
  }

  private static boolean transactionReplacementTester(
      final TransactionPoolConfiguration poolConfig,
      final PendingTransaction pt1,
      final PendingTransaction pt2) {
    final TransactionPoolReplacementHandler transactionReplacementHandler =
        new TransactionPoolReplacementHandler(
            poolConfig.getPriceBump(), poolConfig.getBlobPriceBump());
    return transactionReplacementHandler.shouldReplace(pt1, pt2, mockBlockHeader());
  }

  static class Scenario extends BaseTransactionPoolTest implements Runnable {

    final String description;
    final TransactionPoolConfiguration poolConfig;
    final EvictCollectorLayer dropped;
    final SparseTransactions sparse;
    final ReadyTransactions ready;
    final AbstractPrioritizedTransactions prio;
    final LayeredPendingTransactions pending;
    final NotificationsChecker notificationsChecker = new NotificationsChecker();
    final List<Runnable> actions = new ArrayList<>();
    List<PendingTransaction> lastExpectedAll = new ArrayList<>();
    List<PendingTransaction> lastExpectedPrioritized = new ArrayList<>();
    List<PendingTransaction> lastExpectedReady = new ArrayList<>();
    List<PendingTransaction> lastExpectedSparse = new ArrayList<>();
    List<PendingTransaction> lastExpectedDropped = new ArrayList<>();

    final EnumMap<Sender, Long> nonceBySender = new EnumMap<>(Sender.class);

    {
      Arrays.stream(Sender.values()).forEach(e -> nonceBySender.put(e, 0L));
    }

    final EnumSet<Sender> sendersWithReorg = EnumSet.noneOf(Sender.class);

    final EnumMap<Sender, NavigableMap<Long, PendingTransaction>> liveTxsBySender =
        new EnumMap<>(Sender.class);

    {
      Arrays.stream(Sender.values()).forEach(e -> liveTxsBySender.put(e, new TreeMap<>()));
    }

    final EnumMap<Sender, NavigableMap<Long, PendingTransaction>> droppedTxsBySender =
        new EnumMap<>(Sender.class);

    {
      Arrays.stream(Sender.values()).forEach(e -> droppedTxsBySender.put(e, new TreeMap<>()));
    }

    Scenario(final String description) {
      this(description, DEFAULT_TX_POOL_CONFIG);
    }

    Scenario(final String description, final TransactionPoolConfiguration poolConfig) {
      this.description = description;
      this.poolConfig = poolConfig;

      final TransactionPoolMetrics txPoolMetrics = new TransactionPoolMetrics(metricsSystem);
      when(senderBalanceChecker.hasEnoughBalanceFor(any())).thenReturn(true);

      this.dropped = new EvictCollectorLayer(txPoolMetrics);
      final EthScheduler ethScheduler = new DeterministicEthScheduler();
      this.sparse =
          new SparseTransactions(
              poolConfig,
              ethScheduler,
              this.dropped,
              txPoolMetrics,
              (pt1, pt2) -> transactionReplacementTester(poolConfig, pt1, pt2),
              new BlobCache());

      this.ready =
          new ReadyTransactions(
              poolConfig,
              ethScheduler,
              this.sparse,
              txPoolMetrics,
              (pt1, pt2) -> transactionReplacementTester(poolConfig, pt1, pt2),
              new BlobCache());

      this.prio =
          new BaseFeePrioritizedTransactions(
              poolConfig,
              LayersTest::mockBlockHeader,
              ethScheduler,
              this.ready,
              txPoolMetrics,
              (pt1, pt2) -> transactionReplacementTester(poolConfig, pt1, pt2),
              FeeMarket.london(0L),
              new BlobCache(),
              MiningConfiguration.newDefault().setMinTransactionGasPrice(MIN_GAS_PRICE),
              senderBalanceChecker);

      this.pending = new LayeredPendingTransactions(poolConfig, this.prio, ethScheduler);

      this.pending.subscribePendingTransactions(notificationsChecker::collectAddNotification);
      this.pending.subscribeDroppedTransactions(
          (tx, reason) -> notificationsChecker.collectDropNotification(tx));
    }

    @Override
    public void run() {
      actions.forEach(Runnable::run);
      assertExpectedPrioritized(prio, lastExpectedPrioritized);
      assertExpectedReady(ready, lastExpectedReady);
      assertExpectedSparse(sparse, lastExpectedSparse);
      assertExpectedDropped(dropped, lastExpectedDropped);
    }

    public Scenario addForSender(final Sender sender, final long... nonce) {
      return addForSender(sender, EIP1559, NO_DELEGATIONS, nonce);
    }

    public Scenario addForSender(
        final Sender sender, final TransactionType type, final long... nonce) {
      internalAddForSender(sender, type, NO_DELEGATIONS, nonce);
      actions.add(notificationsChecker::assertExpectedNotifications);
      return this;
    }

    public Scenario addForSender(
        final Sender sender, final AuthorityAndNonce[] authorityAndNonces, final long... nonce) {
      return addForSender(sender, DELEGATE_CODE, authorityAndNonces, nonce);
    }

    public Scenario addForSender(
        final Sender sender,
        final TransactionType type,
        final AuthorityAndNonce[] authorityAndNonces,
        final long... nonce) {
      internalAddForSender(sender, type, authorityAndNonces, nonce);
      actions.add(notificationsChecker::assertExpectedNotifications);
      return this;
    }

    private void internalAddForSender(
        final Sender sender,
        final TransactionType type,
        final AuthorityAndNonce[] authorityAndNonces,
        final long... nonce) {
      actions.add(
          () -> {
            Arrays.stream(nonce)
                .forEach(
                    n -> {
                      final var pendingTx = create(sender, type, authorityAndNonces, n);
                      final Account mockSender = mock(Account.class);
                      when(mockSender.getNonce()).thenReturn(nonceBySender.get(sender));
                      pending.addTransaction(pendingTx, Optional.of(mockSender));
                      notificationsChecker.addExpectedAddNotification(pendingTx);
                    });

            // reorg case
            if (sendersWithReorg.contains(sender)) {
              // reorg is removing and re-adding all sender txs, so assert notifications accordingly
              final var currentPendingTxs =
                  liveTxsBySender.get(sender).tailMap(nonce[nonce.length - 1], false).values();
              currentPendingTxs.forEach(
                  pt -> {
                    notificationsChecker.addExpectedAddNotification(pt);
                    notificationsChecker.addExpectedDropNotification(pt);
                  });
              sendersWithReorg.remove(sender);
            }

            // reconciliation case
            final var txsRemovedByReconciliation =
                liveTxsBySender.get(sender).headMap(nonceBySender.get(sender), false).values();
            if (!txsRemovedByReconciliation.isEmpty()) {
              // reconciliation is removing all sender txs, and re-adding only the ones with a
              // larger nonce, so assert notifications accordingly
              final var reconciledPendingTxs =
                  liveTxsBySender.get(sender).tailMap(nonce[nonce.length - 1], false).values();
              txsRemovedByReconciliation.forEach(notificationsChecker::addExpectedDropNotification);
              reconciledPendingTxs.forEach(
                  pt -> {
                    notificationsChecker.addExpectedDropNotification(pt);
                    notificationsChecker.addExpectedAddNotification(pt);
                  });
              txsRemovedByReconciliation.clear();
            }

            handleDropped();
          });
    }

    private void handleDropped() {
      // handle dropped tx due to layer or pool full
      final var droppedTxs = dropped.getEvictedTransactions();
      droppedTxs.forEach(notificationsChecker::addExpectedDropNotification);
      droppedTxs.stream()
          .forEach(
              pt -> {
                liveTxsBySender.get(Sender.getByAddress(pt.getSender())).remove(pt.getNonce());
                droppedTxsBySender.get(Sender.getByAddress(pt.getSender())).put(pt.getNonce(), pt);
              });
    }

    public Scenario addForSenders(final Object... args) {
      for (int i = 0; i < args.length; i = i + 2) {
        final Sender sender = (Sender) args[i];
        final long nonce = (int) args[i + 1];
        internalAddForSender(sender, EIP1559, NO_DELEGATIONS, nonce);
      }
      actions.add(notificationsChecker::assertExpectedNotifications);
      return this;
    }

    public Scenario replaceForSender(final Sender sender, final long... nonce) {
      internalReplaceForSender(sender, nonce);
      actions.add(notificationsChecker::assertExpectedNotifications);
      return this;
    }

    private Scenario internalReplaceForSender(final Sender sender, final long... nonce) {
      actions.add(
          () -> {
            Arrays.stream(nonce)
                .forEach(
                    n -> {
                      final var maybeExistingTx = getMaybe(sender, n);
                      maybeExistingTx.ifPresentOrElse(
                          existingTx -> {
                            final var pendingTx = replace(sender, existingTx);
                            final Account mockSender = mock(Account.class);
                            when(mockSender.getNonce()).thenReturn(nonceBySender.get(sender));
                            pending.addTransaction(pendingTx, Optional.of(mockSender));
                            notificationsChecker.addExpectedAddNotification(pendingTx);
                            notificationsChecker.addExpectedDropNotification(existingTx);
                          },
                          () ->
                              fail(
                                  "Could not replace non-existing transaction with nonce "
                                      + n
                                      + " for sender "
                                      + sender.name()));
                    });
          });
      return this;
    }

    public Scenario replaceForSenders(final Object... args) {
      for (int i = 0; i < args.length; i = i + 2) {
        final Sender sender = (Sender) args[i];
        final long nonce = (int) args[i + 1];
        internalReplaceForSender(sender, nonce);
      }
      actions.add(notificationsChecker::assertExpectedNotifications);
      return this;
    }

    public Scenario confirmedForSenders(final Object... args) {
      actions.add(
          () -> {
            final Map<Sender, Long> maxConfirmedNonceBySender = new HashMap<>();
            for (int i = 0; i < args.length; i = i + 2) {
              final Sender sender = (Sender) args[i];
              final long nonce = (int) args[i + 1];
              maxConfirmedNonceBySender.put(sender, nonce);
              nonceBySender.put(sender, nonce + 1);

              // if the confirmed tx contains delegations then update the confirmed nonce
              // accordingly
              getMaybe(sender, nonce)
                  .ifPresent(
                      confirmedTx ->
                          confirmedTx
                              .getTransaction()
                              .getCodeDelegationList()
                              .ifPresent(
                                  codeDelegations ->
                                      codeDelegations.forEach(
                                          cd -> {
                                            final var authority =
                                                Sender.getByAddress(cd.authorizer().get());
                                            maxConfirmedNonceBySender.compute(
                                                authority,
                                                (unused, currentMax) ->
                                                    currentMax == null
                                                        ? cd.nonce()
                                                        : Math.max(currentMax, cd.nonce()));
                                            nonceBySender.compute(
                                                authority,
                                                (unused, currentNonce) ->
                                                    currentNonce == null
                                                        ? cd.nonce() + 1
                                                        : Math.max(currentNonce, cd.nonce()) + 1);
                                          })));
            }

            maxConfirmedNonceBySender.entrySet().stream()
                .forEach(
                    san -> {
                      final var sender = san.getKey();
                      final var nonce = san.getValue();
                      for (final var pendingTx : getAll(sender)) {
                        if (pendingTx.getNonce() <= nonce) {
                          notificationsChecker.addExpectedDropNotification(
                              liveTxsBySender.get(sender).remove(pendingTx.getNonce()));
                        }
                      }
                    });

            prio.blockAdded(
                FeeMarket.london(0L),
                mockBlockHeader(),
                maxConfirmedNonceBySender.entrySet().stream()
                    .collect(
                        Collectors.toMap(entry -> entry.getKey().address, Map.Entry::getValue)));
            notificationsChecker.assertExpectedNotifications();
          });
      return this;
    }

    public Scenario setAccountNonce(final Sender sender, final long nonce) {
      actions.add(() -> nonceBySender.put(sender, nonce));
      return this;
    }

    public Scenario reorgForSenders(final Object... args) {
      actions.add(
          () -> {
            for (int i = 0; i < args.length; i = i + 2) {
              final Sender sender = (Sender) args[i];
              final long nonce = (int) args[i + 1];
              nonceBySender.put(sender, nonce);
              sendersWithReorg.add(sender);
            }
          });
      return this;
    }

    private PendingTransaction create(
        final Sender sender,
        final TransactionType type,
        final AuthorityAndNonce[] authorityAndNonces,
        final long nonce) {
      if (liveTxsBySender.get(sender).containsKey(nonce)) {
        fail(
            "Transaction for sender " + sender.name() + " with nonce " + nonce + " already exists");
      }
      final var newPendingTx =
          switch (type) {
            case FRONTIER -> createFrontierPendingTransaction(sender, nonce);
            case ACCESS_LIST -> createAccessListPendingTransaction(sender, nonce);
            case EIP1559 -> createEIP1559PendingTransaction(sender, nonce);
            case BLOB -> createBlobPendingTransaction(sender, nonce);
            case DELEGATE_CODE ->
                createEIP7702PendingTransaction(sender, nonce, authorityAndNonces);
          };
      liveTxsBySender.get(sender).put(nonce, newPendingTx);
      return newPendingTx;
    }

    private PendingTransaction replace(final Sender sender, final PendingTransaction pendingTx) {
      final var replaceTx =
          createRemotePendingTransaction(
              createTransactionReplacement(pendingTx.getTransaction(), sender.key),
              sender.hasPriority);
      liveTxsBySender.get(sender).replace(pendingTx.getNonce(), replaceTx);
      return replaceTx;
    }

    private Optional<PendingTransaction> getMaybe(final Sender sender, final long nonce) {
      return Optional.ofNullable(liveTxsBySender.get(sender).get(nonce));
    }

    private PendingTransaction get(final Sender sender, final long nonce) {
      return getMaybe(sender, nonce).get();
    }

    private List<PendingTransaction> getAll(final Sender sender) {
      return List.copyOf(liveTxsBySender.get(sender).values());
    }

    private PendingTransaction createFrontierPendingTransaction(
        final Sender sender, final long nonce) {
      return createRemotePendingTransaction(
          createTransaction(FRONTIER, nonce, Wei.ONE, 0, List.of(), sender.key),
          sender.hasPriority);
    }

    private PendingTransaction createAccessListPendingTransaction(
        final Sender sender, final long nonce) {
      return createRemotePendingTransaction(
          createTransaction(ACCESS_LIST, nonce, Wei.ONE, 0, List.of(), sender.key),
          sender.hasPriority);
    }

    private PendingTransaction createEIP1559PendingTransaction(
        final Sender sender, final long nonce) {
      return createRemotePendingTransaction(
          createEIP1559Transaction(nonce, sender.key, sender.gasFeeMultiplier), sender.hasPriority);
    }

    private PendingTransaction createBlobPendingTransaction(final Sender sender, final long nonce) {
      return createRemotePendingTransaction(
          createEIP4844Transaction(nonce, sender.key, sender.gasFeeMultiplier, 1),
          sender.hasPriority);
    }

    private PendingTransaction createEIP7702PendingTransaction(
        final Sender sender, final long nonce, final AuthorityAndNonce[] authorityAndNonces) {
      return createRemotePendingTransaction(
          createEIP7702Transaction(
              nonce, sender.key, sender.gasFeeMultiplier, toCodeDelegations(authorityAndNonces)),
          sender.hasPriority);
    }

    public Scenario expectedAllForSender(final Sender sender, final long... nonce) {
      actions.add(
          () -> {
            lastExpectedAll = expectedForSender(sender, nonce);
            assertExpectedAll(prio, sender, lastExpectedAll);
          });
      return this;
    }

    public Scenario expectedPrioritizedForSender(final Sender sender, final long... nonce) {
      actions.add(
          () -> {
            lastExpectedPrioritized = expectedForSender(sender, nonce);
            assertExpectedPrioritized(prio, lastExpectedPrioritized);
          });
      return this;
    }

    public Scenario expectedReadyForSender(final Sender sender, final long... nonce) {
      actions.add(
          () -> {
            lastExpectedReady = expectedForSender(sender, nonce);
            assertExpectedReady(ready, lastExpectedReady);
          });
      return this;
    }

    public Scenario expectedSparseForSender(final Sender sender, final long... nonce) {
      actions.add(
          () -> {
            lastExpectedSparse = expectedForSender(sender, nonce);
            assertExpectedSparse(sparse, lastExpectedSparse);
          });
      return this;
    }

    public Scenario expectedDroppedForSender(final Sender sender, final long... nonce) {
      actions.add(
          () -> {
            lastExpectedDropped = droppedForSender(sender, nonce);
            assertExpectedDropped(dropped, lastExpectedDropped);
          });
      return this;
    }

    public Scenario expectedPrioritizedForSenders(
        final Sender sender1, final long nonce1, final Sender sender2, final Object... args) {
      actions.add(
          () -> {
            lastExpectedPrioritized = expectedForSenders(sender1, nonce1, sender2, args);
            assertExpectedPrioritized(prio, lastExpectedPrioritized);
          });
      return this;
    }

    public Scenario expectedPrioritizedForSenders() {
      actions.add(
          () -> {
            lastExpectedPrioritized = List.of();
            assertExpectedPrioritized(prio, lastExpectedPrioritized);
          });
      return this;
    }

    public Scenario expectedReadyForSenders(
        final Sender sender1, final long nonce1, final Sender sender2, final Object... args) {
      actions.add(
          () -> {
            lastExpectedReady = expectedForSenders(sender1, nonce1, sender2, args);
            assertExpectedReady(ready, lastExpectedReady);
          });
      return this;
    }

    public Scenario expectedReadyForSenders() {
      actions.add(
          () -> {
            lastExpectedReady = List.of();
            assertExpectedReady(ready, lastExpectedReady);
          });
      return this;
    }

    public Scenario expectedSparseForSenders(
        final Sender sender1, final long nonce1, final Sender sender2, final Object... args) {
      actions.add(
          () -> {
            lastExpectedSparse = expectedForSenders(sender1, nonce1, sender2, args);
            assertExpectedSparse(sparse, lastExpectedSparse);
          });
      return this;
    }

    public Scenario expectedSparseForSenders() {
      actions.add(
          () -> {
            lastExpectedSparse = List.of();
            assertExpectedSparse(sparse, lastExpectedSparse);
          });
      return this;
    }

    public Scenario expectedDroppedForSenders(
        final Sender sender1, final long nonce1, final Sender sender2, final Object... args) {
      actions.add(
          () -> {
            lastExpectedDropped = expectedForSenders(sender1, nonce1, sender2, args);
            assertExpectedDropped(dropped, lastExpectedDropped);
          });
      return this;
    }

    public Scenario expectedDroppedForSenders() {
      actions.add(
          () -> {
            lastExpectedDropped = List.of();
            assertExpectedDropped(dropped, lastExpectedDropped);
          });
      return this;
    }

    private void assertExpectedAll(
        final AbstractPrioritizedTransactions prioLayer,
        final Sender sender,
        final List<PendingTransaction> expected) {
      final List<PendingTransaction> allForSender = prioLayer.getAllBySender().get(sender.address);
      if (expected.isEmpty()) {
        assertThat(allForSender).describedAs("All").isNull();
      } else {
        assertThat(allForSender).describedAs("All").containsExactlyElementsOf(expected);
      }
    }

    private void assertExpectedPrioritized(
        final AbstractPrioritizedTransactions prioLayer, final List<PendingTransaction> expected) {
      assertThat(prioLayer.getByScore())
          .describedAs("Prioritized")
          .containsExactlyElementsOf(expected);
    }

    private void assertExpectedReady(
        final ReadyTransactions readyLayer, final List<PendingTransaction> expected) {
      assertThat(readyLayer.getBySender().stream().flatMap(List::stream).toList())
          .describedAs("Ready")
          .containsExactlyElementsOf(expected);
    }

    private void assertExpectedSparse(
        final SparseTransactions sparseLayer, final List<PendingTransaction> expected) {
      assertThat(sparseLayer.getBySender().stream().flatMap(List::stream).toList())
          .describedAs("Sparse")
          .containsExactlyElementsOf(expected);
    }

    private void assertExpectedDropped(
        final EvictCollectorLayer evictCollector, final List<PendingTransaction> expected) {
      assertThat(evictCollector.getEvictedTransactions())
          .describedAs("Dropped")
          .containsExactlyInAnyOrderElementsOf(expected);
    }

    private List<PendingTransaction> expectedForSenders(
        final Sender sender1, final long nonce1, final Sender sender2, final Object... args) {
      final List<PendingTransaction> expected = new ArrayList<>();
      expected.add(get(sender1, nonce1));
      final List<Object> sendersAndNonce = new ArrayList<>(Arrays.asList(args));
      sendersAndNonce.add(0, sender2);
      for (int i = 0; i < sendersAndNonce.size(); i = i + 2) {
        final Sender sender = (Sender) sendersAndNonce.get(i);
        final long nonce = (int) sendersAndNonce.get(i + 1);
        expected.add(get(sender, nonce));
      }
      return Collections.unmodifiableList(expected);
    }

    private List<PendingTransaction> expectedForSender(final Sender sender, final long... nonce) {
      return Arrays.stream(nonce).mapToObj(n -> get(sender, n)).toList();
    }

    private List<PendingTransaction> droppedForSender(final Sender sender, final long... nonce) {
      return Arrays.stream(nonce).mapToObj(n -> droppedTxsBySender.get(sender).get(n)).toList();
    }

    public Scenario expectedNextNonceForSenders(final Object... args) {
      for (int i = 0; i < args.length; i = i + 2) {
        final Sender sender = (Sender) args[i];
        final Integer nullableInt = (Integer) args[i + 1];
        final OptionalLong nonce =
            nullableInt == null ? OptionalLong.empty() : OptionalLong.of(nullableInt);
        actions.add(() -> assertThat(prio.getNextNonceFor(sender.address)).isEqualTo(nonce));
      }
      return this;
    }

    public Scenario removeForSender(final Sender sender, final long... nonce) {
      actions.add(
          () -> {
            Arrays.stream(nonce)
                .forEach(
                    n -> {
                      final var maybeLiveTx = getMaybe(sender, n);
                      final var pendingTx =
                          maybeLiveTx.orElseGet(() -> create(sender, EIP1559, NO_DELEGATIONS, n));
                      prio.remove(pendingTx, INVALIDATED);
                      maybeLiveTx.ifPresent(
                          liveTx -> {
                            notificationsChecker.addExpectedDropNotification(liveTx);
                            liveTxsBySender.get(sender).remove(liveTx.getNonce());
                            droppedTxsBySender.get(sender).put(liveTx.getNonce(), liveTx);
                          });
                    });
            handleDropped();
            notificationsChecker.assertExpectedNotifications();
          });
      return this;
    }

    public Scenario penalizeForSender(final Sender sender, final long... nonce) {
      actions.add(
          () ->
              Arrays.stream(nonce)
                  .forEach(
                      n -> {
                        final var senderTxs = prio.getAllFor(sender.address);
                        Arrays.stream(nonce)
                            .mapToObj(
                                n2 ->
                                    senderTxs.stream().filter(pt -> pt.getNonce() == n2).findAny())
                            .map(Optional::get)
                            .forEach(
                                penalizedTransaction ->
                                    prio.penalize(penalizedTransaction, TX_EVALUATION_TOO_LONG));
                      }));
      return this;
    }

    public Scenario expectedSelectedTransactions(final Object... args) {
      actions.add(
          () -> {
            List<PendingTransaction> expectedSelected = new ArrayList<>();
            for (int i = 0; i < args.length; i = i + 2) {
              final Sender sender = (Sender) args[i];
              final long nonce = (int) args[i + 1];
              expectedSelected.add(get(sender, nonce));
            }

            assertThat(prio.getBySender().stream().flatMap(List::stream).toList())
                .containsExactlyElementsOf(expectedSelected);
          });
      return this;
    }

    public Scenario hasSenderEnoughBalance(final Sender sender, final boolean hasEnoughBalance) {
      actions.add(
          () -> {
            reset(senderBalanceChecker);
            when(senderBalanceChecker.hasEnoughBalanceFor(
                    argThat(
                        pendingTransaction ->
                            pendingTransaction
                                .getTransaction()
                                .getSender()
                                .equals(sender.address))))
                .thenReturn(hasEnoughBalance);
          });
      return this;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  enum Sender {
    S1(false, 1),
    S2(false, 2),
    S3(false, 3),
    S4(false, 4),
    SP1(true, 1),
    SP2(true, 2);

    final KeyPair key;
    final Address address;
    final int gasFeeMultiplier;

    final boolean hasPriority;

    Sender(final boolean hasPriority, final int gasFeeMultiplier) {
      this.key = SIGNATURE_ALGORITHM.generateKeyPair();
      this.address = Util.publicKeyToAddress(key.getPublicKey());
      this.gasFeeMultiplier = gasFeeMultiplier;
      this.hasPriority = hasPriority;
    }

    static Sender getByAddress(final Address address) {
      return Arrays.stream(values()).filter(s -> s.address.equals(address)).findAny().get();
    }
  }

  static class NotificationsChecker {
    private final List<Transaction> collectedAddNotifications =
        Collections.synchronizedList(new ArrayList<>());
    private final List<Transaction> collectedDropNotifications =
        Collections.synchronizedList(new ArrayList<>());
    private final List<Transaction> expectedAddNotifications = new ArrayList<>();
    private final List<Transaction> expectedDropNotifications = new ArrayList<>();

    void collectAddNotification(final Transaction tx) {
      collectedAddNotifications.add(tx);
    }

    void collectDropNotification(final Transaction tx) {
      collectedDropNotifications.add(tx);
    }

    void addExpectedAddNotification(final PendingTransaction tx) {
      expectedAddNotifications.add(tx.getTransaction());
    }

    void addExpectedDropNotification(final PendingTransaction tx) {
      expectedDropNotifications.add(tx.getTransaction());
    }

    void assertExpectedNotifications() {
      assertAddNotifications(expectedAddNotifications);
      assertDropNotifications(expectedDropNotifications);
    }

    private void assertAddNotifications(final List<Transaction> expectedAddedTxs) {
      await()
          .untilAsserted(
              () ->
                  assertThat(collectedAddNotifications)
                      .describedAs("Added notifications")
                      .containsExactlyInAnyOrderElementsOf(expectedAddedTxs));
      collectedAddNotifications.clear();
      expectedAddNotifications.clear();
    }

    private void assertDropNotifications(final List<Transaction> expectedDroppedTxs) {
      await()
          .untilAsserted(
              () ->
                  assertThat(collectedDropNotifications)
                      .describedAs("Dropped notifications")
                      .containsExactlyInAnyOrderElementsOf(expectedDroppedTxs));
      collectedDropNotifications.clear();
      expectedDropNotifications.clear();
    }
  }

  record AuthorityAndNonce(Sender sender, long nonce) {
    static final AuthorityAndNonce[] NO_DELEGATIONS = new AuthorityAndNonce[0];

    static AuthorityAndNonce[] delegation(final Sender sender, final long nonce) {
      return new AuthorityAndNonce[] {new AuthorityAndNonce(sender, nonce)};
    }

    static CodeDelegation toCodeDelegation(final AuthorityAndNonce authorityAndNonce) {
      return createSignedCodeDelegation(
          BigInteger.ZERO, Address.ZERO, authorityAndNonce.nonce, authorityAndNonce.sender.key);
    }

    static List<CodeDelegation> toCodeDelegations(final AuthorityAndNonce[] authorityAndNonces) {
      return Arrays.stream(authorityAndNonces).map(AuthorityAndNonce::toCodeDelegation).toList();
    }
  }
}
