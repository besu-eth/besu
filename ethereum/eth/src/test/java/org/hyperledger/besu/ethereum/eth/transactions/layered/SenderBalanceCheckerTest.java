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
package org.hyperledger.besu.ethereum.eth.transactions.layered;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SenderBalanceCheckerTest extends BaseTransactionPoolTest {

  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
  private final GasCalculator gasCalculator = mock(GasCalculator.class);
  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final MutableBlockchain blockchain = mock(MutableBlockchain.class);
  private final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
  private final MutableWorldState worldState = mock(MutableWorldState.class);
  private final BlockHeader chainHeadHeader = new BlockHeaderTestFixture().buildHeader();

  private SenderBalanceChecker balanceChecker;

  @BeforeEach
  void setup() {
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(blockchain.getChainHeadHeader()).thenReturn(chainHeadHeader);
    when(worldStateArchive.getWorldState(any())).thenReturn(Optional.of(worldState));
    when(protocolSchedule.getByBlockHeader(chainHeadHeader)).thenReturn(protocolSpec);
    when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);
    balanceChecker = new SenderBalanceChecker.WorldStateChecker(protocolSchedule, protocolContext);
  }

  private void setSenderBalance(final Wei balance) {
    final Account account = mock(Account.class);
    when(account.getBalance()).thenReturn(balance);
    when(worldState.get(SENDER1)).thenReturn(account);
  }

  @Test
  public void zeroBalanceSenderHasEnoughBalanceForZeroUpfrontCostTransaction() {
    // on free gas networks sender accounts commonly have zero balance,
    // and a zero gas price, zero value transaction has zero upfront cost
    setSenderBalance(Wei.ZERO);

    final var zeroCostTx = createTransaction(TransactionType.FRONTIER, 0, Wei.ZERO, 0, null, KEYS1);

    assertThat(balanceChecker.hasEnoughBalanceFor(createRemotePendingTransaction(zeroCostTx)))
        .isTrue();
  }

  @Test
  public void zeroBalanceSenderHasNotEnoughBalanceForPayingTransaction() {
    setSenderBalance(Wei.ZERO);

    final var payingTx = createTransaction(TransactionType.FRONTIER, 0, Wei.of(10), 0, null, KEYS1);

    assertThat(balanceChecker.hasEnoughBalanceFor(createRemotePendingTransaction(payingTx)))
        .isFalse();
  }

  @Test
  public void senderWithEnoughBalanceCanPayUpfrontCost() {
    final var payingTx = createTransaction(TransactionType.FRONTIER, 0, Wei.of(10), 0, null, KEYS1);
    setSenderBalance(payingTx.getUpfrontCost(0L));

    assertThat(balanceChecker.hasEnoughBalanceFor(createRemotePendingTransaction(payingTx)))
        .isTrue();
  }

  @Test
  public void sequentialTransactionsDepleteTheCachedSenderBalance() {
    final var payingTx0 =
        createTransaction(TransactionType.FRONTIER, 0, Wei.of(10), 0, null, KEYS1);
    final var payingTx1 =
        createTransaction(TransactionType.FRONTIER, 1, Wei.of(10), 0, null, KEYS1);
    // enough balance for the first transaction only
    setSenderBalance(payingTx0.getUpfrontCost(0L));

    assertThat(balanceChecker.hasEnoughBalanceFor(createRemotePendingTransaction(payingTx0)))
        .isTrue();
    assertThat(balanceChecker.hasEnoughBalanceFor(createRemotePendingTransaction(payingTx1)))
        .isFalse();
  }
}
