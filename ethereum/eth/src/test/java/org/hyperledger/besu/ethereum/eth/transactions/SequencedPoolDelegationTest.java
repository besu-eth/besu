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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.TransactionTestFixture.createSignedCodeDelegation;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.BaseFeePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.testutil.TestClock;

import java.math.BigInteger;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.junit.jupiter.api.Test;

/**
 * The sequenced pool is meant for gas-free networks, where the fundability of the queued
 * transactions is not a concern, so the limits on the pending transactions of delegated accounts
 * must not apply to it.
 */
public class SequencedPoolDelegationTest extends AbstractTransactionPoolTestBase {

  private static final Address DELEGATION_TARGET =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");

  @Override
  protected PendingTransactions createPendingTransactions(
      final TransactionPoolConfiguration poolConfig,
      final BiFunction<PendingTransaction, PendingTransaction, Boolean>
          transactionReplacementTester) {
    return new BaseFeePendingTransactionsSorter(
        poolConfig,
        TestClock.system(ZoneId.systemDefault()),
        metricsSystem,
        protocolContext.getBlockchain()::getChainHeadHeader,
        transactionEventNotifier.newEmitter());
  }

  @Override
  protected Transaction createTransaction(final int nonce, final Wei maxPrice) {
    return createTransactionBaseFeeMarket(nonce, maxPrice);
  }

  @Override
  protected TransactionTestFixture createBaseTransaction(final int nonce) {
    return createBaseTransactionBaseFeeMarket(nonce);
  }

  @Override
  protected ExecutionContextTestFixture createExecutionContextTestFixture() {
    return createExecutionContextTestFixtureBaseFeeMarket();
  }

  @Override
  protected FeeMarket getFeeMarket() {
    return FeeMarket.london(0L, Optional.of(BASE_FEE_FLOOR));
  }

  @Override
  protected Block appendBlock(
      final Difficulty difficulty,
      final BlockHeader parentBlock,
      final Transaction... transactionsToAdd) {
    return appendBlockBaseFeeMarket(difficulty, parentBlock, transactionsToAdd);
  }

  @Test
  public void sequencedPoolDoesNotRequireDelegationLimits() {
    assertThat(transactionPool.pendingTransactionsImplementation())
        .isEqualTo(BaseFeePendingTransactionsSorter.class);
  }

  @Test
  public void selfDelegatingSenderIsNotLimitedToTheNextNonce() {
    givenAllTransactionsAreValid();
    final KeyPair sender = SignatureAlgorithmFactory.getInstance().generateKeyPair();

    // the authorization nonce (1) is consumed by the delegation, and a layered pool would only
    // accept the nonce 2 after it, but here the whole queue (up to the future-nonce limit of the
    // pool) is accepted
    final Transaction delegationTx =
        createBaseTransactionBaseFeeMarket(0)
            .type(TransactionType.DELEGATE_CODE)
            .gasLimit(100_000L)
            .to(Optional.of(DELEGATION_TARGET))
            .codeDelegations(
                List.of(createSignedCodeDelegation(BigInteger.ZERO, DELEGATION_TARGET, 1L, sender)))
            .createTransaction(sender);
    transactionPool.addRemoteTransactions(List.of(delegationTx));
    assertTransactionPending(delegationTx);

    for (int nonce = 1; nonce <= 4; nonce++) {
      final Transaction tx = createBaseTransactionBaseFeeMarket(nonce).createTransaction(sender);
      transactionPool.addRemoteTransactions(List.of(tx));
      assertTransactionPending(tx);
    }
    assertThat(transactionPool.trackedDelegationTransactionCount()).isZero();
  }
}
