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
package org.hyperledger.besu.tests.acceptance.plugins;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.blockchain.Amount;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.SignUtil;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.crypto.RawTransaction;
import org.web3j.tx.gas.DefaultGasProvider;
import org.web3j.utils.Numeric;

public class TransactionPoolValidatorPluginTest extends AcceptanceTestBase {
  private BesuNode node;

  @BeforeEach
  public void setUp() throws Exception {
    node =
        besu.createQbftPluginsNode(
            "node",
            Collections.singletonList("testPlugins"),
            PluginConfiguration.DEFAULT,
            List.of(
                "--plugin-txpool-validator1-test-enabled=true",
                "--plugin-txpool-validator2-test-enabled=true"));
    cluster.start(node);
  }

  @Test
  public void transactionIsAccepted() {
    final Account recipient = accounts.createAccount("recipient");

    // txpool validator plugins accepts only Frontier txs without a payload
    final var transferTx =
        accountTransactions.createTransfer(recipient, Amount.wei(BigInteger.ONE));

    final var txHash = node.execute(transferTx);

    node.verify(eth.expectSuccessfulTransactionReceipt(txHash.getBytes().toHexString()));
  }

  @Test
  public void transactionIsRejectedByPlugin1() {
    // wait for London HF to activate
    cluster.verify(blockchain.reachesHeight(node, 2));

    final Account recipient = accounts.createAccount("recipient");

    // txpool validator plugins accepts only Frontier txs without a payload
    final var eip1559TransferTx =
        accountTransactions.create1559Transfer(recipient, 1, 4, Amount.wei(BigInteger.TEN));

    assertThatThrownBy(() -> node.execute(eip1559TransferTx))
        .hasMessageContaining("Only Frontier transactions are allowed here");
  }

  @Test
  public void transactionIsRejectedByPlugin2() {

    final Account recipient = accounts.createAccount("recipient");

    // txpool validator plugins accepts only Frontier txs without a payload
    final RawTransaction txWithPayload =
        RawTransaction.createTransaction(
            BigInteger.ZERO,
            DefaultGasProvider.GAS_PRICE,
            DefaultGasProvider.GAS_LIMIT,
            recipient.getAddress(),
            BigInteger.ZERO,
            "0x11");

    final String rawSigned =
        Numeric.toHexString(
            SignUtil.signTransaction(
                txWithPayload, accounts.getPrimaryBenefactor(), new SECP256K1(), Optional.empty()));

    assertThatThrownBy(() -> node.execute(ethTransactions.sendRawTransaction(rawSigned)))
        .hasMessageContaining("Transaction with payload not allowed here");
  }

  @Test
  public void transactionIsRejectedByBothPlugins() {
    // wait for London HF to activate
    cluster.verify(blockchain.reachesHeight(node, 2));

    final Account recipient = accounts.createAccount("recipient");

    // txpool validator plugins accepts only Frontier txs without a payload
    // order of validation is not fixed
    final RawTransaction txWithPayload =
        RawTransaction.createTransaction(
            4,
            BigInteger.ZERO,
            DefaultGasProvider.GAS_LIMIT,
            recipient.getAddress(),
            BigInteger.ZERO,
            "0x11",
            DefaultGasProvider.GAS_PRICE,
            DefaultGasProvider.GAS_PRICE);

    final String rawSigned =
        Numeric.toHexString(
            SignUtil.signTransaction(
                txWithPayload, accounts.getPrimaryBenefactor(), new SECP256K1(), Optional.empty()));

    assertThatThrownBy(() -> node.execute(ethTransactions.sendRawTransaction(rawSigned)))
        .extracting(Throwable::getMessage)
        .matches(
            msg ->
                msg.contains("Transaction with payload not allowed here")
                    || msg.contains("Only Frontier transactions are allowed here"));
  }
}
