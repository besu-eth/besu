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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationFactory;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.SignUtil;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.crypto.RawTransaction;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.tx.gas.DefaultGasProvider;
import org.web3j.utils.Numeric;

public class ExtendTransactionValidatorPluginTest extends AcceptanceTestBase {
  private BesuNode minerNode, minerNode2, minerNode3;
  private BesuNode extraValidatorNode;

  @BeforeEach
  public void setUp() throws Exception {
    minerNode =
        besu.createQbftNode(
            "miner",
            b ->
                b.genesisConfigProvider(
                    GenesisConfigurationFactory::createQbftLondonGenesisConfig));
    minerNode2 =
        besu.createQbftNode(
            "miner2",
            b ->
                b.genesisConfigProvider(
                    GenesisConfigurationFactory::createQbftLondonGenesisConfig));
    minerNode3 =
        besu.createQbftNode(
            "miner3",
            b ->
                b.genesisConfigProvider(
                    GenesisConfigurationFactory::createQbftLondonGenesisConfig));
    extraValidatorNode =
        besu.createQbftPluginsNode(
            "node1",
            Collections.singletonList("testPlugins"),
            PluginConfiguration.DEFAULT,
            Collections.singletonList("--plugin-tx-validator-test-enabled=true"),
            "DEBUG");
    cluster.start(minerNode, extraValidatorNode, minerNode2, minerNode3);

    minerNode.awaitPeerDiscovery(net.awaitPeerCount(3));
    extraValidatorNode.awaitPeerDiscovery(net.awaitPeerCount(3));
  }

  @Test
  public void nonFrontierTransactionsAreNotAccepted() {

    final Account sender = accounts.getPrimaryBenefactor();
    final Account recipient = accounts.createAccount("account-two");

    final RawTransaction eip1559Tx =
        RawTransaction.createEtherTransaction(
            4L,
            sender.getNextNonce(),
            DefaultGasProvider.GAS_LIMIT,
            recipient.getAddress(),
            BigInteger.ONE,
            DefaultGasProvider.GAS_PRICE,
            DefaultGasProvider.GAS_PRICE);

    final String rawSigned =
        Numeric.toHexString(
            SignUtil.signTransaction(eip1559Tx, sender, new SECP256K1(), Optional.empty()));

    assertThatThrownBy(
            () -> extraValidatorNode.execute(ethTransactions.sendRawTransaction(rawSigned)))
        .hasMessage("Plugin has marked the transaction as invalid");
  }

  @Test
  public void blockWithNonFrontierTransactionsIsNotAccepted() {

    final Account sender = accounts.getPrimaryBenefactor();
    final Account recipient = accounts.createAccount("account-two");

    final RawTransaction eip1559Tx =
        RawTransaction.createEtherTransaction(
            4L,
            sender.getNextNonce(),
            DefaultGasProvider.GAS_LIMIT,
            recipient.getAddress(),
            BigInteger.ONE,
            DefaultGasProvider.GAS_PRICE,
            DefaultGasProvider.GAS_PRICE);

    final String rawSigned =
        Numeric.toHexString(
            SignUtil.signTransaction(eip1559Tx, sender, new SECP256K1(), Optional.empty()));

    // London activates at block 2; wait for it before submitting an EIP-1559 tx
    waitForBlockHeight(minerNode, 2);

    final String eip1559TxHash = minerNode.execute(ethTransactions.sendRawTransaction(rawSigned));

    minerNode.verify(eth.expectSuccessfulTransactionReceipt(eip1559TxHash));

    await()
        .until(
            () -> {
              final var badBlocks = extraValidatorNode.execute(debug.getBadBlocks());
              if (badBlocks.isEmpty()) {
                return false;
              }
              final EthBlock.TransactionObject badBlockTx =
                  (EthBlock.TransactionObject)
                      badBlocks.get(0).block().getTransactions().get(0).get();
              assertThat(badBlockTx.get().getHash()).isEqualTo(eip1559TxHash);
              return true;
            });
  }
}
