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
package org.hyperledger.besu.tests.acceptance.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.WaitUtils;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeConfigurationBuilder;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationFactory;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

public class DebugGetModifiedAccountsAcceptanceTest extends AcceptanceTestBase {

  private BesuNode node;
  private Account recipient;

  @BeforeEach
  public void setUpNode() throws Exception {
    node =
        besu.create(
            new BesuNodeConfigurationBuilder()
                .name("debugModifiedAccounts")
                .jsonRpcEnabled()
                .jsonRpcDebug()
                .miningEnabled()
                .devMode(false)
                .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
                .genesisConfigProvider(GenesisConfigurationFactory::createQbftGenesisConfig)
                .build());
    cluster.start(node);
  }

  @AfterEach
  public void tearDownNode() {
    if (node != null) {
      cluster.stopNode(node);
    }
  }

  @Test
  public void shouldReturnModifiedAccountsByNumberAndHash() {
    recipient = accounts.createAccount("modified-recipient");
    final Hash transactionHash = node.execute(accountTransactions.createTransfer(recipient, 1));

    final TransactionReceipt receipt = waitForTransactionReceipt(transactionHash);
    final BigInteger blockNumber = receipt.getBlockNumber();
    final BigInteger parentBlockNumber = blockNumber.subtract(BigInteger.ONE);
    final EthBlock.Block block =
        node.execute(ethTransactions.block(DefaultBlockParameter.valueOf(blockNumber)));
    final EthBlock.Block parentBlock =
        node.execute(ethTransactions.block(DefaultBlockParameter.valueOf(parentBlockNumber)));

    assertModifiedAccounts(
        node.execute(debug.getModifiedAccountsByNumber(List.of(quantity(blockNumber)))));
    assertModifiedAccounts(
        node.execute(
            debug.getModifiedAccountsByNumber(
                List.of(quantity(parentBlockNumber), quantity(blockNumber)))));
    assertModifiedAccounts(node.execute(debug.getModifiedAccountsByHash(List.of(block.getHash()))));
    assertModifiedAccounts(
        node.execute(
            debug.getModifiedAccountsByHash(List.of(parentBlock.getHash(), block.getHash()))));
  }

  private TransactionReceipt waitForTransactionReceipt(final Hash transactionHash) {
    final AtomicReference<Optional<TransactionReceipt>> maybeReceipt =
        new AtomicReference<>(Optional.empty());
    WaitUtils.waitFor(
        120,
        () -> {
          maybeReceipt.set(
              node.execute(ethTransactions.getTransactionReceipt(transactionHash.toHexString())));
          assertThat(maybeReceipt.get()).isPresent();
        });
    return maybeReceipt.get().orElseThrow();
  }

  private void assertModifiedAccounts(final List<String> result) {
    assertThat(result)
        .containsExactlyInAnyOrder(
            // Bonsai trie logs include the block reward credit to the mining coinbase account.
            recipientAddress(), senderAddress(), node.getAddress().toHexString());
  }

  private String recipientAddress() {
    return recipient.getAddress();
  }

  private String senderAddress() {
    return accounts.getPrimaryBenefactor().getAddress();
  }

  private String quantity(final BigInteger value) {
    return "0x" + value.toString(16);
  }
}
