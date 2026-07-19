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
package org.hyperledger.besu.tests.acceptance.bft.qbft;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.OptionalLong;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class QbftTransactionGasLimitAcceptanceTest extends AcceptanceTestBase {

  private static final long OSAKA_DEFAULT_TX_GAS_LIMIT = 16_777_216L; // 2^24
  private static final long HIGH_GAS_LIMIT = OSAKA_DEFAULT_TX_GAS_LIMIT + 1_000_000L;

  @Test
  public void transactionGasLimitZeroAllowsTransactionAboveDefaultCap() throws Exception {
    final List<BesuNode> validators = createValidators(OptionalLong.of(0L));
    cluster.start(validators.toArray(new BesuNode[0]));

    cluster.verify(blockchain.reachesHeight(validators.get(0), 1, 85));

    final Account sender = accounts.createAccount("account1");
    validators.get(0).execute(accountTransactions.createTransfer(sender, 50));
    cluster.verify(sender.balanceEquals(50));

    // pertxgaslimit=0 means unlimited; a tx with gas > Osaka default cap should succeed
    validators
        .get(1)
        .execute(accountTransactions.createTransferWithGasLimit(sender, 1, HIGH_GAS_LIMIT));
    cluster.verify(sender.balanceEquals(51));
  }

  @Test
  public void absentGasLimitRejectsTransactionAboveDefaultCap() throws Exception {
    final List<BesuNode> validators = createValidators(OptionalLong.empty());
    cluster.start(validators.toArray(new BesuNode[0]));

    cluster.verify(blockchain.reachesHeight(validators.get(0), 1, 85));

    final Account sender = accounts.createAccount("account1");
    validators.get(0).execute(accountTransactions.createTransfer(sender, 50));
    cluster.verify(sender.balanceEquals(50));

    // With no pertxgaslimit, the Osaka default cap (2^24) applies; a tx with gas above it
    // must be rejected
    assertThatThrownBy(
            () ->
                validators
                    .get(1)
                    .execute(
                        accountTransactions.createTransferWithGasLimit(sender, 1, HIGH_GAS_LIMIT)))
        .isInstanceOf(RuntimeException.class);
  }

  private List<BesuNode> createValidators(final OptionalLong transactionGasLimit)
      throws IOException {
    final String[] validatorNames = {"validator1", "validator2", "validator3", "validator4"};

    // Phase 1: create nodes using the standard factory (handles JSON-RPC, WebSocket, key pair,
    // and genesis config provider correctly)
    final BesuNode node1 =
        besu.createQbftNodeWithContractBasedValidators("validator1", validatorNames);
    final BesuNode node2 =
        besu.createQbftNodeWithContractBasedValidators("validator2", validatorNames);
    final BesuNode node3 =
        besu.createQbftNodeWithContractBasedValidators("validator3", validatorNames);
    final BesuNode node4 =
        besu.createQbftNodeWithContractBasedValidators("validator4", validatorNames);

    final List<BesuNode> allNodes = List.of(node1, node2, node3, node4);

    // Phase 2: generate shared genesis with all 4 validators by calling the provider that was
    // configured by the factory.  The genesisConfigProvider was set up with
    // createQbftValidatorContractGenesisConfig and will generate a genesis containing all 4
    // validator addresses in the contract.
    final String sharedGenesis =
        node1
            .getGenesisConfigProvider()
            .create(allNodes)
            .orElseThrow(() -> new RuntimeException("Failed to create genesis config"));

    // Phase 3: modify the genesis to add pertxgaslimit and activate Osaka
    final String modifiedGenesis = modifyGenesis(sharedGenesis, transactionGasLimit);

    // Phase 4: set the modified shared genesis on all nodes
    // When Cluster.start() runs, it will check getGenesisConfig().isEmpty() and skip calling
    // the provider, using our custom genesis instead
    allNodes.forEach(node -> node.setGenesisConfig(modifiedGenesis));

    return allNodes;
  }

  @SuppressWarnings("unchecked")
  private static String modifyGenesis(
      final String genesisJson, final OptionalLong transactionGasLimit) {
    try {
      final ObjectMapper objectMapper = new ObjectMapper();
      final var genesisMap =
          objectMapper.readValue(
              genesisJson, new TypeReference<java.util.Map<String, Object>>() {});

      final var configMap = (java.util.Map<String, Object>) genesisMap.get("config");
      final var qbftMap = (java.util.Map<String, Object>) configMap.get("qbft");

      // Complete fork ladder from Constantinople to London (block-based), then time-based forks
      // London is required so EIP-1559 baseFeePerGas is available in the genesis block header,
      // otherwise TransactionPriceCalculator.eip1559 throws NoSuchElementException
      configMap.put("constantinopleBlock", 0);
      configMap.put("petersburgBlock", 0);
      configMap.put("istanbulBlock", 0);
      configMap.put("berlinBlock", 0);
      configMap.put("londonBlock", 0);
      configMap.put("shanghaiTime", 0);
      configMap.put("cancunTime", 0);
      configMap.put("pragueTime", 0);
      configMap.put("osakaTime", 0);

      // Provide initial base fee for EIP-1559 (required when London+ is active at genesis)
      genesisMap.put("baseFeePerGas", "0x7");

      transactionGasLimit.ifPresent(cap -> qbftMap.put("pertxgaslimit", cap));

      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(genesisMap);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
