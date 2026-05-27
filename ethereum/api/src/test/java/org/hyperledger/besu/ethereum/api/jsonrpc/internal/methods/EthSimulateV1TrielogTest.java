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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonBlockStateCallParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.SimulateV1Parameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockStateCallResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EthSimulateV1TrielogTest {

  private BonsaiWorldStateProvider archive;
  private BlockchainQueries blockchainQueries;
  private EthSimulateV1 method;

  protected final ProtocolSchedule protocolSchedule =
      MainnetProtocolSchedule.fromConfig(
          GenesisConfig.fromResource("/dev.json").getConfigOptions(),
          MiningConfiguration.MINING_DISABLED,
          new BadBlockManager(),
          false,
          BalConfiguration.DEFAULT,
          new NoOpMetricsSystem());

  protected final GenesisState genesisState =
      GenesisState.fromConfig(
          GenesisConfig.fromResource("/dev.json"), protocolSchedule, new CodeCache());

  protected final MutableBlockchain blockchain = createInMemoryBlockchain(genesisState.getBlock());

  protected final GenesisAccount senderAccount =
      GenesisConfig.fromResource("/dev.json")
          .streamAllocations()
          .filter(ga -> ga.privateKey() != null)
          .findFirst()
          .orElseThrow();

  @BeforeEach
  public void createStorage() {
    archive = createBonsaiInMemoryWorldStateArchive(blockchain);
    var ws = archive.getWorldState();
    genesisState.writeStateTo(ws);

    blockchainQueries =
        new BlockchainQueries(
            protocolSchedule, blockchain, archive, MiningConfiguration.MINING_DISABLED);

    method =
        new EthSimulateV1(
            null,
            blockchainQueries,
            protocolSchedule,
            new TransactionSimulator(
                blockchain,
                archive,
                protocolSchedule,
                MiningConfiguration.MINING_DISABLED,
                Long.MAX_VALUE),
            MiningConfiguration.MINING_DISABLED,
            ImmutableApiConfiguration.builder().build());
  }

  @Test
  public void shouldReturnTrielogWhenReturnTrieLogTrue() {
    Address testAddress = Address.fromHexString("0xdeadbeef");

    // Create call parameter for the transaction
    CallParameter callParameter = simpleSend(testAddress, Wei.of(1_337_000L));

    // Create block state call parameter
    JsonBlockStateCallParameter blockStateCall =
        new JsonBlockStateCallParameter(List.of(callParameter), null, null);

    // Create simulation parameter with returnTrieLog = true
    SimulateV1Parameter simulateV1Parameter =
        new SimulateV1Parameter(List.of(blockStateCall), false, false, false, true, false);

    JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "eth_simulateV1", new Object[] {simulateV1Parameter, "latest"}));

    JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    assertThat(response).isNotNull();
    @SuppressWarnings("unchecked")
    List<BlockStateCallResult> results = (List<BlockStateCallResult>) response.getResult();

    assertThat(results).hasSize(1);
    BlockStateCallResult result = results.get(0);

    // Verify that the call was successful
    assertThat(result.getTransactionProcessingResults()).hasSize(1);
    assertThat(result.getTransactionProcessingResults().get(0).getStatus()).isEqualTo("0x1");

    // Verify trielog
    assertThat(result.getTrieLog()).isPresent();
    String trieLogData = result.getTrieLog().get();
    assertThat(trieLogData.length()).isGreaterThan(0);

    // verify trielog contents:
    var trieLogAccountChanges =
        archive
            .getTrieLogManager()
            .getTrieLogFactory()
            .deserialize(Bytes.fromHexString(trieLogData).toArrayUnsafe())
            .getAccountChanges();

    var senderPrior = trieLogAccountChanges.get(senderAccount.address()).getPrior();
    var senderPost = trieLogAccountChanges.get(senderAccount.address()).getUpdated();
    assertThat(senderPrior.getBalance().subtract(senderPost.getBalance()))
        .isEqualTo(Wei.of(1_337_000L)); // zero base fee, balance delta should just be value xfer
    assertThat(trieLogAccountChanges.get(testAddress).getUpdated().getBalance())
        .isEqualTo(Wei.of(1_337_000L));
  }

  @Test
  public void shouldNotReturnTrielogWhenReturnTrieLogFalse() {
    Address testAddress = Address.fromHexString("0xdeadbeef");
    CallParameter callParameter = simpleSend(testAddress, Wei.fromEth(1L));

    JsonBlockStateCallParameter blockStateCall =
        new JsonBlockStateCallParameter(List.of(callParameter), null, null);

    // Create simulation parameter with returnTrieLog = false
    SimulateV1Parameter simulateV1Parameter =
        new SimulateV1Parameter(List.of(blockStateCall), false, false, false, false, false);

    JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "eth_simulateV1", new Object[] {simulateV1Parameter, "latest"}));

    JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    assertThat(response).isNotNull();
    @SuppressWarnings("unchecked")
    List<BlockStateCallResult> results = (List<BlockStateCallResult>) response.getResult();

    assertThat(results).hasSize(1);
    BlockStateCallResult result = results.get(0);

    // Verify trielog is empty when returnTrieLog is false
    assertThat(result.getTrieLog()).isEmpty();
  }

  @Test
  public void shouldReturnTrielogWithMultipleTransactions() {
    Address testAddress1 = Address.fromHexString("0xdeadbeef");
    Address testAddress2 = Address.fromHexString("0xcafebabe");

    // Create multiple call parameters that will modify state
    CallParameter callParameter1 = simpleSend(testAddress1, Wei.of(1337L));
    CallParameter callParameter2 = simpleSend(testAddress2, Wei.of(420L));

    JsonBlockStateCallParameter blockStateCall =
        new JsonBlockStateCallParameter(List.of(callParameter1, callParameter2), null, null);

    SimulateV1Parameter simulateV1Parameter =
        new SimulateV1Parameter(List.of(blockStateCall), false, false, false, true, false);

    JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "eth_simulateV1", new Object[] {simulateV1Parameter, "latest"}));

    JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    assertThat(response).isNotNull();
    @SuppressWarnings("unchecked")
    List<BlockStateCallResult> results = (List<BlockStateCallResult>) response.getResult();

    assertThat(results).hasSize(1);
    BlockStateCallResult result = results.get(0);

    // Verify trielog is present and not empty for multiple transactions
    assertThat(result.getTrieLog()).isPresent();
    String trieLogData = result.getTrieLog().get();
    assertThat(trieLogData.length()).isGreaterThan(0);

    // Verify both transactions were processed
    assertThat(result.getTransactionProcessingResults()).hasSize(2);
    assertThat(result.getTransactionProcessingResults().get(0).getStatus()).isEqualTo("0x1");
    assertThat(result.getTransactionProcessingResults().get(1).getStatus()).isEqualTo("0x1");

    // verify trielog contents:
    var trieLogAccountChanges =
        archive
            .getTrieLogManager()
            .getTrieLogFactory()
            .deserialize(Bytes.fromHexString(trieLogData).toArrayUnsafe())
            .getAccountChanges();

    var senderPrior = trieLogAccountChanges.get(senderAccount.address()).getPrior();
    var senderPost = trieLogAccountChanges.get(senderAccount.address()).getUpdated();
    assertThat(senderPrior.getBalance().subtract(senderPost.getBalance()))
        .isEqualTo(Wei.of(1_337L + 420L)); // zero base fee, balance delta should just be value xfer
    assertThat(trieLogAccountChanges.get(testAddress1).getUpdated().getBalance())
        .isEqualTo(Wei.of(1_337L));
    assertThat(trieLogAccountChanges.get(testAddress2).getUpdated().getBalance())
        .isEqualTo(Wei.of(420L));
  }

  private CallParameter simpleSend(final Address toAddress, final Wei value) {
    return ImmutableCallParameter.builder()
        .sender(senderAccount.address())
        .to(toAddress)
        .value(value)
        .gas(21_000L)
        .gasPrice(Wei.of(0L))
        .build();
  }
}
