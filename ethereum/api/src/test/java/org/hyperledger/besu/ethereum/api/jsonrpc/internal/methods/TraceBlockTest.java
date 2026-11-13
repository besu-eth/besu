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
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldState;
import static org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture.getGenesisConfigOptions;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TraceBlockTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Address RECIPIENT =
      Address.fromHexString("0x2000000000000000000000000000000000000002");
  private static final Address BENEFICIARY =
      Address.fromHexString("0x3000000000000000000000000000000000000003");

  private final Blockchain blockchain = mock(Blockchain.class);
  private final WorldStateArchive archive = mock(WorldStateArchive.class);
  private final KeyPair keys = SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private final Address sender = Address.extract(keys.getPublicKey());
  private final DeterministicEthScheduler scheduler = new DeterministicEthScheduler();

  private ProtocolSchedule schedule;
  private BlockchainQueries queries;
  private TraceBlock method;
  private BlockHeader parent;
  private Block block;
  private Transaction transaction;

  @BeforeEach
  void setUp() {
    schedule =
        MainnetProtocolSchedule.fromConfig(
            getGenesisConfigOptions("/prague_all_milestones_zero.json"),
            MiningConfiguration.newDefault(),
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    queries =
        new BlockchainQueries(schedule, blockchain, archive, MiningConfiguration.newDefault());
    method = new TraceBlock(schedule, queries, new NoOpMetricsSystem(), scheduler);
    parent =
        new BlockHeaderTestFixture()
            .number(0)
            .timestamp(1)
            .gasLimit(30_000_000)
            .baseFeePerGas(Wei.ONE)
            .excessBlobGas(BlobGas.ZERO)
            .blobGasUsed(0L)
            .buildHeader();
    when(blockchain.getBlockHeader(parent.getHash())).thenReturn(Optional.of(parent));
    when(archive.getWorldState(any())).thenAnswer(invocation -> Optional.of(newState()));
    transaction = transaction(0);
    setBlock(List.of(transaction));
  }

  @AfterEach
  void tearDown() {
    scheduler.stop();
  }

  @Test
  void shouldReturnPrunedHistoryWhenParentStateIsUnavailableAndHeadStateIsAvailable() {
    unavailableParentState(true);

    final JsonRpcResponse response = response("0x1");

    assertError(response, RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
    assertThat(RpcErrorType.PRUNED_HISTORY_UNAVAILABLE.getCode()).isEqualTo(4444);
  }

  @Test
  void shouldReturnWorldStateUnavailableWhenHeadStateIsAlsoUnavailable() {
    unavailableParentState(false);

    assertError(response("0x1"), RpcErrorType.WORLD_STATE_UNAVAILABLE);
    assertThat(RpcErrorType.WORLD_STATE_UNAVAILABLE.getCode()).isEqualTo(-32000);
  }

  @Test
  void shouldTraceBlockWhenParentStateIsAvailable() {
    final JsonNode result = successResult(response("0x1"));

    assertThat(result).isNotEmpty();
    assertThat(result.toString()).contains(transaction.getHash().toHexString());
  }

  @Test
  void shouldReturnEmptyTraceForGenesisBlock() {
    final JsonNode result = successResult(response("0x0"));

    assertThat(result).isEmpty();
  }

  private void unavailableParentState(final boolean headStateAvailable) {
    when(archive.getWorldState(any())).thenReturn(Optional.empty());
    final BlockHeader head =
        new BlockHeaderTestFixture().number(10).stateRoot(Hash.ZERO).buildHeader();
    when(blockchain.getChainHeadHeader()).thenReturn(head);
    when(archive.isWorldStateAvailable(head.getStateRoot(), head.getHash()))
        .thenReturn(headStateAvailable);
  }

  private JsonNode successResult(final JsonRpcResponse response) {
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final Object result = ((JsonRpcSuccessResponse) response).getResult();
    assertThat(result).isInstanceOf(ArrayNode.class);
    return MAPPER.valueToTree(result);
  }

  private static void assertError(final JsonRpcResponse response, final RpcErrorType expected) {
    assertThat(response)
        .as("block with transactions and no parent state")
        .isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType()).isEqualTo(expected);
  }

  private JsonRpcResponse response(final String blockParameter) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "trace_block", new Object[] {blockParameter})));
  }

  private MutableWorldState newState() {
    final MutableWorldState state = createInMemoryWorldState();
    final WorldUpdater updater = state.updater();
    updater.getOrCreate(sender).setBalance(Wei.of(1_000_000_000));
    updater.commit();
    return state;
  }

  private Transaction transaction(final int nonce) {
    return new TransactionTestFixture()
        .sender(sender)
        .nonce(nonce)
        .to(Optional.of(RECIPIENT))
        .gasLimit(100_000)
        .gasPrice(Wei.of(2))
        .value(Wei.of(7))
        .chainId(Optional.of(BigInteger.valueOf(20211)))
        .createTransaction(keys);
  }

  private void setBlock(final List<Transaction> transactions) {
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .parentHash(parent.getHash())
            .number(1)
            .timestamp(2)
            .gasLimit(30_000_000)
            .coinbase(BENEFICIARY)
            .baseFeePerGas(Wei.ONE)
            .parentBeaconBlockRoot(Optional.of(Bytes32.fromHexString("0x1234")))
            .excessBlobGas(BlobGas.ZERO)
            .blobGasUsed(0L)
            .buildHeader();
    block = new Block(header, new BlockBody(transactions, List.of()));
    when(blockchain.getBlockByNumber(1)).thenReturn(Optional.of(block));
  }
}
