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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldState;
import static org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture.getGenesisConfigOptions;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.TransactionLocation;
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
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TraceReplayTransactionTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Address RECIPIENT =
      Address.fromHexString("0x2000000000000000000000000000000000000002");
  private static final Address BENEFICIARY =
      Address.fromHexString("0x3000000000000000000000000000000000000003");
  // Increments storage slot zero, so each transaction depends on the ones before it.
  private static final Bytes COUNTER = Bytes.fromHexString("0x60005460010160005500");

  private final Blockchain blockchain = mock(Blockchain.class);
  private final WorldStateArchive archive = mock(WorldStateArchive.class);
  private final KeyPair keys = SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private final Address sender = Address.extract(keys.getPublicKey());
  private ProtocolSchedule schedule;
  private BlockchainQueries queries;
  private TraceReplayTransaction method;
  private BlockHeader parent;
  private Block block;

  @BeforeEach
  void setup() {
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
    method = new TraceReplayTransaction(schedule, queries);
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
    setBlock(List.of(transaction(0), transaction(1), transaction(2)));
  }

  private MutableWorldState newState() {
    final MutableWorldState state = createInMemoryWorldState();
    final WorldUpdater updater = state.updater();
    updater.getOrCreate(sender).setBalance(Wei.of(1_000_000_000));
    updater.getOrCreate(RECIPIENT).setCode(COUNTER);
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
    when(blockchain.getBlockByHash(block.getHash())).thenReturn(Optional.of(block));
    when(blockchain.getBlockByNumber(1)).thenReturn(Optional.of(block));
    for (int i = 0; i < transactions.size(); i++) {
      when(blockchain.getTransactionLocation(transactions.get(i).getHash()))
          .thenReturn(Optional.of(new TransactionLocation(block.getHash(), i)));
    }
  }

  private JsonRpcResponse response(final Object... params) {
    return method.response(
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", method.getName(), params)));
  }

  private JsonNode replay(final int index, final List<String> types) {
    final JsonRpcResponse response =
        response(block.getBody().getTransactions().get(index).getHash(), types);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    return MAPPER.valueToTree(((JsonRpcSuccessResponse) response).getResult());
  }

  static Stream<List<String>> selections() {
    return Stream.of(
        List.of(),
        List.of("trace"),
        List.of("stateDiff"),
        List.of("vmTrace"),
        List.of("trace", "stateDiff"),
        List.of("trace", "vmTrace"),
        List.of("stateDiff", "vmTrace"),
        List.of("trace", "stateDiff", "vmTrace"));
  }

  @ParameterizedTest
  @MethodSource("selections")
  void shouldMatchBlockReplayForEveryTransactionAndSelection(final List<String> types) {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(1L);
    final DeterministicEthScheduler scheduler = new DeterministicEthScheduler();
    try {
      final JsonRpcResponse blockResponse =
          new TraceReplayBlockTransactions(schedule, queries, new NoOpMetricsSystem(), scheduler)
              .response(
                  new JsonRpcRequestContext(
                      new JsonRpcRequest(
                          "2.0", "trace_replayBlockTransactions", new Object[] {"0x1", types})));
      assertThat(blockResponse).isInstanceOf(JsonRpcSuccessResponse.class);
      final JsonNode envelopes =
          MAPPER.valueToTree(((JsonRpcSuccessResponse) blockResponse).getResult());
      assertThat(envelopes).hasSize(3);
      for (int i = 0; i < 3; i++) {
        assertThat(replay(i, types)).isEqualTo(envelopes.get(i));
      }
    } finally {
      scheduler.stop();
    }
  }

  @Test
  void shouldExecuteOnTopOfPrecedingTransactions() {
    final JsonNode diff = replay(2, List.of("stateDiff")).get("stateDiff");
    assertThat(diff.get(sender.toHexString()).at("/nonce/*/from").asText()).isEqualTo("0x2");
    assertThat(diff.get(sender.toHexString()).at("/nonce/*/to").asText()).isEqualTo("0x3");
    final JsonNode slot =
        diff.get(RECIPIENT.toHexString()).get("storage").get(Bytes32.ZERO.toHexString());
    assertThat(Bytes32.fromHexString(slot.at("/*/from").asText()).toUnsignedBigInteger())
        .isEqualTo(BigInteger.TWO);
    assertThat(Bytes32.fromHexString(slot.at("/*/to").asText()).toUnsignedBigInteger())
        .isEqualTo(BigInteger.valueOf(3));
  }

  @Test
  void shouldReturnNullForUnknownTransaction() {
    final JsonRpcResponse response = response(Hash.ZERO, List.of("trace"));
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    assertThat(((JsonRpcSuccessResponse) response).getResult()).isNull();
  }

  @Test
  void shouldErrorWhenParentStateIsUnavailable() {
    when(archive.getWorldState(any())).thenReturn(Optional.empty());
    assertError(
        response(block.getBody().getTransactions().get(1).getHash(), List.of("trace")),
        RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
  }

  @Test
  void shouldErrorWhenBlockBodyIsUnavailable() {
    when(blockchain.getBlockByHash(block.getHash())).thenReturn(Optional.empty());
    assertError(
        response(block.getBody().getTransactions().get(1).getHash(), List.of("trace")),
        RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
  }

  @Test
  void shouldErrorWhenTrieNodesAreMissing() {
    when(archive.getWorldState(any()))
        .thenAnswer(
            invocation -> {
              final MutableWorldState state = spy(newState());
              doThrow(new MerkleTrieException("missing node")).when(state).updater();
              return Optional.of(state);
            });
    assertError(
        response(block.getBody().getTransactions().get(1).getHash(), List.of("trace")),
        RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
  }

  @Test
  void shouldReportReplayFailureAsInternalError() {
    final Hash hash = block.getBody().getTransactions().get(1).getHash();
    when(blockchain.getTransactionLocation(hash))
        .thenReturn(Optional.of(new TransactionLocation(block.getHash(), 5)));
    assertError(response(hash, List.of("trace")), RpcErrorType.INTERNAL_ERROR);
  }

  @Test
  void shouldReportInvalidPrecedingTransactionAsInternalError() {
    setBlock(List.of(transaction(0), transaction(5), transaction(1)));
    assertError(
        response(block.getBody().getTransactions().get(2).getHash(), List.of()),
        RpcErrorType.INTERNAL_ERROR);
  }

  @Test
  void shouldReportInvalidTargetTransactionAsInternalError() {
    setBlock(List.of(transaction(0), transaction(5)));
    assertError(
        response(block.getBody().getTransactions().get(1).getHash(), List.of()),
        RpcErrorType.INTERNAL_ERROR);
  }

  @Test
  void shouldRejectInvalidParameters() {
    assertThatThrownBy(() -> response("0x1234", List.of("trace")))
        .isInstanceOfSatisfying(
            InvalidJsonRpcParameters.class,
            e ->
                assertThat(e.getRpcErrorType())
                    .isEqualTo(RpcErrorType.INVALID_TRANSACTION_HASH_PARAMS));
    assertThatThrownBy(() -> response(Hash.ZERO, List.of("bla")))
        .isInstanceOfSatisfying(
            InvalidJsonRpcParameters.class,
            e -> assertThat(e.getRpcErrorType()).isEqualTo(RpcErrorType.INVALID_TRACE_TYPE_PARAMS));
  }

  private void assertError(final JsonRpcResponse response, final RpcErrorType expected) {
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType()).isEqualTo(expected);
  }
}
