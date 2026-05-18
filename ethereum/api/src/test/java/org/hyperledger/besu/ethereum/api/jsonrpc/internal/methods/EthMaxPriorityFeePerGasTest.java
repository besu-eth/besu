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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EthMaxPriorityFeePerGasTest {
  private static final String JSON_RPC_VERSION = "2.0";
  private static final String ETH_METHOD =
      RpcMethod.ETH_GET_MAX_PRIORITY_FEE_PER_GAS.getMethodName();
  private static final Wei DEFAULT_MIN_PRIORITY_FEE_PER_GAS = Wei.ZERO;
  private static final long DEFAULT_BLOCK_GAS_LIMIT = 100_000;
  private static final long DEFAULT_BLOCK_GAS_USED = 21_000;
  private static final Wei DEFAULT_BASE_FEE = Wei.of(100_000);

  private EthMaxPriorityFeePerGas method;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private Blockchain blockchain;
  private MiningConfiguration miningConfiguration;

  @BeforeEach
  public void setUp() {
    miningConfiguration =
        MiningConfiguration.newDefault().setMinPriorityFeePerGas(DEFAULT_MIN_PRIORITY_FEE_PER_GAS);
    method = createEthMaxPriorityFeePerGasMethod();
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  public void whenNoTransactionsExistReturnMinPriorityFeePerGasPrice() {
    final JsonRpcRequestContext request = requestWithParams();
    final Wei expectedWei = Wei.ONE;
    miningConfiguration.setMinPriorityFeePerGas(expectedWei);
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), expectedWei.toShortHexString());

    mockBlockchain(10, 0);
    final JsonRpcResponse actualResponse = method.response(request);
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void whenTransactionsExistReturnMedianMaxPriorityFeePerGasPrice() {
    final JsonRpcRequestContext request = requestWithParams();
    final Wei expectedWei = Wei.of(51_000); // max priority fee per gas prices are 1000-100000 wei.
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), expectedWei.toShortHexString());

    mockBlockchain(100, 1);

    final JsonRpcResponse actualResponse = method.response(request);
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void returnMinPriorityFeePerGasWhenMedianValueIsLower() {
    final JsonRpcRequestContext request = requestWithParams();
    final Wei expectedWei = Wei.of(100_000);
    miningConfiguration.setMinPriorityFeePerGas(expectedWei);

    mockBlockchain(100, 1);

    // median value is 51000 wei, that is lower than the value this node is willing to accept,
    // so the configured min priority fee per gas is returned.
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), expectedWei.toShortHexString());

    final JsonRpcResponse actualResponse = method.response(request);
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void atGenesisReturnMinPriorityFeePerGas() {
    final JsonRpcRequestContext request = requestWithParams();
    final Wei expectedWei = Wei.ONE;
    miningConfiguration.setMinPriorityFeePerGas(expectedWei);
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), expectedWei.toShortHexString());

    mockBlockchain(0, 0);
    final JsonRpcResponse actualResponse = method.response(request);
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  private JsonRpcRequestContext requestWithParams(final Object... params) {
    return new JsonRpcRequestContext(new JsonRpcRequest(JSON_RPC_VERSION, ETH_METHOD, params));
  }

  private void mockBlockchain(final long chainHeadBlockNumber, final int txsNum) {
    final var genesisBaseFee = DEFAULT_BASE_FEE;
    final var blocksByNumber = new HashMap<Long, Block>();
    final var blocksByHash = new HashMap<Hash, Block>();

    final var genesisBlock = createFakeBlock(0, 0, genesisBaseFee);
    blocksByNumber.put(0L, genesisBlock);
    blocksByHash.put(genesisBlock.getHash(), genesisBlock);

    final var baseFeeMarket = FeeMarket.cancunDefault(0, Optional.empty());

    var baseFee = genesisBaseFee;
    for (long i = 1; i <= chainHeadBlockNumber; i++) {
      final var parentHeader = blocksByNumber.get(i - 1).getHeader();
      baseFee =
          baseFeeMarket.computeBaseFee(
              i,
              parentHeader.getBaseFee().get(),
              parentHeader.getGasUsed(),
              parentHeader.getGasLimit());
      final var block = createFakeBlock(i, txsNum, baseFee);
      blocksByNumber.put(i, block);
      blocksByHash.put(block.getHash(), block);
    }

    final Block chainHeadBlock = blocksByNumber.get(chainHeadBlockNumber);
    lenient().when(blockchain.getChainHeadHeader()).thenReturn(chainHeadBlock.getHeader());
    lenient().when(blockchain.observeBlockAdded(any())).thenReturn(0L);
    lenient()
        .when(blockchain.getBlockHeaders(anyLong(), anyInt()))
        .thenAnswer(
            invocation -> {
              final long start = invocation.getArgument(0, Long.class);
              final int count = invocation.getArgument(1, Integer.class);
              return IntStream.range(0, count)
                  .mapToObj(idx -> blocksByNumber.get(start + idx).getHeader())
                  .toList();
            });
    lenient()
        .when(blockchain.getBlockBody(any(Hash.class)))
        .thenAnswer(
            invocation -> {
              final Hash hash = invocation.getArgument(0, Hash.class);
              return Optional.ofNullable(blocksByHash.get(hash)).map(Block::getBody);
            });
  }

  private Block createFakeBlock(final long height, final int txsNum, final Wei baseFee) {
    return createFakeBlock(
        height, txsNum, baseFee, DEFAULT_BLOCK_GAS_LIMIT, DEFAULT_BLOCK_GAS_USED * txsNum);
  }

  private Block createFakeBlock(
      final long height,
      final int txsNum,
      final Wei baseFee,
      final long gasLimit,
      final long gasUsed) {
    return new Block(
        new BlockHeader(
            Hash.EMPTY,
            Hash.EMPTY_TRIE_HASH,
            Address.ZERO,
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY_TRIE_HASH,
            LogsBloomFilter.builder().build(),
            Difficulty.ONE,
            height,
            gasLimit,
            gasUsed,
            0,
            Bytes.EMPTY,
            baseFee,
            Bytes32.wrap(Hash.EMPTY.getBytes()),
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new MainnetBlockHeaderFunctions()),
        new BlockBody(
            IntStream.range(0, txsNum)
                .mapToObj(
                    i ->
                        new Transaction.Builder()
                            .chainId(BigInteger.ONE)
                            .type(TransactionType.EIP1559)
                            .nonce(i)
                            .maxFeePerGas(Wei.of(height * 10_000L))
                            .maxPriorityFeePerGas(Wei.of(height * 1_000L))
                            .gasLimit(gasUsed)
                            .value(Wei.ZERO)
                            .build())
                .toList(),
            List.of()));
  }

  private EthMaxPriorityFeePerGas createEthMaxPriorityFeePerGasMethod() {
    return new EthMaxPriorityFeePerGas(
        new BlockchainQueries(
            protocolSchedule,
            blockchain,
            null,
            Optional.empty(),
            Optional.empty(),
            ImmutableApiConfiguration.builder().build(),
            miningConfiguration));
  }
}
