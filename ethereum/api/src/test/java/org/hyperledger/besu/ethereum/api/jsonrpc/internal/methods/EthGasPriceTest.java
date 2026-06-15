/*
 * Copyright ConsenSys AG.
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
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
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EthGasPriceTest {
  private static final String JSON_RPC_VERSION = "2.0";
  private static final String ETH_METHOD = "eth_gasPrice";
  private static final long DEFAULT_BLOCK_GAS_LIMIT = 100_000;
  private static final long DEFAULT_BLOCK_GAS_USED = 21_000;
  private static final Wei DEFAULT_MIN_GAS_PRICE = Wei.of(1_000);
  private static final Wei DEFAULT_BASE_FEE = Wei.of(100_000);

  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private Blockchain blockchain;
  private EthGasPrice method;
  private MiningConfiguration miningConfiguration;

  @BeforeEach
  public void setUp() {
    ApiConfiguration apiConfig = createDefaultApiConfiguration();
    miningConfiguration =
        MiningConfiguration.newDefault().setMinTransactionGasPrice(DEFAULT_MIN_GAS_PRICE);
    method = createEthGasPriceMethod(apiConfig);
  }

  @Test
  public void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  public void shouldReturnMinValueWhenNoTransactionsExist() {
    final JsonRpcRequestContext request = requestWithParams();
    final String expectedWei = "0x4d2"; // minGasPrice > nextBlockBaseFee
    miningConfiguration.setMinTransactionGasPrice(Wei.fromHexString(expectedWei));
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), expectedWei);

    mockBaseFeeMarket();

    mockBlockchain(1000, 0);

    final JsonRpcResponse actualResponse = method.response(request);
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);

    verify(blockchain).getChainHeadBlock();
    verify(blockchain, times(99)).getBlockByNumber(anyLong());
    verifyNoMoreInteractions(blockchain);
  }

  @Test
  public void shouldReturnBaseFeeAsMinValueOnGenesis() {
    final JsonRpcRequestContext request = requestWithParams();
    final String expectedWei =
        DEFAULT_BASE_FEE.toShortHexString(); // nextBlockBaseFee > minGasPrice
    miningConfiguration.setMinTransactionGasPrice(Wei.fromHexString(expectedWei));
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), expectedWei);

    mockBaseFeeMarket();

    mockBlockchain(0, 0);

    final JsonRpcResponse actualResponse = method.response(request);
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);

    verify(blockchain).getChainHeadBlock();
    verifyNoMoreInteractions(blockchain);
  }

  @Test
  public void shouldReturnMedianWhenTransactionsExist() {
    final JsonRpcRequestContext request = requestWithParams();
    final String expectedWei = "0x911c70"; // 9.51 mwei, gas prices are 9.01-10 mwei.
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), expectedWei);

    mockBaseFeeMarket();

    mockBlockchain(1000L, 1);

    final JsonRpcResponse actualResponse = method.response(request);
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);

    verify(blockchain).getChainHeadBlock();
    verify(blockchain, times(99)).getBlockByNumber(anyLong());
    verifyNoMoreInteractions(blockchain);
  }

  @Test
  public void shortChainQueriesAllBlocks() {
    final JsonRpcRequestContext request = requestWithParams();
    final String expectedWei = "0x64190"; // 410 kwei
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), expectedWei);

    mockBaseFeeMarket();

    mockBlockchain(80L, 1);

    final JsonRpcResponse actualResponse = method.response(request);
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);

    verify(blockchain).getChainHeadBlock();
    verify(blockchain, times(80)).getBlockByNumber(anyLong());
    verifyNoMoreInteractions(blockchain);
  }

  /**
   * Test to verify that the method returns the lower bound gas price when the lower bound parameter
   * is present.
   */
  @Test
  public void shouldReturnLimitedPriceWhenLowerBoundIsPresent() {
    long expectedGasPrice = 31_841 * 2;
    long lowerBoundCoefficient = 200;
    verifyGasPriceLimit(lowerBoundCoefficient, null, expectedGasPrice);
  }

  /**
   * Test to verify that the method returns the upper bound gas price when the upper bound parameter
   * is present.
   */
  @Test
  public void shouldReturnLimitedPriceWhenUpperBoundIsPresent() {
    long expectedGasPrice = (long) (31_841 * 1.5);
    long upperBoundCoefficient = 150;
    verifyGasPriceLimit(null, upperBoundCoefficient, expectedGasPrice);
  }

  /**
   * Test to verify that the method returns the actual gas price when the gas price is within the
   * bound range.
   */
  @Test
  public void shouldReturnActualGasPriceWhenWithinBoundRange() {
    long gasPrice = 60_000;
    long lowerBoundCoefficient = 120;
    long upperBoundCoefficient = 200;
    verifyGasPriceLimit(lowerBoundCoefficient, upperBoundCoefficient, gasPrice);
  }

  @Test
  public void whenGasPriceBlocksIsZeroReturnLowerBoundWithoutSamplingHistoricalBlocks() {
    mockBaseFeeMarket();

    // Only stub chain head header — getChainHeadBlock and getBlockByNumber must never be called
    final Block chainHeadBlock = createFakeBlock(1000L, 1, DEFAULT_BASE_FEE);
    when(blockchain.getChainHeadHeader()).thenReturn(chainHeadBlock.getHeader());

    method =
        createEthGasPriceMethod(ImmutableApiConfiguration.builder().gasPriceBlocks(0L).build());

    final JsonRpcResponse actualResponse = method.response(requestWithParams());

    assertThat(actualResponse).isInstanceOf(JsonRpcSuccessResponse.class);
    verify(blockchain).getChainHeadHeader();
    verify(blockchain, never()).getChainHeadBlock();
    verify(blockchain, never()).getBlockByNumber(anyLong());
    verifyNoMoreInteractions(blockchain);
  }

  private static Stream<Arguments> ethGasPriceAtGenesis() {
    return Stream.of(
        // base fee > min gas price
        Arguments.of(
            DEFAULT_MIN_GAS_PRICE.divide(2),
            Optional.of(DEFAULT_MIN_GAS_PRICE),
            DEFAULT_MIN_GAS_PRICE.subtract(
                DEFAULT_MIN_GAS_PRICE
                    .multiply(125)
                    .divide(1000)) // expect base fee for the 1st block
            ),
        // base fee < min gas price
        Arguments.of(
            DEFAULT_BASE_FEE.multiply(2),
            Optional.of(DEFAULT_BASE_FEE),
            DEFAULT_BASE_FEE.multiply(2)) // expect min gas price value
        ,

        // no base fee market
        Arguments.of(
            DEFAULT_MIN_GAS_PRICE,
            Optional.empty(),
            DEFAULT_MIN_GAS_PRICE // expect min gas price value
            ));
  }

  @ParameterizedTest
  @MethodSource("ethGasPriceAtGenesis")
  public void ethGasPriceAtGenesis(
      final Wei minGasPrice, final Optional<Wei> maybeGenesisBaseFee, final Wei expectedGasPrice) {
    miningConfiguration.setMinTransactionGasPrice(minGasPrice);

    if (maybeGenesisBaseFee.isPresent()) {
      mockBaseFeeMarket();
      mockBlockchain(maybeGenesisBaseFee.get(), 0, 0);
    } else {
      mockGasPriceMarket();
      mockBlockchain(null, 0, 0);
    }

    final JsonRpcRequestContext request = requestWithParams();
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            request.getRequest().getId(), expectedGasPrice.toShortHexString());

    final JsonRpcResponse actualResponse = method.response(request);
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);

    verify(blockchain).getChainHeadBlock();
    verifyNoMoreInteractions(blockchain);
  }

  /**
   * Helper method to verify the gas price limit.
   *
   * @param lowerBoundCoefficient The lower bound of the gas price.
   * @param upperBoundCoefficient The upper bound of the gas price.
   * @param expectedGasPrice The expected gas price.
   */
  private void verifyGasPriceLimit(
      final Long lowerBoundCoefficient,
      final Long upperBoundCoefficient,
      final long expectedGasPrice) {
    miningConfiguration.setMinTransactionGasPrice(Wei.of(100));

    mockBaseFeeMarket();

    var apiConfig =
        createApiConfiguration(
            Optional.ofNullable(lowerBoundCoefficient), Optional.ofNullable(upperBoundCoefficient));
    method = createEthGasPriceMethod(apiConfig);

    final JsonRpcRequestContext request = requestWithParams();
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            request.getRequest().getId(), Wei.of(expectedGasPrice).toShortHexString());

    final var chainHeadBlockNumber = 10L;
    mockBlockchain(chainHeadBlockNumber, 1);

    final JsonRpcResponse actualResponse = method.response(request);
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  private void mockBaseFeeMarket() {
    mockFeeMarket(FeeMarket.london(0));
  }

  private void mockGasPriceMarket() {
    mockFeeMarket(FeeMarket.legacy());
  }

  private void mockFeeMarket(final FeeMarket feeMarket) {
    final var protocolSpec = mock(ProtocolSpec.class);
    when(protocolSpec.getFeeMarket()).thenReturn(feeMarket);
    when(protocolSchedule.getForNextBlockHeader(any(), anyLong())).thenReturn(protocolSpec);
  }

  private void mockBlockchain(final long chainHeadBlockNumber, final int txsNum) {
    mockBlockchain(DEFAULT_BASE_FEE, chainHeadBlockNumber, txsNum);
  }

  private void mockBlockchain(
      final Wei genesisBaseFee, final long chainHeadBlockNumber, final int txsNum) {
    final var blocksByNumber = new HashMap<Long, Block>();

    final var genesisBlock = createFakeBlock(0, 0, genesisBaseFee);
    blocksByNumber.put(0L, genesisBlock);

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
      blocksByNumber.put(i, createFakeBlock(i, txsNum, baseFee));
    }

    when(blockchain.getChainHeadBlock()).thenReturn(blocksByNumber.get(chainHeadBlockNumber));
    if (chainHeadBlockNumber > 0) {
      when(blockchain.getBlockByNumber(anyLong()))
          .thenAnswer(
              invocation -> Optional.of(blocksByNumber.get(invocation.getArgument(0, Long.class))));
    }
    lenient()
        .when(blockchain.getChainHeadHeader())
        .thenReturn(blocksByNumber.get(chainHeadBlockNumber).getHeader());
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
            null),
        new BlockBody(
            IntStream.range(0, txsNum)
                .mapToObj(
                    i ->
                        new Transaction.Builder()
                            .nonce(i)
                            .gasPrice(Wei.of(height * 10_000L))
                            .gasLimit(gasUsed)
                            .value(Wei.ZERO)
                            .build())
                .toList(),
            List.of()));
  }

  private JsonRpcRequestContext requestWithParams(final Object... params) {
    return new JsonRpcRequestContext(new JsonRpcRequest(JSON_RPC_VERSION, ETH_METHOD, params));
  }

  private ApiConfiguration createDefaultApiConfiguration() {
    return createApiConfiguration(Optional.empty(), Optional.empty());
  }

  private ApiConfiguration createApiConfiguration(
      final Optional<Long> lowerBoundCoefficient, final Optional<Long> upperBoundCoefficient) {
    ImmutableApiConfiguration.Builder builder = ImmutableApiConfiguration.builder();

    lowerBoundCoefficient.ifPresent(
        value ->
            builder
                .isGasAndPriorityFeeLimitingEnabled(true)
                .lowerBoundGasAndPriorityFeeCoefficient(value));
    upperBoundCoefficient.ifPresent(
        value ->
            builder
                .isGasAndPriorityFeeLimitingEnabled(true)
                .upperBoundGasAndPriorityFeeCoefficient(value));

    return builder.build();
  }

  private EthGasPrice createEthGasPriceMethod(final ApiConfiguration apiConfig) {
    return new EthGasPrice(
        new BlockchainQueries(
            protocolSchedule,
            blockchain,
            null,
            Optional.empty(),
            Optional.empty(),
            apiConfig,
            miningConfiguration),
        apiConfig);
  }
}
