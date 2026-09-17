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
import static org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher.BLOCKS_PER_BLOOM_CACHE;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher;
import org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher.CachingStatus;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AdminGenerateLogBloomCacheTest {

  private static final long HEAD = 0x1000L;

  @Mock private BlockchainQueries blockchainQueries;
  @Mock private TransactionLogBloomCacher transactionLogBloomCacher;
  @Captor private ArgumentCaptor<Long> fromBlock;
  @Captor private ArgumentCaptor<Long> toBlock;

  private AdminGenerateLogBloomCache method;

  @BeforeEach
  public void setup() {
    method = new AdminGenerateLogBloomCache(blockchainQueries);
  }

  @Test
  public void requestWithZeroParameters_NoCacher_returnsNull() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_generateLogBloomCache", new String[] {}));

    when(blockchainQueries.getTransactionLogBloomCacher()).thenReturn(Optional.empty());

    final JsonRpcResponse actualResponse = method.response(request);

    verifyNoMoreInteractions(blockchainQueries);

    assertThat(actualResponse).isInstanceOf(JsonRpcSuccessResponse.class);
    assertThat(((JsonRpcSuccessResponse) actualResponse).getResult()).isNull();
  }

  /**
   * With no arguments at all the bounds are constants -- a deliberately crossed (0, -1) that
   * requestCaching rejects -- so the chain head is never read. The head lookup in the method is
   * memoized precisely so that stays true; this test pins it.
   */
  @Test
  public void noArgumentsResolvesToRejectedBoundsWithoutReadingTheChainHead() {
    final CachingStatus expectedStatus = new CachingStatus();
    when(blockchainQueries.getTransactionLogBloomCacher())
        .thenReturn(Optional.of(transactionLogBloomCacher));
    when(transactionLogBloomCacher.requestCaching(fromBlock.capture(), toBlock.capture()))
        .thenReturn(expectedStatus);

    method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "admin_generateLogBloomCache", new String[] {})));

    assertThat(fromBlock.getValue()).isZero();
    assertThat(toBlock.getValue()).isEqualTo(-1L);
    verify(blockchainQueries, never()).headBlockNumber();
    verifyNoMoreInteractions(blockchainQueries);
  }

  static Stream<Arguments> blockParameterVectors() {
    return Stream.of(
        // params, expected start block, expected stop block
        arguments(List.of("earliest"), 0L, HEAD + 1),
        arguments(List.of("latest"), HEAD, HEAD + 1),
        arguments(List.of("pending"), HEAD, HEAD + 1),
        arguments(List.of("0x50"), 0x50L, HEAD + 1),
        arguments(List.of("earliest", "earliest"), 0L, 0L),
        arguments(List.of("latest", "earliest"), HEAD, 0L),
        arguments(List.of("pending", "earliest"), HEAD, 0L),
        arguments(List.of("0x50", "earliest"), 0x50L, 0L),
        arguments(List.of("earliest", "latest"), 0L, HEAD + 1),
        arguments(List.of("latest", "latest"), HEAD, HEAD + 1),
        arguments(List.of("pending", "latest"), HEAD, HEAD + 1),
        arguments(List.of("0x50", "latest"), 0x50L, HEAD + 1),
        arguments(List.of("earliest", "pending"), 0L, HEAD + 1),
        arguments(List.of("latest", "pending"), HEAD, HEAD + 1),
        arguments(List.of("pending", "pending"), HEAD, HEAD + 1),
        arguments(List.of("0x50", "pending"), 0x50L, HEAD + 1),
        arguments(List.of("earliest", "0x100"), 0L, 0x100L),
        arguments(List.of("latest", "0x100"), HEAD, 0x100L),
        arguments(List.of("pending", "0x100"), HEAD, 0x100L),
        arguments(List.of("0x50", "0x100"), 0x50L, 0x100L),
        arguments(List.of("earliest", "0x10"), 0L, 0x10L),
        arguments(List.of("latest", "0x10"), HEAD, 0x10L),
        arguments(List.of("pending", "0x10"), HEAD, 0x10L),
        arguments(List.of("0x50", "0x10"), 0x50L, 0x10L),
        // explicit block numbers beyond the head are clamped to it
        arguments(List.of("earliest", "0xffffffff"), 0L, HEAD + 1),
        arguments(List.of("0xffffffff", "0xffffffff"), HEAD, HEAD + 1),
        arguments(List.of("0x50", "0xffffffff"), 0x50L, HEAD + 1));
  }

  /**
   * Both bounds are clamped to the chain head, so `latest`, `pending`, an omitted stop block and an
   * out-of-range explicit number all resolve to the head rather than Long.MAX_VALUE. The stop bound
   * is exclusive, hence HEAD + 1; see {@link
   * #resolvedRangeIncludesTheSegmentContainingTheChainHead(java.util.List)}.
   *
   * <p>The no-arguments case is covered separately by {@link
   * #noArgumentsResolvesToRejectedBoundsWithoutReadingTheChainHead()}: it is the one form that
   * never reads the head, so including it here would leave an unused stub.
   */
  @ParameterizedTest(name = "{0} -> start {1}, stop {2}")
  @MethodSource("blockParameterVectors")
  public void resolvedBoundsAreClampedToChainHead(
      final List<String> params, final long expectedFromBlock, final long expectedToBlock) {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "admin_generateLogBloomCache", params.toArray(new String[0])));

    final CachingStatus expectedStatus = new CachingStatus();

    // lenient: `earliest, earliest` resolves to (0, 0) without consulting the head
    lenient().when(blockchainQueries.headBlockNumber()).thenReturn(HEAD);
    when(blockchainQueries.getTransactionLogBloomCacher())
        .thenReturn(Optional.of(transactionLogBloomCacher));
    when(transactionLogBloomCacher.requestCaching(fromBlock.capture(), toBlock.capture()))
        .thenReturn(expectedStatus);

    final JsonRpcResponse actualResponse = method.response(request);

    assertThat(actualResponse).isInstanceOf(JsonRpcSuccessResponse.class);
    assertThat(((JsonRpcSuccessResponse) actualResponse).getResult()).isSameAs(expectedStatus);
    assertThat(fromBlock.getValue()).isEqualTo(expectedFromBlock);
    assertThat(toBlock.getValue()).isEqualTo(expectedToBlock);
    // ignoreStubs: only headBlockNumber() and getTransactionLogBloomCacher() may be touched
    verifyNoMoreInteractions(ignoreStubs(blockchainQueries));
  }

  static Stream<Arguments> openEndedStopForms() {
    return Stream.of(
        arguments(List.of("earliest")),
        arguments(List.of("earliest", "latest")),
        arguments(List.of("earliest", "pending")),
        arguments(List.of("latest")),
        arguments(List.of("pending")),
        arguments(List.of("earliest", "0xffffffff")));
  }

  /**
   * The stop bound is consumed as an <em>exclusive</em> upper bound: {@code
   * TransactionLogBloomCacher.generateLogBloomCache} walks {@code for (blockNum = start; blockNum <
   * stop; blockNum += BLOCKS_PER_BLOOM_CACHE)}.
   *
   * <p>So resolving an open-ended stop to exactly the head drops the segment that <em>starts</em>
   * at the head, which is the segment holding the head block itself whenever the head sits on a
   * BLOCKS_PER_BLOOM_CACHE boundary. Every form meaning "up to the chain head" must therefore leave
   * the head block inside the half-open range [start, stop).
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("openEndedStopForms")
  public void resolvedRangeIncludesTheSegmentContainingTheChainHead(final List<String> params) {
    // a head sitting exactly on a segment boundary is the case that loses a segment
    final long boundaryHead = BLOCKS_PER_BLOOM_CACHE;

    final CachingStatus expectedStatus = new CachingStatus();
    when(blockchainQueries.headBlockNumber()).thenReturn(boundaryHead);
    when(blockchainQueries.getTransactionLogBloomCacher())
        .thenReturn(Optional.of(transactionLogBloomCacher));
    when(transactionLogBloomCacher.requestCaching(fromBlock.capture(), toBlock.capture()))
        .thenReturn(expectedStatus);

    method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "admin_generateLogBloomCache", params.toArray(new String[0]))));

    assertThat(toBlock.getValue())
        .as(
            "%s: the stop bound is exclusive, so it must exceed the head block %d for the head's "
                + "segment to be generated",
            params, boundaryHead)
        .isGreaterThan(boundaryHead);
    assertThat(fromBlock.getValue())
        .as("%s: the head block must be at or after the start bound", params)
        .isLessThanOrEqualTo(boundaryHead);
  }

  /** The defect: no combination of parameters may hand the cacher an unbounded stop block. */
  @Test
  public void noParameterCombinationProducesAnUnboundedStopBlock() {
    final String[] blockParams = {"earliest", "latest", "pending", "0x50", "0xffffffff"};

    for (final String start : blockParams) {
      for (final String stop : blockParams) {
        final CachingStatus expectedStatus = new CachingStatus();
        when(blockchainQueries.headBlockNumber()).thenReturn(HEAD);
        when(blockchainQueries.getTransactionLogBloomCacher())
            .thenReturn(Optional.of(transactionLogBloomCacher));
        when(transactionLogBloomCacher.requestCaching(fromBlock.capture(), toBlock.capture()))
            .thenReturn(expectedStatus);

        method.response(
            new JsonRpcRequestContext(
                new JsonRpcRequest(
                    "2.0", "admin_generateLogBloomCache", new String[] {start, stop})));

        assertThat(fromBlock.getValue())
            .as("start block for [%s, %s]", start, stop)
            .isLessThanOrEqualTo(HEAD);
        assertThat(toBlock.getValue())
            .as("stop block for [%s, %s]", start, stop)
            .isLessThanOrEqualTo(HEAD + 1);
      }
    }
  }
}
