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
package org.hyperledger.besu.ethereum.api.graphql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter.NormalBlockAdapter;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.chain.Blockchain;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import graphql.schema.DataFetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Regression coverage for HYB-07: {@code blocks(from,to)} previously accepted an unbounded {@code
 * to} (up to {@code Long.MAX_VALUE}) with no span cap and no liveness check, letting a single query
 * loop for an effectively unbounded number of iterations and pin a request-handling thread
 * indefinitely, uncancellable even after the caller disconnected.
 */
@ExtendWith(MockitoExtension.class)
class RangeBlockDataFetcherTest extends AbstractDataFetcherTest {

  private static final long CHAIN_HEAD = 100L;

  private final Blockchain blockchain = mock(Blockchain.class);

  @BeforeEach
  @Override
  public void before() {
    super.before();
    when(graphQLContext.get(GraphQLContextType.BLOCKCHAIN_QUERIES)).thenReturn(query);
    when(query.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(CHAIN_HEAD);
  }

  private DataFetcher<List<NormalBlockAdapter>> fetcherWithMaxRange(final long maxBlockRange) {
    return new GraphQLDataFetchers(supportedCapabilities, maxBlockRange).getRangeBlockDataFetcher();
  }

  private void stubBlockByNumberPresent() {
    when(query.blockByNumber(anyLong()))
        .thenReturn(Optional.of(new BlockWithMetadata<>(null, null, null, null, 0)));
  }

  @Test
  void rejectsFromBeyondTo() {
    when(environment.containsArgument("from")).thenReturn(true);
    when(environment.getArgument("from")).thenReturn(10L);
    when(environment.containsArgument("to")).thenReturn(true);
    when(environment.getArgument("to")).thenReturn(5L);

    assertThatThrownBy(() -> fetcherWithMaxRange(5000L).get(environment))
        .isInstanceOf(GraphQLException.class);
  }

  @Test
  void rejectsNegativeFrom() {
    when(environment.containsArgument("from")).thenReturn(true);
    when(environment.getArgument("from")).thenReturn(-1L);
    when(environment.containsArgument("to")).thenReturn(true);
    when(environment.getArgument("to")).thenReturn(10L);

    assertThatThrownBy(() -> fetcherWithMaxRange(5000L).get(environment))
        .isInstanceOf(GraphQLException.class);
  }

  @Test
  void rejectsSpanExceedingCapEvenFarBeyondChainHead() {
    // The reported attack: a `to` far beyond both the chain head and Long-range sanity
    // (Long.MAX_VALUE - 1) must be rejected outright by the span cap, without ever touching
    // the per-block loop - not "clamped then looped".
    when(environment.containsArgument("from")).thenReturn(true);
    when(environment.getArgument("from")).thenReturn(1_000_000_000_000L);
    when(environment.containsArgument("to")).thenReturn(true);
    when(environment.getArgument("to")).thenReturn(Long.MAX_VALUE - 1);

    assertThatThrownBy(() -> fetcherWithMaxRange(5000L).get(environment))
        .isInstanceOf(GraphQLException.class);
    verify(query, never()).blockByNumber(anyLong());
  }

  @Test
  void rejectsSpanExceedingCapOnAShortLegitimateLookingChain() {
    // Span cap must trigger on the raw caller-supplied range, independent of chain height - both
    // from/to here are individually unremarkable, only their span (6000) exceeds the cap.
    when(environment.containsArgument("from")).thenReturn(true);
    when(environment.getArgument("from")).thenReturn(0L);
    when(environment.containsArgument("to")).thenReturn(true);
    when(environment.getArgument("to")).thenReturn(6000L);

    assertThatThrownBy(() -> fetcherWithMaxRange(5000L).get(environment))
        .isInstanceOf(GraphQLException.class);
  }

  @Test
  void allowsSpanWithinCap() throws Exception {
    when(environment.containsArgument("from")).thenReturn(true);
    when(environment.getArgument("from")).thenReturn(0L);
    when(environment.containsArgument("to")).thenReturn(true);
    when(environment.getArgument("to")).thenReturn(10L);
    when(graphQLContext.<Supplier<Boolean>>get(GraphQLContextType.IS_ALIVE_HANDLER))
        .thenReturn(() -> true);
    stubBlockByNumberPresent();

    final List<NormalBlockAdapter> results = fetcherWithMaxRange(5000L).get(environment);

    assertThat(results).hasSize(11);
  }

  @Test
  void zeroMaxBlockRangeMeansNoSpanCap() throws Exception {
    when(environment.containsArgument("from")).thenReturn(true);
    when(environment.getArgument("from")).thenReturn(0L);
    when(environment.containsArgument("to")).thenReturn(true);
    when(environment.getArgument("to")).thenReturn(10_000L);
    when(graphQLContext.<Supplier<Boolean>>get(GraphQLContextType.IS_ALIVE_HANDLER))
        .thenReturn(() -> true);
    stubBlockByNumberPresent();

    // No span-cap rejection - but the chain-head clamp (below) still bounds the actual loop.
    final List<NormalBlockAdapter> results = fetcherWithMaxRange(0L).get(environment);

    assertThat(results).hasSize((int) CHAIN_HEAD + 1);
  }

  @Test
  void clampsToChainHeadEvenWithNoSpanCapConfigured() throws Exception {
    // With the span cap disabled (0 = no limit), the chain-head clamp is the ONLY thing standing
    // between a caller-supplied Long.MAX_VALUE-ish `to` and an effectively infinite loop. Confirm
    // it alone still bounds the loop to real chain height.
    when(environment.containsArgument("from")).thenReturn(true);
    when(environment.getArgument("from")).thenReturn(0L);
    when(environment.containsArgument("to")).thenReturn(true);
    when(environment.getArgument("to")).thenReturn(Long.MAX_VALUE - 1);
    when(graphQLContext.<Supplier<Boolean>>get(GraphQLContextType.IS_ALIVE_HANDLER))
        .thenReturn(() -> true);
    stubBlockByNumberPresent();

    fetcherWithMaxRange(0L).get(environment);

    verify(query, times((int) CHAIN_HEAD + 1)).blockByNumber(anyLong());
  }

  @Test
  void omittedToDefaultsToChainHeadAndIsStillCapped() throws Exception {
    when(environment.containsArgument("from")).thenReturn(true);
    when(environment.getArgument("from")).thenReturn(0L);
    when(environment.containsArgument("to")).thenReturn(false);
    when(graphQLContext.<Supplier<Boolean>>get(GraphQLContextType.IS_ALIVE_HANDLER))
        .thenReturn(() -> true);
    stubBlockByNumberPresent();

    final List<NormalBlockAdapter> results = fetcherWithMaxRange(5000L).get(environment);

    assertThat(results).hasSize((int) CHAIN_HEAD + 1);
  }

  @Test
  void omittedFromDefaultsToChainHeadInsteadOfNpeing() throws Exception {
    // blocks(to: 100) with no `from` at all: `from` is a nullable Long in the GraphQL schema, so
    // this must not unbox a null into a primitive long. Mirrors the existing to-omitted default.
    when(environment.containsArgument("from")).thenReturn(false);
    when(environment.containsArgument("to")).thenReturn(true);
    when(environment.getArgument("to")).thenReturn(CHAIN_HEAD);
    when(graphQLContext.<Supplier<Boolean>>get(GraphQLContextType.IS_ALIVE_HANDLER))
        .thenReturn(() -> true);
    stubBlockByNumberPresent();

    final List<NormalBlockAdapter> results = fetcherWithMaxRange(5000L).get(environment);

    assertThat(results).hasSize(1);
  }

  @Test
  void abortsAsSoonAsCallerIsGone() {
    when(environment.containsArgument("from")).thenReturn(true);
    when(environment.getArgument("from")).thenReturn(0L);
    when(environment.containsArgument("to")).thenReturn(true);
    when(environment.getArgument("to")).thenReturn(10L);
    when(graphQLContext.<Supplier<Boolean>>get(GraphQLContextType.IS_ALIVE_HANDLER))
        .thenReturn(() -> false);

    assertThatThrownBy(() -> fetcherWithMaxRange(5000L).get(environment))
        .isInstanceOf(RuntimeException.class);
    verify(query, never()).blockByNumber(anyLong());
  }

  @Test
  void nullIsAliveHandlerDoesNotThrowInsteadOfAborting() throws Exception {
    // IS_ALIVE_HANDLER is always populated for real HTTP requests, but a GraphQlContext built
    // without it (e.g. a hand-built test context) must fail open, not NPE inside the loop.
    when(environment.containsArgument("from")).thenReturn(true);
    when(environment.getArgument("from")).thenReturn(0L);
    when(environment.containsArgument("to")).thenReturn(true);
    when(environment.getArgument("to")).thenReturn(10L);
    when(graphQLContext.<Supplier<Boolean>>get(GraphQLContextType.IS_ALIVE_HANDLER))
        .thenReturn(null);
    stubBlockByNumberPresent();

    final List<NormalBlockAdapter> results = fetcherWithMaxRange(5000L).get(environment);

    assertThat(results).hasSize(11);
  }
}
