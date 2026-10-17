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
import static org.mockito.Mockito.lenient;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.FilterParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Resolution of the block tags trace_filter accepts as range bounds. */
@ExtendWith(MockitoExtension.class)
public class TraceFilterBlockTagTest {

  private static final long HEAD = 2000;
  private static final long SAFE = 1500;
  private static final long FINALIZED = 1400;

  @Mock ProtocolSchedule protocolSchedule;
  @Mock BlockchainQueries blockchainQueries;

  private TraceFilter method;

  @BeforeEach
  public void setUp() {
    lenient().when(blockchainQueries.headBlockNumber()).thenReturn(HEAD);
    method =
        new TraceFilter(
            protocolSchedule,
            blockchainQueries,
            0L,
            new NoOpMetricsSystem(),
            new DeterministicEthScheduler());
  }

  // an inverted range is rejected with both resolved bounds in the message, which shows the block
  // each tag resolved to without tracing it
  @ParameterizedTest
  @CsvSource({"safe, 1500", "finalized, 1400"})
  public void resolvesTagToItsBlockAsFromBlock(final String tag, final long expected) {
    stubTags();

    assertThatThrownBy(
            () ->
                method.response(request(new BlockParameter(tag), new BlockParameter(expected - 1))))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessage(
            "fromBlock (" + expected + ") is greater than toBlock (" + (expected - 1) + ")");
  }

  @ParameterizedTest
  @CsvSource({"safe, 1500", "finalized, 1400"})
  public void resolvesTagToItsBlockAsToBlock(final String tag, final long expected) {
    stubTags();

    assertThatThrownBy(
            () ->
                method.response(request(new BlockParameter(expected + 1), new BlockParameter(tag))))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessage(
            "fromBlock (" + (expected + 1) + ") is greater than toBlock (" + expected + ")");
  }

  @ParameterizedTest
  @ValueSource(strings = {"safe", "finalized"})
  public void rejectsUnavailableTag(final String tag) {
    lenient().when(blockchainQueries.safeBlockHeader()).thenReturn(Optional.empty());
    lenient().when(blockchainQueries.finalizedBlockHeader()).thenReturn(Optional.empty());

    assertThatThrownBy(
            () -> method.response(request(new BlockParameter(tag), BlockParameter.LATEST)))
        .isInstanceOfSatisfying(
            InvalidJsonRpcParameters.class,
            e ->
                assertThat(e.getRpcErrorType())
                    .isEqualTo(RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS));
  }

  private void stubTags() {
    lenient()
        .when(blockchainQueries.safeBlockHeader())
        .thenReturn(Optional.of(new BlockHeaderTestFixture().number(SAFE).buildHeader()));
    lenient()
        .when(blockchainQueries.finalizedBlockHeader())
        .thenReturn(Optional.of(new BlockHeaderTestFixture().number(FINALIZED).buildHeader()));
  }

  private static JsonRpcRequestContext request(
      final BlockParameter fromBlock, final BlockParameter toBlock) {
    final FilterParameter filterParameter =
        new FilterParameter(fromBlock, toBlock, null, null, null, null, null, null, null);
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "trace_filter", new Object[] {filterParameter}));
  }
}
