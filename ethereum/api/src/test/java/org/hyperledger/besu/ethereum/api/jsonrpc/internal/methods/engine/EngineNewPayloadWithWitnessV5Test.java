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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BOGOTA;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INTERNAL_ERROR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.WitnessCodeTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Inherits the {@code engine_newPayloadV5} suite so the witness variant is held to the same
 * request-validation, fork-gating and error-handling contract, and adds the behaviour specific to
 * this method: asking block processing to collect witness data, and refusing to answer when that
 * data is unavailable.
 *
 * <p>The VALID-response cases are disabled here rather than reimplemented. Producing a witness
 * needs a real Bonsai world state — a trie log, the parent state, and a second trie pass — which
 * cannot be faked through the mocks this suite is built on. That path is covered end to end by the
 * zkEVM execution-spec reference tests, which compare every generated witness against the spec
 * fixtures.
 */
public class EngineNewPayloadWithWitnessV5Test extends EngineNewPayloadV5Test {

  private static final String WITNESS_NEEDS_REAL_WORLD_STATE =
      "Witness generation needs a real Bonsai world state; covered by the zkEVM reference tests";

  @BeforeEach
  @Override
  public void before() {
    super.before();
    // The witness variant calls the three-arg overload, which the inherited setup does not stub.
    when(mergeCoordinator.rememberBlock(any(), any(), any()))
        .thenReturn(new BlockProcessingResult(Optional.empty()));
  }

  @Override
  protected EngineNewPayloadV1<?, ?> createMethodInstance() {
    return new EngineNewPayloadWithWitnessV5<>(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .mergeCoordinator(mergeCoordinator)
            .ethPeers(ethPeers)
            .metricsSystem(new NoOpMetricsSystem())
            .transactionPool(transactionPool)
            .maxRequestBlocks(0)
            .build(),
        AMSTERDAM,
        BOGOTA);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadWithWitnessV5");
  }

  @Test
  public void shouldAskBlockProcessingToCollectWitnessData() {
    final BlockAccessList blockAccessList = new BlockAccessList(emptyList());
    final BlockHeader header =
        setupPayloadV5(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.empty()),
            blockAccessList,
            0L);

    respV5(mockEnginePayloadParam(header, emptyList(), blockAccessList, 0L));

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Optional<WitnessCodeTracker>> tracker =
        ArgumentCaptor.forClass(Optional.class);
    verify(mergeCoordinator).rememberBlock(any(), any(), tracker.capture());
    assertThat(tracker.getValue())
        .as("the witness variant must hand a collector to block processing")
        .isPresent();
  }

  @Test
  public void shouldReturnInternalErrorWhenWitnessDataUnavailable() {
    final BlockAccessList blockAccessList = new BlockAccessList(emptyList());
    final BlockHeader header =
        setupPayloadV5(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.empty()),
            blockAccessList,
            0L);

    // A successful import whose result carries no witness data must not be reported as VALID.
    final JsonRpcResponse resp =
        respV5(mockEnginePayloadParam(header, emptyList(), blockAccessList, 0L));

    assertThat(fromErrorResp(resp).getCode()).isEqualTo(INTERNAL_ERROR.getCode());
  }

  @Override
  @Test
  @Disabled(WITNESS_NEEDS_REAL_WORLD_STATE)
  public void shouldReturnValid() {}

  @Override
  @Test
  @Disabled(WITNESS_NEEDS_REAL_WORLD_STATE)
  public void shouldReturnValidIfBlockAccessListMatchesHeader() {}

  @Override
  @Test
  @Disabled(WITNESS_NEEDS_REAL_WORLD_STATE)
  public void shouldReturnInvalidOnBlockExecutionError() {}

  @Override
  @Test
  @Disabled(WITNESS_NEEDS_REAL_WORLD_STATE)
  public void shouldReturnValidIfWithdrawalsIsNotNull_WhenWithdrawalsAllowed() {}

  @Override
  @Test
  @Disabled(WITNESS_NEEDS_REAL_WORLD_STATE)
  public void validateVersionedHash_whenListIsPresentAndEmpty() {}

  @Override
  @Test
  @Disabled(WITNESS_NEEDS_REAL_WORLD_STATE)
  public void shouldReturnValidWhenPraguePayloadHasEmptyExecutionRequests() {}

  @Override
  @Test
  @Disabled(WITNESS_NEEDS_REAL_WORLD_STATE)
  public void shouldReturnValidIfRequestsIsNotNull_WhenRequestsAllowed() {}
}
