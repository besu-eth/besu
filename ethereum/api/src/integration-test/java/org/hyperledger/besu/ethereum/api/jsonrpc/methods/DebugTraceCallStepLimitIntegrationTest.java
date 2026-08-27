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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.BlockchainImporter;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcTestMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugTraceCall;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ImmutableTransactionTraceParams;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.OpCodeLoggerTracerResult;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.testutil.BlockTestUtil;

import java.nio.charset.StandardCharsets;

import com.google.common.io.Resources;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test verifying that the server-side step limit (--rpc-max-trace-steps) prevents
 * debug_traceCall from accumulating an unbounded number of structLog frames.
 *
 * <p>The PoC bytecode (0x600062000400525b600756) expands memory to ~1 KB then loops forever.
 * Without a cap this fills the heap. With the cap, tracing stops at the configured limit and the
 * response carries {@code "truncated": true}.
 */
public class DebugTraceCallStepLimitIntegrationTest {

  // Bytecode: PUSH1 0x00, PUSH3 0x000400, MSTORE, JUMPDEST (PC=7), PUSH1 0x07, JUMP
  // Expands EVM memory to ~1 KB then loops until gas is exhausted.
  private static final String LOOP_BYTECODE = "0x600062000400525b600756";

  private static final Address TARGET =
      Address.fromHexString("0x0000000000000000000000000000000000009999");
  private static final long SERVER_STEP_LIMIT = 5L;

  private static JsonRpcTestMethodsFactory blockchain;

  @BeforeAll
  static void setUpOnce() throws Exception {
    final String genesisJson =
        Resources.toString(BlockTestUtil.getTestGenesisUrl(), StandardCharsets.UTF_8);
    blockchain =
        new JsonRpcTestMethodsFactory(
            new BlockchainImporter(BlockTestUtil.getTestBlockchainUrl(), genesisJson));
  }

  private DebugTraceCall methodWithLimit(final long limit) {
    final var apiConfig = ImmutableApiConfiguration.builder().debugTraceStepLimit(limit).build();
    return new DebugTraceCall(
        blockchain.getBlockchainQueries(),
        blockchain.getProtocolSchedule(),
        blockchain.getTransactionSimulator(),
        apiConfig);
  }

  private JsonRpcRequestContext buildRequest(final ImmutableTransactionTraceParams traceParams) {
    final var callParams = ImmutableCallParameter.builder().to(TARGET).build();
    final Object[] params = new Object[] {callParams, "latest", traceParams};
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceCall", params));
  }

  private StateOverrideMap loopOverride() {
    final StateOverrideMap overrides = new StateOverrideMap();
    overrides.put(TARGET, new StateOverride.Builder().withCode(LOOP_BYTECODE).build());
    return overrides;
  }

  @Test
  void serverStepLimitTruncatesStructLogs() {
    final var traceParams =
        ImmutableTransactionTraceParams.builder()
            .enableMemoryNullable(true)
            .stateOverrides(loopOverride())
            .build();

    final var response =
        (JsonRpcSuccessResponse)
            methodWithLimit(SERVER_STEP_LIMIT).response(buildRequest(traceParams));
    final var result = (OpCodeLoggerTracerResult) response.getResult();

    assertThat(result.getStructLogs()).hasSize((int) SERVER_STEP_LIMIT);
    assertThat(result.truncated()).isTrue();
  }

  @Test
  void callerLimitBelowServerLimitIsHonoured() {
    // Caller requests only 3 steps; server cap is 100 — caller's lower value should win
    final var traceParams =
        ImmutableTransactionTraceParams.builder()
            .enableMemoryNullable(true)
            .limit(3)
            .stateOverrides(loopOverride())
            .build();

    final var response =
        (JsonRpcSuccessResponse) methodWithLimit(100L).response(buildRequest(traceParams));
    final var result = (OpCodeLoggerTracerResult) response.getResult();

    assertThat(result.getStructLogs()).hasSize(3);
    assertThat(result.truncated()).isTrue();
  }

  @Test
  void callerCannotExceedServerLimit() {
    // Caller requests 50000 steps but server allows only SERVER_STEP_LIMIT
    final var traceParams =
        ImmutableTransactionTraceParams.builder()
            .enableMemoryNullable(true)
            .limit(50_000)
            .stateOverrides(loopOverride())
            .build();

    final var response =
        (JsonRpcSuccessResponse)
            methodWithLimit(SERVER_STEP_LIMIT).response(buildRequest(traceParams));
    final var result = (OpCodeLoggerTracerResult) response.getResult();

    assertThat(result.getStructLogs()).hasSize((int) SERVER_STEP_LIMIT);
    assertThat(result.truncated()).isTrue();
  }
}
