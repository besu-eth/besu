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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class TransactionTraceParamsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void emptyOptionsObjectShouldMatchDefaultTraceOptions() throws Exception {
    // Passing {} should be equivalent to passing no options at all.
    // All disable* fields default to false, so all tracing is enabled by default.
    final OpCodeTracerConfig defaultConfig = TraceOptions.DEFAULT.opCodeTracerConfig();

    // Parse an empty JSON options object — simulates debug_traceTransaction(hash, {})
    final TransactionTraceParams emptyParams = MAPPER.readValue("{}", TransactionTraceParams.class);
    final OpCodeTracerConfig emptyParamsConfig = emptyParams.traceOptions().opCodeTracerConfig();

    assertThat(emptyParamsConfig.traceMemory())
        .describedAs("traceMemory should match DEFAULT")
        .isEqualTo(defaultConfig.traceMemory());

    assertThat(emptyParamsConfig.traceStorage())
        .describedAs("traceStorage should match DEFAULT")
        .isEqualTo(defaultConfig.traceStorage());

    assertThat(emptyParamsConfig.traceStack())
        .describedAs("traceStack should match DEFAULT")
        .isEqualTo(defaultConfig.traceStack());
  }

  @Test
  public void defaultsShouldMatchOpCodeTracerConfigDefaults() {
    // TraceOptions.DEFAULT should use OpCodeTracerConfig.DEFAULT directly.
    // Memory tracing is off by default for performance reasons.
    final OpCodeTracerConfig defaultConfig = TraceOptions.DEFAULT.opCodeTracerConfig();

    assertThat(defaultConfig.traceStorage()).isTrue();
    assertThat(defaultConfig.traceMemory()).isFalse();
    assertThat(defaultConfig.traceStack()).isTrue();
  }

  @Test
  public void nonOpcodeTracerShouldEnableMemoryByDefault() throws Exception {
    // Non-opcode tracers (e.g. callTracer) need memory for internal operations
    // such as extracting CREATE init code, so memory should be enabled by default
    final TransactionTraceParams callTracerParams =
        MAPPER.readValue("{\"tracer\": \"callTracer\"}", TransactionTraceParams.class);
    final OpCodeTracerConfig config = callTracerParams.traceOptions().opCodeTracerConfig();

    assertThat(config.traceMemory())
        .describedAs("callTracer should have memory enabled by default")
        .isTrue();
  }

  @Test
  public void nonOpcodeTracerShouldRespectExplicitDisableMemory() throws Exception {
    // When user explicitly sets disableMemory, it should be respected even for callTracer
    final TransactionTraceParams params =
        MAPPER.readValue(
            "{\"tracer\": \"callTracer\", \"disableMemory\": true}", TransactionTraceParams.class);
    final OpCodeTracerConfig config = params.traceOptions().opCodeTracerConfig();

    assertThat(config.traceMemory())
        .describedAs("explicit disableMemory=true should be respected")
        .isFalse();
  }

  @Test
  public void enableMemoryTrueShouldEnableMemory() throws Exception {
    final TransactionTraceParams params =
        MAPPER.readValue("{\"enableMemory\": true}", TransactionTraceParams.class);
    final OpCodeTracerConfig config = params.traceOptions().opCodeTracerConfig();

    assertThat(config.traceMemory())
        .describedAs("enableMemory=true should enable memory tracing")
        .isTrue();
  }

  @Test
  public void enableMemoryFalseShouldDisableMemory() throws Exception {
    final TransactionTraceParams params =
        MAPPER.readValue("{\"enableMemory\": false}", TransactionTraceParams.class);
    final OpCodeTracerConfig config = params.traceOptions().opCodeTracerConfig();

    assertThat(config.traceMemory())
        .describedAs("enableMemory=false should disable memory tracing")
        .isFalse();
  }

  @Test
  public void enableMemoryShouldTakePrecedenceOverDisableMemory() throws Exception {
    final TransactionTraceParams params =
        MAPPER.readValue(
            "{\"enableMemory\": true, \"disableMemory\": true}", TransactionTraceParams.class);
    final OpCodeTracerConfig config = params.traceOptions().opCodeTracerConfig();

    assertThat(config.traceMemory())
        .describedAs("enableMemory should take precedence over disableMemory")
        .isTrue();
  }

  @Test
  public void nonOpcodeTracerShouldRespectExplicitEnableMemoryFalse() throws Exception {
    final TransactionTraceParams params =
        MAPPER.readValue(
            "{\"tracer\": \"callTracer\", \"enableMemory\": false}", TransactionTraceParams.class);
    final OpCodeTracerConfig config = params.traceOptions().opCodeTracerConfig();

    assertThat(config.traceMemory())
        .describedAs("explicit enableMemory=false should be respected for callTracer")
        .isFalse();
  }
}
