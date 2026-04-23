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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;

@Value.Immutable
@JsonSerialize(as = ImmutableTransactionTraceParams.class)
@JsonDeserialize(as = ImmutableTransactionTraceParams.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface TransactionTraceParams {

  @JsonProperty("txHash")
  @Nullable String getTransactionHash();

  @JsonProperty(value = "disableStorage")
  @Nullable Boolean disableStorageNullable();

  default boolean disableStorage() {
    return Boolean.TRUE.equals(disableStorageNullable());
  }

  @JsonProperty(value = "disableMemory")
  @Nullable Boolean disableMemoryNullable();

  default boolean disableMemory() {
    return Boolean.TRUE.equals(disableMemoryNullable());
  }

  @JsonProperty(value = "enableMemory")
  @Nullable Boolean enableMemoryNullable();

  default boolean enableMemory() {
    return Boolean.TRUE.equals(enableMemoryNullable());
  }

  @JsonProperty(value = "disableStack")
  @Nullable Boolean disableStackNullable();

  default boolean disableStack() {
    return Boolean.TRUE.equals(disableStackNullable());
  }

  @JsonProperty("tracer")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable String tracer();

  @JsonProperty("tracerConfig")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // The Immutable annotation generates a Guava map for which Jackson deserialization fails. We are
  // explicitly using LinkedHashMap to overcome this issue. The suppression is to avoid warnings
  // about using a non-API type.
  @SuppressWarnings("NonApiType")
  LinkedHashMap<String, Object> tracerConfig();

  @JsonProperty("opcodes")
  @Value.Default
  default Set<String> opcodes() {
    return Collections.emptySet();
  }

  @JsonProperty("stateOverrides")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  StateOverrideMap stateOverrides();

  /**
   * Convert JSON-RPC parameters to a {@link TraceOptions} object.
   *
   * @return TraceOptions object containing the tracer type and configuration.
   */
  default TraceOptions traceOptions() {
    // Convert string tracer to TracerType enum, handling null case
    TracerType tracerType =
        tracer() != null
            ? TracerType.fromString(tracer())
            : TracerType.OPCODE_TRACER; // Default to opcode tracer when null

    var builder = OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT);
    // Only override defaults when the user explicitly provided a value
    if (disableStorageNullable() != null) {
      builder.traceStorage(!disableStorage());
    }
    if (enableMemoryNullable() != null) {
      builder.traceMemory(enableMemory());
    } else if (disableMemoryNullable() != null) {
      builder.traceMemory(!disableMemory());
    } else if (tracerType != TracerType.OPCODE_TRACER) {
      // Non-opcode tracers (e.g. callTracer) need memory capture enabled for internal
      // operations such as extracting CREATE init code, even when disableMemory is not set
      builder.traceMemory(true);
    }
    if (disableStackNullable() != null) {
      builder.traceStack(!disableStack());
    }
    var opCodeTracerConfig = builder.traceOpcodes(opcodes()).build();

    return new TraceOptions(tracerType, opCodeTracerConfig, tracerConfig(), stateOverrides());
  }
}
