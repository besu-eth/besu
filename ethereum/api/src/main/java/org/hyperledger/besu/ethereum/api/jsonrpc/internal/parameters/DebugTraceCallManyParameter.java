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

import org.hyperledger.besu.ethereum.transaction.CallParameter;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

/**
 * Represents one entry in the {@code debug_traceCallMany} parameter array. Each entry is a
 * 2-element JSON array: {@code [callParams, traceConfig]}, where {@code traceConfig} is the same
 * optional object accepted by {@code debug_traceCall} (disableStorage, disableMemory, etc.).
 */
public class DebugTraceCallManyParameter {

  private final DebugCallParameterTuple params;

  @JsonCreator
  public DebugTraceCallManyParameter(
      @JsonDeserialize(using = DebugCallParameterDeserializer.class)
          final DebugCallParameterTuple parameters) {
    this.params = parameters;
  }

  public DebugCallParameterTuple getTuple() {
    return params;
  }

  /** Holds the deserialized {@link CallParameter} and optional {@link TransactionTraceParams}. */
  public static class DebugCallParameterTuple {
    private final CallParameter callParameter;
    private final TransactionTraceParams traceParams;

    public DebugCallParameterTuple(
        final CallParameter callParameter, final TransactionTraceParams traceParams) {
      this.callParameter = callParameter;
      this.traceParams = traceParams;
    }

    public CallParameter getCallParameter() {
      return callParameter;
    }

    public TransactionTraceParams getTraceParams() {
      return traceParams;
    }
  }
}

class DebugCallParameterDeserializer
    extends StdDeserializer<DebugTraceCallManyParameter.DebugCallParameterTuple> {

  private static final ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

  public DebugCallParameterDeserializer() {
    this(null);
  }

  public DebugCallParameterDeserializer(final Class<?> vc) {
    super(vc);
  }

  @Override
  public DebugTraceCallManyParameter.DebugCallParameterTuple deserialize(
      final JsonParser p, final DeserializationContext ctxt) throws IOException {
    final JsonNode tupleNode = p.getCodec().readTree(p);
    final CallParameter callParameter =
        mapper.readValue(tupleNode.get(0).toString(), CallParameter.class);
    // traceConfig (index 1) is optional
    final TransactionTraceParams traceParams =
        tupleNode.size() > 1 && !tupleNode.get(1).isNull()
            ? mapper.readValue(tupleNode.get(1).toString(), TransactionTraceParams.class)
            : null;
    return new DebugTraceCallManyParameter.DebugCallParameterTuple(callParameter, traceParams);
  }
}
