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
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

/**
 * Parameter wrapper for a single entry in the {@code debug_traceCallMany} batch.
 *
 * <p>Each entry is a JSON array of the form {@code [callParams, traceConfig]} where {@code
 * traceConfig} is optional and follows the same shape as the third parameter of {@code
 * debug_traceCall}.
 */
public class DebugTraceCallManyParameter {
  private final DebugTraceCallParameterTuple params;

  @JsonCreator
  public DebugTraceCallManyParameter(
      @JsonDeserialize(using = DebugTraceCallParameterDeserializer.class)
          final DebugTraceCallParameterTuple parameters) {
    this.params = parameters;
  }

  public DebugTraceCallParameterTuple getTuple() {
    return this.params;
  }
}

class DebugTraceCallParameterDeserializer extends StdDeserializer<DebugTraceCallParameterTuple> {
  private static final ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

  public DebugTraceCallParameterDeserializer(final Class<?> vc) {
    super(vc);
  }

  public DebugTraceCallParameterDeserializer() {
    this(null);
  }

  @Override
  public DebugTraceCallParameterTuple deserialize(
      final JsonParser p, final DeserializationContext ctxt) throws IOException {
    final JsonNode tupleNode = p.getCodec().readTree(p);
    if (!tupleNode.isArray() || tupleNode.isEmpty() || tupleNode.get(0).isNull()) {
      throw MismatchedInputException.from(
          p,
          DebugTraceCallParameterTuple.class,
          "debug_traceCallMany entry must be a non-empty array whose first element is the call params object");
    }
    final CallParameter callParameter =
        mapper.readValue(tupleNode.get(0).toString(), CallParameter.class);
    final TransactionTraceParams traceParams =
        tupleNode.size() > 1 && !tupleNode.get(1).isNull()
            ? mapper.readValue(tupleNode.get(1).toString(), TransactionTraceParams.class)
            : null;
    return new DebugTraceCallParameterTuple(callParameter, traceParams);
  }
}
