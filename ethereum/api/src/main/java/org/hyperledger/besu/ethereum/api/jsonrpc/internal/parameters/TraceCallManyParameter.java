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

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.transaction.CallParameter;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class TraceCallManyParameter {
  TraceCallParameterTuple params;

  @JsonCreator
  public TraceCallManyParameter(
      @JsonDeserialize(using = TraceCallParameterDeserializer.class)
          final TraceCallParameterTuple parameters) {
    this.params = parameters;
  }

  public TraceCallParameterTuple getTuple() {
    return this.params;
  }
}

class TraceCallParameterDeserializer extends StdDeserializer<TraceCallParameterTuple> {
  private static final ObjectMapper mapper = JsonRpcObjectMapperFactory.createParameterMapper();

  public TraceCallParameterDeserializer(final Class<?> vc) {
    super(vc);
  }

  public TraceCallParameterDeserializer() {
    this(null);
  }

  @Override
  public TraceCallParameterTuple deserialize(final JsonParser p, final DeserializationContext ctxt)
      throws IOException {
    final JsonNode tupleNode = p.getCodec().readTree(p);
    return new TraceCallParameterTuple(
        mapper.readValue(tupleNode.get(0).toString(), CallParameter.class),
        mapper.readValue(tupleNode.get(1).toString(), TraceTypeParameter.class));
  }
}
