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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Custom deserializer for {@link CallParameter} within eth_simulateV1 calls.
 *
 * <p>When both {@code input} and {@code data} are provided with different values, {@code input}
 * takes precedence and {@code data} is ignored. This matches the behaviour of other EL clients
 * (Geth, Nethermind, Reth, Erigon) for eth_simulateV1, where only {@code input} is defined in the
 * execution-apis spec.
 *
 * <p>For eth_call and other methods, the standard {@link CallParameter} validation still applies
 * and an error is returned when the two fields conflict.
 */
public class SimulateCallParameterDeserializer extends StdDeserializer<CallParameter> {

  public SimulateCallParameterDeserializer() {
    super(CallParameter.class);
  }

  @Override
  public CallParameter deserialize(final JsonParser p, final DeserializationContext ctxt)
      throws IOException {
    final ObjectNode node = p.readValueAsTree();
    final JsonNode inputNode = node.get("input");
    final JsonNode dataNode = node.get("data");
    if (inputNode != null && dataNode != null && !inputNode.equals(dataNode)) {
      // input takes precedence; remove data so the standard CallParameter check does not fire
      node.remove("data");
    }
    return p.getCodec().treeToValue(node, CallParameter.class);
  }
}
