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
package org.hyperledger.besu.ethereum.transaction;

import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.ethereum.core.SimulationCodeDelegation;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Deserializes code delegation entries for call parameters, accepting either signed delegations or
 * simulation-only delegations that specify an authority directly.
 */
public class CodeDelegationParameterDeserializer extends JsonDeserializer<CodeDelegation> {

  @Override
  public CodeDelegation deserialize(final JsonParser p, final DeserializationContext ctxt)
      throws IOException {
    final ObjectMapper mapper = (ObjectMapper) p.getCodec();
    final JsonNode node = mapper.readTree(p);

    final boolean hasAuthority = node.hasNonNull("authority");
    final boolean hasSignature =
        node.hasNonNull("r")
            || node.hasNonNull("s")
            || node.hasNonNull("v")
            || node.hasNonNull("yParity");

    if (hasAuthority && hasSignature) {
      throw new IllegalArgumentException(
          "code delegation cannot specify both authority and signature fields");
    }

    if (hasAuthority) {
      return mapper.treeToValue(node, SimulationCodeDelegation.class);
    }

    // Normalize yParity to v for CodeDelegation deserialization.
    // EIP-7702 allows both field names; if only yParity is provided, map it to v.
    if (node.hasNonNull("yParity") && !node.hasNonNull("v") && node.isObject()) {
      ((ObjectNode) node).set("v", node.get("yParity"));
    }

    return mapper.treeToValue(node, org.hyperledger.besu.ethereum.core.CodeDelegation.class);
  }
}
