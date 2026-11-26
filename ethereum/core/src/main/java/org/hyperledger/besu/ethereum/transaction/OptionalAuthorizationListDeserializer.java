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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Deserializes {@code authorizationList} so that a present empty array is distinct from an omitted
 * field. {@link java.util.Optional#empty()} means the field was absent; {@code Optional.of} wraps
 * the list that was provided, including an empty one.
 */
public class OptionalAuthorizationListDeserializer
    extends JsonDeserializer<Optional<List<CodeDelegation>>> {

  private static final CodeDelegationParameterDeserializer ENTRY =
      new CodeDelegationParameterDeserializer();

  @Override
  public Optional<List<CodeDelegation>> deserialize(
      final JsonParser p, final DeserializationContext ctxt) throws IOException {
    final ObjectMapper mapper = (ObjectMapper) p.getCodec();
    final JsonNode node = mapper.readTree(p);
    if (node == null || node.isNull()) {
      return Optional.of(List.of());
    }
    if (!node.isArray()) {
      ctxt.reportInputMismatch(
          OptionalAuthorizationListDeserializer.class, "authorizationList must be an array");
    }
    final List<CodeDelegation> entries = new ArrayList<>(node.size());
    for (final JsonNode item : node) {
      final JsonParser itemParser = mapper.treeAsTokens(item);
      if (itemParser.currentToken() == null) {
        itemParser.nextToken();
      }
      entries.add(ENTRY.deserialize(itemParser, ctxt));
    }
    return Optional.of(List.copyOf(entries));
  }

  @Override
  public Optional<List<CodeDelegation>> getNullValue(final DeserializationContext ctxt) {
    return Optional.of(List.of());
  }
}
