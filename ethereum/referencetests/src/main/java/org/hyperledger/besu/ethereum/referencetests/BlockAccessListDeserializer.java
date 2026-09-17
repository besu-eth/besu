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
package org.hyperledger.besu.ethereum.referencetests;

import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;

public class BlockAccessListDeserializer extends JsonDeserializer<BlockAccessList>
    implements ContextualDeserializer {

  private final String propertyName;

  public BlockAccessListDeserializer() {
    this.propertyName = null;
  }

  private BlockAccessListDeserializer(final String propertyName) {
    this.propertyName = propertyName;
  }

  @Override
  public JsonDeserializer<?> createContextual(
      final DeserializationContext ctxt, final BeanProperty property) {
    final String name = property != null ? property.getName() : null;
    return new BlockAccessListDeserializer(name);
  }

  @Override
  public BlockAccessList deserialize(final JsonParser p, final DeserializationContext ctxt)
      throws IOException {
    final ObjectCodec codec = p.getCodec();
    final JsonNode node = codec.readTree(p);

    final JsonNode balNode = findArrayNode(node, propertyName, true);
    if (balNode == null) {
      return null;
    }

    final JavaType listType =
        ctxt.getTypeFactory().constructCollectionType(List.class, AccountChangesJson.class);
    final List<AccountChangesJson> list;
    try (JsonParser nodeParser = balNode.traverse(codec)) {
      nodeParser.nextToken();
      list = ctxt.readValue(nodeParser, listType);
    } catch (final IOException e) {
      throw ctxt.weirdStringException(
          balNode.toString(),
          BlockAccessList.class,
          "Failed to parse blockAccessList: " + e.getMessage());
    }

    return AccountChangesJson.toBlockAccessList(list);
  }

  private JsonNode findArrayNode(
      final JsonNode node, final String fieldName, final boolean isRoot) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (isRoot && node.isArray()) {
      return node;
    }
    if (fieldName != null && node.has(fieldName)) {
      final JsonNode child = node.get(fieldName);
      if (child.isArray()) {
        return child;
      }
    }
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(node.fields(), 0), false)
        .map(e -> findArrayNode(e.getValue(), fieldName, false))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }
}
