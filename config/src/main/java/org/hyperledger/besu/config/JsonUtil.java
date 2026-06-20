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
package org.hyperledger.besu.config;

import org.hyperledger.besu.util.number.PositiveNumber;

import java.io.IOException;
import java.net.URL;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.filter.FilteringParserDelegate;
import com.fasterxml.jackson.core.filter.TokenFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Predicates;
import org.apache.tuweni.bytes.Bytes;

/** The Json util class. */
public class JsonUtil {
  private static final JsonFactory JSON_FACTORY =
      JsonFactory.builder()
          .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
          .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
          .build();

  /** Default constructor. */
  private JsonUtil() {}

  /**
   * Converts all the object keys (but none of the string values) to lowercase for easier lookup.
   * This is useful in cases such as the 'genesis.json' file where all keys are assumed to be case
   * insensitive.
   *
   * @param objectNode The ObjectNode to be normalized
   * @return a copy of the json object with all keys in lower case.
   */
  public static ObjectNode normalizeKeys(final ObjectNode objectNode) {
    return normalizeKeys(objectNode, Predicates.alwaysTrue());
  }

  /**
   * Converts all the object keys (but none of the string values) to lowercase for easier lookup.
   * This is useful in cases such as the 'genesis.json' file where all keys are assumed to be case
   * insensitive.
   *
   * @param objectNode The ObjectNode to be normalized
   * @param fieldPredicate The predicate to filter the fields to normalize
   * @return a copy of the json object with all keys in lower case.
   */
  public static ObjectNode normalizeKeys(
      final ObjectNode objectNode, final Predicate<Map.Entry<String, JsonNode>> fieldPredicate) {
    final ObjectNode normalized = JsonUtil.createEmptyObjectNode();
    objectNode
        .propertyStream()
        .forEach(
            entry -> {
              if (!fieldPredicate.test(entry)) {
                return;
              }

              final String key = entry.getKey();
              final JsonNode value = entry.getValue();
              final String normalizedKey = normalizeKey(key);
              if (value instanceof ObjectNode nestedObject) {
                normalized.set(normalizedKey, normalizeKeys(nestedObject));
              } else if (value instanceof ArrayNode nestedArray) {
                normalized.set(normalizedKey, normalizeKeysInArray(nestedArray));
              } else {
                normalized.set(normalizedKey, value);
              }
            });
    return normalized;
  }

  /**
   * Converts the key to lowercase for easier lookup. This is useful in cases such as the
   * 'genesis.json' file where all keys are assumed to be case insensitive.
   *
   * @param key the key to be normalized
   * @return key in lower case.
   */
  public static String normalizeKey(final String key) {
    return key.toLowerCase(Locale.US);
  }

  private static ArrayNode normalizeKeysInArray(final ArrayNode arrayNode) {
    final ArrayNode normalizedArray = JsonUtil.createEmptyArrayNode();
    arrayNode.forEach(
        value -> {
          if (value instanceof ObjectNode nestedObject) {
            normalizedArray.add(normalizeKeys(nestedObject));
          } else if (value instanceof ArrayNode nestedArray) {
            normalizedArray.add(normalizeKeysInArray(nestedArray));
          } else {
            normalizedArray.add(value);
          }
        });
    return normalizedArray;
  }

  /**
   * Get the string representation of the value at {@code key}. For example, a numeric value like 5
   * will be returned as "5".
   *
   * @param node The {@code ObjectNode} from which the value will be extracted.
   * @param key The key corresponding to the value to extract.
   * @return The value at the given key as a string if it exists.
   */
  public static Optional<String> getValueAsString(final ObjectNode node, final String key) {
    return getValue(node, key).map(JsonNode::asText);
  }

  /**
   * Get the string representation of the value at {@code key}. For example, a numeric value like 5
   * will be returned as "5".
   *
   * @param node The {@code ObjectNode} from which the value will be extracted.
   * @param key The key corresponding to the value to extract.
   * @param defaultValue The value to return if no value is found at {@code key}.
   * @return The value at the given key as a string if it exists, otherwise {@code defaultValue}
   */
  public static String getValueAsString(
      final ObjectNode node, final String key, final String defaultValue) {
    return getValueAsString(node, key).orElse(defaultValue);
  }

  /**
   * Checks whether an {@code ObjectNode} contains the given key.
   *
   * @param node The {@code ObjectNode} to inspect.
   * @param key The key to check.
   * @return Returns true if the given key is set.
   */
  public static boolean hasKey(final ObjectNode node, final String key) {
    return node.has(key);
  }

  /**
   * Returns textual (string) value at {@code key}. See {@link #getValueAsString} for retrieving
   * non-textual values in string form.
   *
   * @param node The {@code ObjectNode} from which the value will be extracted.
   * @param key The key corresponding to the value to extract.
   * @return The textual value at {@code key} if it exists.
   */
  public static Optional<String> getString(final ObjectNode node, final String key) {
    return getValue(node, key)
        .filter(jsonNode -> validateType(jsonNode, JsonNodeType.STRING))
        .map(JsonNode::asText);
  }

  /**
   * Returns textual (string) value at {@code key}. See {@link #getValueAsString} for retrieving
   * non-textual values in string form.
   *
   * @param node The {@code ObjectNode} from which the value will be extracted.
   * @param key The key corresponding to the value to extract.
   * @param defaultValue The value to return if no value is found at {@code key}.
   * @return The textual value at {@code key} if it exists, otherwise {@code defaultValue}
   */
  public static String getString(
      final ObjectNode node, final String key, final String defaultValue) {
    return getString(node, key).orElse(defaultValue);
  }

  /**
   * Gets int.
   *
   * @param node the node
   * @param key the key
   * @return the int
   */
  public static OptionalInt getInt(final ObjectNode node, final String key) {
    return getValue(node, key)
        .filter(jsonNode -> validateType(jsonNode, JsonNodeType.NUMBER))
        .filter(JsonUtil::validateInt)
        .map(JsonNode::asInt)
        .map(OptionalInt::of)
        .orElse(OptionalInt.empty());
  }

  /**
   * Gets int.
   *
   * @param node the node
   * @param key the key
   * @param defaultValue the default value
   * @return the int
   */
  public static int getInt(final ObjectNode node, final String key, final int defaultValue) {
    return getInt(node, key).orElse(defaultValue);
  }

  /**
   * Gets positive int.
   *
   * @param node the node
   * @param key the key
   * @return the positive int
   */
  public static OptionalInt getPositiveInt(final ObjectNode node, final String key) {
    return getValueAsString(node, key)
        .map(v -> OptionalInt.of(parsePositiveInt(key, v)))
        .orElse(OptionalInt.empty());
  }

  /**
   * Gets positive int.
   *
   * @param node the node
   * @param key the key
   * @param defaultValue the default value
   * @return the positive int
   */
  public static int getPositiveInt(
      final ObjectNode node, final String key, final int defaultValue) {
    final String value = getValueAsString(node, key, String.valueOf(defaultValue));
    return parsePositiveInt(key, value);
  }

  private static int parsePositiveInt(final String key, final String value) {
    try {
      return PositiveNumber.fromString(value).getValue();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid property value, " + key + " should be a positive integer: " + value);
    }
  }

  /**
   * Gets long.
   *
   * @param json the json
   * @param key the key
   * @return the long
   */
  public static OptionalLong getLong(final ObjectNode json, final String key) {
    return getValue(json, key)
        .filter(jsonNode -> validateType(jsonNode, JsonNodeType.NUMBER))
        .filter(JsonUtil::validateLong)
        .map(JsonNode::asLong)
        .map(OptionalLong::of)
        .orElse(OptionalLong.empty());
  }

  /**
   * Gets long.
   *
   * @param json the json
   * @param key the key
   * @param defaultValue the default value
   * @return the long
   */
  public static long getLong(final ObjectNode json, final String key, final long defaultValue) {
    return getLong(json, key).orElse(defaultValue);
  }

  /**
   * Gets boolean.
   *
   * @param node the node
   * @param key the key
   * @return the boolean
   */
  public static Optional<Boolean> getBoolean(final ObjectNode node, final String key) {
    return getValue(node, key)
        .filter(jsonNode -> validateType(jsonNode, JsonNodeType.BOOLEAN))
        .map(JsonNode::asBoolean);
  }

  /**
   * Gets boolean.
   *
   * @param node the node
   * @param key the key
   * @param defaultValue the default value
   * @return the boolean
   */
  public static boolean getBoolean(
      final ObjectNode node, final String key, final boolean defaultValue) {
    return getBoolean(node, key).orElse(defaultValue);
  }

  /**
   * Gets Bytes.
   *
   * @param json the json
   * @param key the key
   * @return the Bytes
   */
  public static Optional<Bytes> getBytes(final ObjectNode json, final String key) {
    return getParsedValue(json, key, Bytes::fromHexString);
  }

  /**
   * Gets Wei.
   *
   * @param json the json
   * @param key the key
   * @param defaultValue the default value
   * @return the Wei
   */
  public static Bytes getBytes(final ObjectNode json, final String key, final Bytes defaultValue) {
    return getBytes(json, key).orElse(defaultValue);
  }

  private static <T> Optional<T> getParsedValue(
      final ObjectNode json, final String name, final Function<String, T> parser) {

    return getValue(json, name).map(JsonNode::asText).map(parser);
  }

  /**
   * Create empty object node object node.
   *
   * @return the object node
   */
  public static ObjectNode createEmptyObjectNode() {
    final ObjectMapper mapper = getObjectMapper();
    return mapper.createObjectNode();
  }

  /**
   * Create empty array node array node.
   *
   * @return the array node
   */
  public static ArrayNode createEmptyArrayNode() {
    final ObjectMapper mapper = getObjectMapper();
    return mapper.createArrayNode();
  }

  /**
   * Object node from map object node.
   *
   * @param map the map
   * @return the object node
   */
  public static ObjectNode objectNodeFromMap(final Map<String, Object> map) {
    return getObjectMapper().valueToTree(map);
  }

  /**
   * Object node from string object node.
   *
   * @param jsonData the json data
   * @return the object node
   */
  public static ObjectNode objectNodeFromString(final String jsonData) {
    return objectNodeFromString(jsonData, false);
  }

  /**
   * Object node from string object node.
   *
   * @param jsonData the json data
   * @param allowComments true to allow comments
   * @param excludeFields names of the fields to not read
   * @return the object node
   */
  public static ObjectNode objectNodeFromString(
      final String jsonData, final boolean allowComments, final String... excludeFields) {
    try {
      return objectNodeFromParser(
          JSON_FACTORY.createParser(jsonData), allowComments, excludeFields);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Object node from string object node.
   *
   * @param jsonSource the json data
   * @param allowComments true to allow comments
   * @param excludeFields names of the fields to not read
   * @return the object node
   */
  public static ObjectNode objectNodeFromURL(
      final URL jsonSource, final boolean allowComments, final String... excludeFields) {
    try {
      return objectNodeFromParser(
          JSON_FACTORY.createParser(jsonSource).enable(Feature.AUTO_CLOSE_SOURCE),
          allowComments,
          excludeFields);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get a JsonParser to parse JSON from URL.
   *
   * @param jsonSource the json source
   * @param allowComments true to allow comments
   * @return the json parser
   */
  public static JsonParser jsonParserFromURL(final URL jsonSource, final boolean allowComments) {
    try {
      return JSON_FACTORY
          .createParser(jsonSource)
          .enable(Feature.AUTO_CLOSE_SOURCE)
          .configure(Feature.ALLOW_COMMENTS, allowComments);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static ObjectNode objectNodeFromParser(
      final JsonParser baseParser, final boolean allowComments, final String... excludeFields) {
    try {
      final var parser =
          excludeFields.length > 0
              ? new FilteringParserDelegate(
                  baseParser,
                  new NameExcludeFilter(excludeFields),
                  TokenFilter.Inclusion.INCLUDE_ALL_AND_PATH,
                  true)
              : baseParser;
      parser.configure(Feature.ALLOW_COMMENTS, allowComments);

      final ObjectMapper objectMapper = new ObjectMapper();
      final JsonNode jsonNode = objectMapper.readTree(parser);
      validateType(jsonNode, JsonNodeType.OBJECT);
      return (ObjectNode) jsonNode;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Object node from URL.
   *
   * @param jsonSource the URL of the json source
   * @param allowComments true to allow comments
   * @return the object node
   */
  public static ObjectNode objectNodeFromURL(final URL jsonSource, final boolean allowComments) {

    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(Feature.ALLOW_COMMENTS, allowComments);
    try {
      final JsonNode jsonNode = objectMapper.readTree(jsonSource);
      validateType(jsonNode, JsonNodeType.OBJECT);
      return (ObjectNode) jsonNode;
    } catch (final IOException e) {
      // Reading directly from a string should not raise an IOException, just catch and rethrow
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets json.
   *
   * @param objectNode the object node
   * @return the json
   * @throws JsonProcessingException the json processing exception
   */
  public static String getJson(final Object objectNode) throws JsonProcessingException {
    return getJson(objectNode, true);
  }

  /**
   * Gets json.
   *
   * @param objectNode the object node
   * @param prettyPrint true for pretty print
   * @return the json
   * @throws JsonProcessingException the json processing exception
   */
  public static String getJson(final Object objectNode, final boolean prettyPrint)
      throws JsonProcessingException {
    final ObjectMapper mapper = getObjectMapper();
    if (prettyPrint) {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
    } else {
      return mapper.writeValueAsString(objectNode);
    }
  }

  /**
   * Gets object mapper.
   *
   * @return the object mapper
   */
  public static ObjectMapper getObjectMapper() {
    return new ObjectMapper();
  }

  /**
   * Gets object node.
   *
   * @param json the json
   * @param fieldKey the field key
   * @return the object node
   */
  public static Optional<ObjectNode> getObjectNode(final ObjectNode json, final String fieldKey) {
    return getObjectNode(json, fieldKey, true);
  }

  /**
   * Gets object node.
   *
   * @param json the json
   * @param fieldKey the field key
   * @param strict true for strict mode
   * @return the object node
   */
  public static Optional<ObjectNode> getObjectNode(
      final ObjectNode json, final String fieldKey, final boolean strict) {
    final JsonNode obj = json.get(fieldKey);
    if (obj == null || obj.isNull()) {
      return Optional.empty();
    }

    if (!obj.isObject()) {
      if (strict) {
        validateType(obj, JsonNodeType.OBJECT);
      } else {
        return Optional.empty();
      }
    }

    return Optional.of((ObjectNode) obj);
  }

  /**
   * Gets array node.
   *
   * @param json the json
   * @param fieldKey the field key
   * @return the array node
   */
  public static Optional<ArrayNode> getArrayNode(final ObjectNode json, final String fieldKey) {
    return getArrayNode(json, fieldKey, true);
  }

  /**
   * Gets array node.
   *
   * @param json the json
   * @param fieldKey the field key
   * @param strict true for strict mode
   * @return the array node
   */
  public static Optional<ArrayNode> getArrayNode(
      final ObjectNode json, final String fieldKey, final boolean strict) {
    final JsonNode obj = json.get(fieldKey);
    if (obj == null || obj.isNull()) {
      return Optional.empty();
    }

    if (!obj.isArray()) {
      if (strict) {
        validateType(obj, JsonNodeType.ARRAY);
      } else {
        return Optional.empty();
      }
    }

    return Optional.of((ArrayNode) obj);
  }

  private static Optional<JsonNode> getValue(final ObjectNode node, final String key) {
    final JsonNode jsonNode = node.get(key);
    if (jsonNode == null || jsonNode.isNull()) {
      return Optional.empty();
    }
    return Optional.of(jsonNode);
  }

  private static boolean validateType(final JsonNode node, final JsonNodeType expectedType) {
    if (node.getNodeType() != expectedType) {
      final String errorMessage =
          String.format(
              "Expected %s value but got %s",
              expectedType.toString().toLowerCase(Locale.ROOT),
              node.getNodeType().toString().toLowerCase(Locale.ROOT));
      throw new IllegalArgumentException(errorMessage);
    }
    return true;
  }

  private static boolean validateLong(final JsonNode node) {
    if (!node.canConvertToLong()) {
      throw new IllegalArgumentException("Cannot convert value to long: " + node);
    }
    return true;
  }

  private static boolean validateInt(final JsonNode node) {
    if (!node.canConvertToInt()) {
      throw new IllegalArgumentException("Cannot convert value to integer: " + node);
    }
    return true;
  }

  private static class NameExcludeFilter extends TokenFilter {
    private final Set<String> names;

    public NameExcludeFilter(final String... names) {
      this.names = Set.of(names);
    }

    @Override
    public TokenFilter includeProperty(final String name) {
      if (names.contains(name)) {
        return null;
      }
      return this;
    }

    @Override
    public boolean includeEmptyObject(final boolean contentsFiltered) {
      return !contentsFiltered;
    }

    @Override
    public boolean includeEmptyArray(final boolean contentsFiltered) {
      return !contentsFiltered;
    }
  }
}
