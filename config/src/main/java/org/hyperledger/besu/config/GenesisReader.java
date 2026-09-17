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
package org.hyperledger.besu.config;

import static org.hyperledger.besu.config.JsonUtil.normalizeKey;
import static org.hyperledger.besu.config.JsonUtil.normalizeKeys;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Streams;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

interface GenesisReader {
  String CONFIG_FIELD = "config";
  String ALLOCATION_FIELD = "alloc";

  ObjectNode getRoot();

  ObjectNode getConfig();

  Stream<GenesisAccount> streamAllocations();

  class FromObjectNode implements GenesisReader {
    private final ObjectNode allocations;
    private final ObjectNode rootWithoutAllocations;

    public FromObjectNode(final ObjectNode root) {
      this.allocations =
          root.get(ALLOCATION_FIELD) != null
              ? (ObjectNode) root.get(ALLOCATION_FIELD)
              : JsonUtil.createEmptyObjectNode();
      this.rootWithoutAllocations =
          normalizeKeys(root, field -> !field.getKey().equals(ALLOCATION_FIELD));
    }

    @Override
    public ObjectNode getRoot() {
      return rootWithoutAllocations;
    }

    @Override
    public ObjectNode getConfig() {
      return JsonUtil.getObjectNode(rootWithoutAllocations, CONFIG_FIELD)
          .orElse(JsonUtil.createEmptyObjectNode());
    }

    @Override
    public Stream<GenesisAccount> streamAllocations() {
      return allocations
          .propertyStream()
          .map(
              entry -> {
                final var on = normalizeKeys((ObjectNode) entry.getValue());
                return new GenesisAccount(
                    Address.fromHexString(entry.getKey()),
                    JsonUtil.getValueAsString(on, "nonce")
                        .map(ParserUtils::parseUnsignedLong)
                        .orElse(0L),
                    JsonUtil.getString(on, "balance")
                        .map(ParserUtils::parseBalance)
                        .orElse(Wei.ZERO),
                    JsonUtil.getBytes(on, "code", null),
                    ParserUtils.getStorageMap(on, "storage"),
                    JsonUtil.getBytes(on, "privatekey").map(Bytes32::wrap).orElse(null));
              });
    }
  }

  class FromURL implements GenesisReader {
    private final URL url;
    private final ObjectNode rootWithoutAllocations;

    public FromURL(final URL url) {
      this.url = url;
      this.rootWithoutAllocations =
          normalizeKeys(JsonUtil.objectNodeFromURL(url, false, ALLOCATION_FIELD));
    }

    @Override
    public ObjectNode getRoot() {
      return rootWithoutAllocations;
    }

    @Override
    public ObjectNode getConfig() {
      return JsonUtil.getObjectNode(rootWithoutAllocations, CONFIG_FIELD)
          .orElse(JsonUtil.createEmptyObjectNode());
    }

    @Override
    public Stream<GenesisAccount> streamAllocations() {
      final var parser = JsonUtil.jsonParserFromURL(url, false);

      try {
        parser.nextToken();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
          if (ALLOCATION_FIELD.equals(parser.currentName())) {
            parser.nextToken();
            parser.nextToken();
            break;
          } else {
            parser.skipChildren();
          }
        }
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }

      return Streams.stream(new AllocationIterator(parser));
    }

    private static class AllocationIterator implements Iterator<GenesisAccount> {
      final JsonParser parser;

      AllocationIterator(final JsonParser parser) {
        this.parser = parser;
      }

      @Override
      public boolean hasNext() {
        final var end = parser.currentToken() == JsonToken.END_OBJECT;
        if (end) {
          try {
            parser.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return !end;
      }

      @Override
      public GenesisAccount next() {
        try {
          final Address address = Address.fromHexString(parser.currentName());
          long nonce = 0;
          Wei balance = Wei.ZERO;
          Bytes code = null;
          Map<UInt256, UInt256> storage = Map.of();
          Bytes32 privateKey = null;
          parser.nextToken(); // consume start object
          while (parser.nextToken() != JsonToken.END_OBJECT) {
            switch (normalizeKey(parser.currentName())) {
              case "nonce" -> {
                parser.nextToken();
                nonce = ParserUtils.parseUnsignedLong(parser.getText());
              }
              case "balance" -> {
                parser.nextToken();
                balance = ParserUtils.parseBalance(parser.getText());
              }
              case "code" -> {
                parser.nextToken();
                code = Bytes.fromHexStringLenient(parser.getText());
              }
              case "privatekey" -> {
                parser.nextToken();
                privateKey = Bytes32.fromHexStringLenient(parser.getText());
              }
              case "storage" -> {
                parser.nextToken();
                storage = new HashMap<>();
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                  final var key = UInt256.fromHexString(parser.currentName());
                  parser.nextToken();
                  final var value = UInt256.fromHexString(parser.getText());
                  storage.put(key, value);
                }
              }
              default -> {} // unknown keys ignored below
            }
            if (parser.currentToken() == JsonToken.START_OBJECT) {
              // ignore any unknown nested object
              parser.skipChildren();
            }
          }
          parser.nextToken();
          return new GenesisAccount(address, nonce, balance, code, storage, privateKey);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  class ParserUtils {
    static long parseUnsignedLong(final String value) {
      String v = value.toLowerCase(Locale.US);
      if (v.startsWith("0x")) {
        v = v.substring(2);
      }
      return Long.parseUnsignedLong(v, 16);
    }

    static Wei parseBalance(final String balance) {
      final BigInteger val;
      if (balance.startsWith("0x")) {
        val = new BigInteger(1, Bytes.fromHexStringLenient(balance).toArrayUnsafe());
      } else {
        val = new BigInteger(balance);
      }

      return Wei.of(val);
    }

    static Map<UInt256, UInt256> getStorageMap(final ObjectNode json, final String key) {
      return JsonUtil.getObjectNode(json, key)
          .map(
              storageMap ->
                  storageMap
                      .propertyStream()
                      .collect(
                          Collectors.toMap(
                              e -> UInt256.fromHexString(e.getKey()),
                              e -> UInt256.fromHexString(e.getValue().asText()))))
          .orElse(Map.of());
    }
  }
}
