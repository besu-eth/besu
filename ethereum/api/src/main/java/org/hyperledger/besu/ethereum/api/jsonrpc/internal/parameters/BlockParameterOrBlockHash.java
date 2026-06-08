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

import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Represents a block parameter (or block hash) that can be a special value ("pending", "earliest",
 * "latest", "finalized", "safe") or a number formatted as a hex string, or a block hash.
 *
 * <p>When distinguishing between a hash and a number it is presumed that a hash won't have three
 * quarters of the leading bytes as zero. This is fine for block hashes but not for precompiled
 * contracts.
 */
public class BlockParameterOrBlockHash {

  private final BlockParameterType type;
  private final OptionalLong number;
  private final Optional<Hash> blockHash;
  private final boolean requireCanonical;

  /**
   * A {@link BlockParameterOrBlockHash} representing the "latest" block. Used as the default when
   * an optional block parameter is omitted from a request (per execution-apis: default 'latest').
   */
  public static final BlockParameterOrBlockHash LATEST =
      new BlockParameterOrBlockHash(
          BlockParameterType.LATEST, OptionalLong.empty(), Optional.empty(), false);

  private BlockParameterOrBlockHash(
      final BlockParameterType type,
      final OptionalLong number,
      final Optional<Hash> blockHash,
      final boolean requireCanonical) {
    this.type = type;
    this.number = number;
    this.blockHash = blockHash;
    this.requireCanonical = requireCanonical;
  }

  @JsonCreator
  public BlockParameterOrBlockHash(final Object value) throws JsonProcessingException {
    if (value instanceof String) {
      final String normalizedValue = String.valueOf(value).toLowerCase(Locale.ROOT);

      if (Objects.equals(normalizedValue, "earliest")) {
        type = BlockParameterType.EARLIEST;
        number = OptionalLong.of(BlockHeader.GENESIS_BLOCK_NUMBER);
        blockHash = Optional.empty();
        requireCanonical = false;
      } else if (Objects.equals(normalizedValue, "latest")) {
        type = BlockParameterType.LATEST;
        number = OptionalLong.empty();
        blockHash = Optional.empty();
        requireCanonical = false;
      } else if (Objects.equals(normalizedValue, "pending")) {
        type = BlockParameterType.PENDING;
        number = OptionalLong.empty();
        blockHash = Optional.empty();
        requireCanonical = false;
      } else if (Objects.equals(normalizedValue, "safe")) {
        type = BlockParameterType.SAFE;
        number = OptionalLong.empty();
        blockHash = Optional.empty();
        requireCanonical = false;
      } else if (Objects.equals(normalizedValue, "finalized")) {
        type = BlockParameterType.FINALIZED;
        number = OptionalLong.empty();
        blockHash = Optional.empty();
        requireCanonical = false;
      } else if (normalizedValue.length() >= 65) { // with or without hex prefix
        type = BlockParameterType.HASH;
        number = OptionalLong.empty();
        blockHash = Optional.of(Hash.fromHexStringLenient(normalizedValue));
        requireCanonical = false;
      } else if (normalizedValue.length() > 16) {
        throw new IllegalArgumentException("hex number > 64 bits");
      } else if (!normalizedValue.startsWith("0x")) {
        throw new IllegalArgumentException(
            "Invalid block number: must be a hex string with 0x prefix");
      } else {
        type = BlockParameterType.NUMERIC;
        number = OptionalLong.of(Long.decode(value.toString()));
        blockHash = Optional.empty();
        requireCanonical = false;
      }
    } else {
      JsonNode jsonNode = JsonUtil.objectNodeFromString(JsonUtil.getJson(value));
      if (jsonNode.get("blockHash") != null) {
        type = BlockParameterType.HASH;
        number = OptionalLong.empty();
        blockHash = Optional.of(Hash.fromHexStringLenient(jsonNode.get("blockHash").asText()));
        if (jsonNode.get("requireCanonical") != null) {
          requireCanonical = jsonNode.get("requireCanonical").asBoolean();
        } else {
          requireCanonical = false;
        }
      } else {
        final String blockNumberText = jsonNode.get("blockNumber").asText();
        if (!blockNumberText.toLowerCase(Locale.ROOT).startsWith("0x")) {
          throw new IllegalArgumentException(
              "Invalid block number: must be a hex string with 0x prefix");
        }
        type = BlockParameterType.NUMERIC;
        number = OptionalLong.of(Long.decode(blockNumberText));
        blockHash = Optional.empty();
        requireCanonical = false;
      }
    }
  }

  public OptionalLong getNumber() {
    return number;
  }

  public Optional<Hash> getHash() {
    return blockHash;
  }

  public boolean getRequireCanonical() {
    return requireCanonical;
  }

  public boolean isPending() {
    return this.type == BlockParameterType.PENDING;
  }

  public boolean isLatest() {
    return this.type == BlockParameterType.LATEST;
  }

  public boolean isSafe() {
    return this.type == BlockParameterType.SAFE;
  }

  public boolean isFinalized() {
    return this.type == BlockParameterType.FINALIZED;
  }

  public boolean isEarliest() {
    return this.type == BlockParameterType.EARLIEST;
  }

  public boolean isNumeric() {
    return this.type == BlockParameterType.NUMERIC;
  }

  public boolean getBlockHash() {
    return this.type == BlockParameterType.HASH;
  }

  private enum BlockParameterType {
    EARLIEST,
    LATEST,
    PENDING,
    SAFE,
    FINALIZED,
    NUMERIC,
    HASH
  }
}
