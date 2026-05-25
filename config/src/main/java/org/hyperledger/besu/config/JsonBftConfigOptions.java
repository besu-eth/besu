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

import org.hyperledger.besu.datatypes.Address;

import java.math.BigInteger;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Json Bft config options. */
public class JsonBftConfigOptions implements BftConfigOptions {

  private static final Logger LOG = LoggerFactory.getLogger(JsonBftConfigOptions.class);

  /** The constant DEFAULT. */
  public static final JsonBftConfigOptions DEFAULT =
      new JsonBftConfigOptions(JsonUtil.createEmptyObjectNode());

  /** The constant EMPTY_BLOCK_PERIOD_SECONDS_KEY. */
  public static final String EMPTY_BLOCK_PERIOD_SECONDS_KEY = "emptyblockperiodseconds";

  /**
   * The deprecated config key for the empty block period (replaced by {@link
   * #EMPTY_BLOCK_PERIOD_SECONDS_KEY}).
   */
  @Deprecated
  public static final String X_EMPTY_BLOCK_PERIOD_SECONDS_KEY = "xemptyblockperiodseconds";

  private static final long DEFAULT_EPOCH_LENGTH = 30_000;
  private static final int DEFAULT_BLOCK_PERIOD_SECONDS = 1;
  // 0 keeps working as before, increase to activate it
  private static final int DEFAULT_EMPTY_BLOCK_PERIOD_SECONDS = 0;
  private static final int DEFAULT_BLOCK_PERIOD_MILLISECONDS = 0; // Experimental for test only
  private static final int DEFAULT_ROUND_EXPIRY_SECONDS = 1;
  // In a healthy network this can be very small. This default limit will allow for suitable
  // protection for on a typical 20 node validator network with multiple rounds
  private static final int DEFAULT_GOSSIPED_HISTORY_LIMIT = 1000;
  private static final int DEFAULT_MESSAGE_QUEUE_LIMIT = 1000;
  private static final int DEFAULT_DUPLICATE_MESSAGE_LIMIT = 100;
  private static final int DEFAULT_FUTURE_MESSAGES_LIMIT = 1000;
  private static final int DEFAULT_FUTURE_MESSAGES_MAX_DISTANCE = 10;

  /** The Bft config root. */
  protected final ObjectNode bftConfigRoot;

  // Ensures the deprecation warning for xemptyblockperiodseconds is emitted at most once
  // per JsonBftConfigOptions instance.
  private final AtomicBoolean emptyBlockPeriodDeprecationWarned = new AtomicBoolean(false);

  /**
   * Instantiates a new Json bft config options.
   *
   * @param bftConfigRoot the bft config root
   */
  public JsonBftConfigOptions(final ObjectNode bftConfigRoot) {
    this.bftConfigRoot = bftConfigRoot;
  }

  @Override
  public long getEpochLength() {
    return JsonUtil.getLong(bftConfigRoot, "epochlength", DEFAULT_EPOCH_LENGTH);
  }

  @Override
  public int getBlockPeriodSeconds() {
    return JsonUtil.getPositiveInt(
        bftConfigRoot, "blockperiodseconds", DEFAULT_BLOCK_PERIOD_SECONDS);
  }

  @Override
  public int getEmptyBlockPeriodSeconds() {
    final boolean hasNewKey = bftConfigRoot.has(EMPTY_BLOCK_PERIOD_SECONDS_KEY);
    final boolean hasDeprecatedKey = bftConfigRoot.has(X_EMPTY_BLOCK_PERIOD_SECONDS_KEY);
    if (hasDeprecatedKey) {
      maybeLogEmptyBlockPeriodDeprecationWarning(hasNewKey);
    }
    if (hasNewKey) {
      return JsonUtil.getInt(
          bftConfigRoot, EMPTY_BLOCK_PERIOD_SECONDS_KEY, DEFAULT_EMPTY_BLOCK_PERIOD_SECONDS);
    }
    return JsonUtil.getInt(
        bftConfigRoot, X_EMPTY_BLOCK_PERIOD_SECONDS_KEY, DEFAULT_EMPTY_BLOCK_PERIOD_SECONDS);
  }

  private void maybeLogEmptyBlockPeriodDeprecationWarning(final boolean newKeyAlsoSet) {
    if (!emptyBlockPeriodDeprecationWarned.compareAndSet(false, true)) {
      return;
    }
    if (newKeyAlsoSet) {
      LOG.warn(
          "Genesis BFT config specifies both '{}' (deprecated) and '{}'. "
              + "The deprecated '{}' is being ignored; please remove it from the genesis file.",
          X_EMPTY_BLOCK_PERIOD_SECONDS_KEY,
          EMPTY_BLOCK_PERIOD_SECONDS_KEY,
          X_EMPTY_BLOCK_PERIOD_SECONDS_KEY);
    } else {
      LOG.warn(
          "Genesis BFT config uses deprecated option '{}'. "
              + "Please rename it to '{}'. The deprecated name will be removed in a future release.",
          X_EMPTY_BLOCK_PERIOD_SECONDS_KEY,
          EMPTY_BLOCK_PERIOD_SECONDS_KEY);
    }
  }

  @Override
  public long getBlockPeriodMilliseconds() {
    return JsonUtil.getLong(
        bftConfigRoot, "xblockperiodmilliseconds", DEFAULT_BLOCK_PERIOD_MILLISECONDS);
  }

  @Override
  public int getRequestTimeoutSeconds() {
    return JsonUtil.getInt(bftConfigRoot, "requesttimeoutseconds", DEFAULT_ROUND_EXPIRY_SECONDS);
  }

  @Override
  public int getGossipedHistoryLimit() {
    return JsonUtil.getInt(bftConfigRoot, "gossipedhistorylimit", DEFAULT_GOSSIPED_HISTORY_LIMIT);
  }

  @Override
  public int getMessageQueueLimit() {
    return JsonUtil.getInt(bftConfigRoot, "messagequeuelimit", DEFAULT_MESSAGE_QUEUE_LIMIT);
  }

  @Override
  public int getDuplicateMessageLimit() {
    return JsonUtil.getInt(bftConfigRoot, "duplicatemessagelimit", DEFAULT_DUPLICATE_MESSAGE_LIMIT);
  }

  @Override
  public int getFutureMessagesLimit() {
    return JsonUtil.getInt(bftConfigRoot, "futuremessageslimit", DEFAULT_FUTURE_MESSAGES_LIMIT);
  }

  @Override
  public int getFutureMessagesMaxDistance() {
    return JsonUtil.getInt(
        bftConfigRoot, "futuremessagesmaxdistance", DEFAULT_FUTURE_MESSAGES_MAX_DISTANCE);
  }

  @Override
  public Optional<Address> getMiningBeneficiary() {
    try {
      return JsonUtil.getString(bftConfigRoot, "miningbeneficiary")
          .map(String::trim)
          .filter(s -> !s.isBlank())
          .map(Address::fromHexStringStrict);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Mining beneficiary in config is not a valid ethereum address", e);
    }
  }

  @Override
  public BigInteger getBlockRewardWei() {
    final Optional<String> configFileContent = JsonUtil.getString(bftConfigRoot, "blockreward");

    if (configFileContent.isEmpty()) {
      return BigInteger.ZERO;
    }
    final String weiStr = configFileContent.get();
    if (weiStr.toLowerCase(Locale.ROOT).startsWith("0x")) {
      return new BigInteger(1, Bytes.fromHexStringLenient(weiStr).toArrayUnsafe());
    }
    return new BigInteger(weiStr);
  }

  @Override
  public Map<String, Object> asMap() {
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    if (bftConfigRoot.has("epochlength")) {
      builder.put("epochLength", getEpochLength());
    }
    if (bftConfigRoot.has("blockperiodseconds")) {
      builder.put("blockPeriodSeconds", getBlockPeriodSeconds());
    }
    if (bftConfigRoot.has("xblockperiodmilliseconds")) {
      builder.put("xBlockPeriodMilliSeconds", getBlockPeriodMilliseconds());
    }
    if (bftConfigRoot.has("requesttimeoutseconds")) {
      builder.put("requestTimeoutSeconds", getRequestTimeoutSeconds());
    }
    if (bftConfigRoot.has("gossipedhistorylimit")) {
      builder.put("gossipedHistoryLimit", getGossipedHistoryLimit());
    }
    if (bftConfigRoot.has("messagequeuelimit")) {
      builder.put("messageQueueLimit", getMessageQueueLimit());
    }
    if (bftConfigRoot.has("duplicatemessagelimit")) {
      builder.put("duplicateMessageLimit", getDuplicateMessageLimit());
    }
    if (bftConfigRoot.has("futuremessageslimit")) {
      builder.put("futureMessagesLimit", getFutureMessagesLimit());
    }
    if (bftConfigRoot.has("futuremessagesmaxdistance")) {
      builder.put("futureMessagesMaxDistance", getFutureMessagesMaxDistance());
    }
    return builder.build();
  }
}
