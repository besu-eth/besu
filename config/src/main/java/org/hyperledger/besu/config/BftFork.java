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

import org.hyperledger.besu.datatypes.Address;

import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Bft fork. */
public class BftFork implements Fork {

  private static final Logger LOG = LoggerFactory.getLogger(BftFork.class);

  /** The constant FORK_BLOCK_KEY. */
  public static final String FORK_BLOCK_KEY = "block";

  /** The constant VALIDATORS_KEY. */
  public static final String VALIDATORS_KEY = "validators";

  /** The constant BLOCK_PERIOD_SECONDS_KEY. */
  public static final String BLOCK_PERIOD_SECONDS_KEY = "blockperiodseconds";

  /**
   * The deprecated config key for the empty block period (replaced by {@link
   * #EMPTY_BLOCK_PERIOD_SECONDS_KEY}).
   */
  @Deprecated
  public static final String X_EMPTY_BLOCK_PERIOD_SECONDS_KEY = "xemptyblockperiodseconds";

  /** The constant EMPTY_BLOCK_PERIOD_SECONDS_KEY. */
  public static final String EMPTY_BLOCK_PERIOD_SECONDS_KEY = "emptyblockperiodseconds";

  /** The constant BLOCK_PERIOD_MILLISECONDS_KEY. */
  public static final String BLOCK_PERIOD_MILLISECONDS_KEY = "xblockperiodmilliseconds";

  /** The constant BLOCK_REWARD_KEY. */
  public static final String BLOCK_REWARD_KEY = "blockreward";

  /** The constant MINING_BENEFICIARY_KEY. */
  public static final String MINING_BENEFICIARY_KEY = "miningbeneficiary";

  /** The Fork config root. */
  protected final ObjectNode forkConfigRoot;

  // Ensures the deprecation warning for xemptyblockperiodseconds is emitted at most once
  // per BftFork instance.
  private final AtomicBoolean emptyBlockPeriodDeprecationWarned = new AtomicBoolean(false);

  /**
   * Instantiates a new Bft fork.
   *
   * @param forkConfigRoot the fork config root
   */
  @JsonCreator
  public BftFork(final ObjectNode forkConfigRoot) {
    this.forkConfigRoot = forkConfigRoot;
  }

  /**
   * Gets fork block.
   *
   * @return the fork block
   */
  @Override
  public long getForkBlock() {
    return JsonUtil.getLong(forkConfigRoot, FORK_BLOCK_KEY)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Fork block not specified for Bft fork in custom forks"));
  }

  /**
   * Gets block period seconds.
   *
   * @return the block period seconds
   */
  public OptionalInt getBlockPeriodSeconds() {
    return JsonUtil.getPositiveInt(forkConfigRoot, BLOCK_PERIOD_SECONDS_KEY);
  }

  /**
   * Gets empty block period seconds.
   *
   * <p>If both the new and deprecated keys are set in this fork config, the new key wins and a
   * deprecation warning is emitted. If only the deprecated key is set, it is used and a deprecation
   * warning is emitted.
   *
   * @return the empty block period seconds
   */
  public OptionalInt getEmptyBlockPeriodSeconds() {
    // It can be 0 to disable custom empty block periods
    final OptionalInt newValue = JsonUtil.getInt(forkConfigRoot, EMPTY_BLOCK_PERIOD_SECONDS_KEY);
    final OptionalInt deprecatedValue =
        JsonUtil.getInt(forkConfigRoot, X_EMPTY_BLOCK_PERIOD_SECONDS_KEY);
    if (deprecatedValue.isPresent()) {
      maybeLogEmptyBlockPeriodDeprecationWarning(newValue.isPresent());
    }
    return newValue.isPresent() ? newValue : deprecatedValue;
  }

  private void maybeLogEmptyBlockPeriodDeprecationWarning(final boolean newKeyAlsoSet) {
    if (!emptyBlockPeriodDeprecationWarned.compareAndSet(false, true)) {
      return;
    }
    final long forkBlock = JsonUtil.getLong(forkConfigRoot, FORK_BLOCK_KEY).orElse(-1L);
    if (newKeyAlsoSet) {
      LOG.warn(
          "Transition fork at block {} specifies both '{}' (deprecated) and '{}'. "
              + "The deprecated '{}' is being ignored; please remove it from the transition config.",
          forkBlock,
          X_EMPTY_BLOCK_PERIOD_SECONDS_KEY,
          EMPTY_BLOCK_PERIOD_SECONDS_KEY,
          X_EMPTY_BLOCK_PERIOD_SECONDS_KEY);
    } else {
      LOG.warn(
          "Transition fork at block {} uses deprecated option '{}'. "
              + "Please rename it to '{}'. The deprecated name will be removed in a future release.",
          forkBlock,
          X_EMPTY_BLOCK_PERIOD_SECONDS_KEY,
          EMPTY_BLOCK_PERIOD_SECONDS_KEY);
    }
  }

  /**
   * Gets block period milliseconds. Experimental for test scenarios only.
   *
   * @return the block period milliseconds
   */
  public OptionalLong getBlockPeriodMilliseconds() {
    return JsonUtil.getLong(forkConfigRoot, BLOCK_PERIOD_MILLISECONDS_KEY);
  }

  /**
   * Gets block reward wei.
   *
   * @return the block reward wei
   */
  public Optional<BigInteger> getBlockRewardWei() {
    final Optional<String> configFileContent = JsonUtil.getString(forkConfigRoot, BLOCK_REWARD_KEY);

    if (configFileContent.isEmpty()) {
      return Optional.empty();
    }
    final String weiStr = configFileContent.get();
    if (weiStr.toLowerCase(Locale.ROOT).startsWith("0x")) {
      return Optional.of(new BigInteger(1, Bytes.fromHexStringLenient(weiStr).toArrayUnsafe()));
    }
    return Optional.of(new BigInteger(weiStr));
  }

  /**
   * Gets mining beneficiary.
   *
   * @return the mining beneficiary
   */
  public Optional<Address> getMiningBeneficiary() {
    try {
      return JsonUtil.getString(forkConfigRoot, MINING_BENEFICIARY_KEY)
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .map(Address::fromHexStringStrict);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Mining beneficiary in transition config is not a valid ethereum address", e);
    }
  }

  /**
   * Is mining beneficiary configured boolean.
   *
   * @return the boolean
   */
  public boolean isMiningBeneficiaryConfigured() {
    return JsonUtil.hasKey(forkConfigRoot, MINING_BENEFICIARY_KEY);
  }

  /**
   * Gets validators.
   *
   * @return the validators
   * @throws IllegalArgumentException the illegal argument exception
   */
  public Optional<List<String>> getValidators() throws IllegalArgumentException {
    final Optional<ArrayNode> validatorNode = JsonUtil.getArrayNode(forkConfigRoot, VALIDATORS_KEY);

    if (validatorNode.isEmpty()) {
      return Optional.empty();
    }

    List<String> validators = Lists.newArrayList();
    validatorNode
        .get()
        .elements()
        .forEachRemaining(
            value -> {
              if (!value.isTextual()) {
                throw new IllegalArgumentException(
                    "Bft Validator fork does not contain a string " + value);
              }

              validators.add(value.asText());
            });
    return Optional.of(validators);
  }
}
