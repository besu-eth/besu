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
package org.hyperledger.besu.evm.precompile;

import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.util.Arrays;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Skeleton class for {@link PrecompiledContract} implementations. */
@SuppressWarnings("unused")
public abstract class AbstractPrecompiledContract implements PrecompiledContract {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractPrecompiledContract.class);

  private final GasCalculator gasCalculator;

  private final String name;

  /**
   * Instantiates a new Abstract precompiled contract.
   *
   * @param name the name
   * @param gasCalculator the gas calculator
   */
  protected AbstractPrecompiledContract(final String name, final GasCalculator gasCalculator) {
    this.name = name;
    this.gasCalculator = gasCalculator;
  }

  /**
   * Gas calculator.
   *
   * @return the gas calculator
   */
  protected GasCalculator gasCalculator() {
    return gasCalculator;
  }

  @Override
  public String getName() {
    return name;
  }

  /** Default result caching to false unless otherwise set. */
  protected static Boolean enableResultCaching = Boolean.FALSE;

  /**
   * Enable or disable precompile result caching.
   *
   * @param enablePrecompileCaching boolean indicating whether to cache precompile results
   */
  public static void setPrecompileCaching(final boolean enablePrecompileCaching) {
    enableResultCaching = enablePrecompileCaching;
  }

  /** enum for precompile cache metric */
  public enum CacheMetric {
    /** a successful cache hit metric */
    HIT,
    /** a cache miss metric */
    MISS,
    /** a false positive cache hit metric */
    FALSE_POSITIVE
  }

  /**
   * record type used for cache event
   *
   * @param precompile precompile name
   * @param cacheMetric cache metric type (hit, miss, false positive).
   */
  public record CacheEvent(String precompile, CacheMetric cacheMetric) {}

  static Consumer<CacheEvent> cacheEventConsumer = __ -> {};

  /**
   * Set an optional cache event consumer, such as a metrics system logger.
   *
   * @param eventConsumer consumer of the CacheEvent.
   */
  public static void setCacheEventConsumer(final Consumer<CacheEvent> eventConsumer) {
    cacheEventConsumer = eventConsumer;
  }

  /**
   * Calculate a cache key over the leading {@code prefixLen} bytes of {@code input}. Bytes beyond
   * the semantic prefix are ignored.
   *
   * @param input bytes
   * @param prefixLen number of leading bytes to include in the key
   * @return integer cache key
   */
  public static Integer getCacheKey(final Bytes input, final int prefixLen) {
    final int len = Math.min(prefixLen, input.size());
    return Arrays.hashCode(input.slice(0, len).toArray());
  }
}
