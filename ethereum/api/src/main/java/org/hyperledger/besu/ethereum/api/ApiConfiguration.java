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
package org.hyperledger.besu.ethereum.api;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthEstimateGas;

import java.time.Duration;

import org.immutables.value.Value;

/**
 * The ApiConfiguration class provides configuration for the API. It includes default values for gas
 * price, max logs range, gas cap, and other parameters.
 */
@Value.Immutable
@Value.Style(allParameters = true)
public abstract class ApiConfiguration {

  /**
   * The default lower bound coefficient for gas and priority fee. This value is used as the default
   * lower bound when calculating the gas and priority fee.
   */
  public static final long DEFAULT_LOWER_BOUND_GAS_AND_PRIORITY_FEE_COEFFICIENT = 0L;

  /**
   * The default upper bound coefficient for gas and priority fee. This value is used as the default
   * upper bound when calculating the gas and priority fee.
   */
  public static final long DEFAULT_UPPER_BOUND_GAS_AND_PRIORITY_FEE_COEFFICIENT = Long.MAX_VALUE;

  /** The default gas cap used for transaction simulation */
  public static final long DEFAULT_GAS_CAP = 100_000_000L;

  /** The default maximum number of concurrently active JSON-RPC filters. */
  public static final int DEFAULT_MAX_FILTER_COUNT = 1000;

  /** The default duration a JSON-RPC filter stays active without being polled. */
  public static final Duration DEFAULT_FILTER_TIMEOUT = Duration.ofMinutes(2);

  /** Constructs a new ApiConfiguration with default values. */
  protected ApiConfiguration() {}

  /**
   * Returns the number of blocks to consider for gas price calculations. Default value is 100.
   *
   * @return the number of blocks for gas price calculations
   */
  @Value.Default
  public long getGasPriceBlocks() {
    return 100;
  }

  /**
   * Returns the percentile to use for gas price calculations. Default value is 50.0.
   *
   * @return the percentile for gas price calculations
   */
  @Value.Default
  public double getGasPricePercentile() {
    return 50.0d;
  }

  /**
   * Returns the ratio to use for eth_estimateGas tolerance. Default value is 0.015. This is
   * effectively how "close" the estimate is required to be. See {@link EthEstimateGas} for how this
   * is used in calculations.
   *
   * @return the decimal ratio to use for eth_estimateGas tolerance
   */
  @Value.Default
  public double getEstimateGasToleranceRatio() {
    return 0.015d;
  }

  /**
   * Returns the maximum gas price. Default value is 500 GWei.
   *
   * @return the maximum gas price
   */
  @Value.Default
  public Wei getGasPriceMax() {
    return Wei.of(500_000_000_000L); // 500 GWei
  }

  /**
   * Returns the fraction to use for gas price calculations. This is derived from the gas price
   * percentile.
   *
   * @return the fraction for gas price calculations
   */
  @Value.Derived
  public double getGasPriceFraction() {
    return getGasPricePercentile() / 100.0;
  }

  /**
   * Returns the maximum range for logs. Default value is 5000.
   *
   * @return the maximum range for logs
   */
  @Value.Default
  public Long getMaxLogsRange() {
    return 5000L;
  }

  /**
   * Returns the gas cap. Default value is 50M.
   *
   * @return the gas cap
   */
  @Value.Default
  public Long getGasCap() {
    return DEFAULT_GAS_CAP;
  }

  /**
   * Returns whether gas and priority fee limiting is enabled. Default value is false.
   *
   * @return true if gas and priority fee limiting is enabled, false otherwise
   */
  @Value.Default
  public boolean isGasAndPriorityFeeLimitingEnabled() {
    return false;
  }

  /**
   * Returns the lower bound coefficient for gas and priority fee. Default value is 0.
   *
   * @return the lower bound coefficient for gas and priority fee
   */
  @Value.Default
  public Long getLowerBoundGasAndPriorityFeeCoefficient() {
    return DEFAULT_LOWER_BOUND_GAS_AND_PRIORITY_FEE_COEFFICIENT;
  }

  /**
   * Returns the upper bound coefficient for gas and priority fee. Default value is Long.MAX_VALUE.
   *
   * @return the upper bound coefficient for gas and priority fee
   */
  @Value.Default
  public Long getUpperBoundGasAndPriorityFeeCoefficient() {
    return DEFAULT_UPPER_BOUND_GAS_AND_PRIORITY_FEE_COEFFICIENT;
  }

  /**
   * Returns the maximum range for trace filter. Default value is 1000.
   *
   * @return the maximum range for trace filter
   */
  @Value.Default
  public Long getMaxTraceFilterRange() {
    return 1000L;
  }

  /**
   * Returns the maximum number of concurrently active JSON-RPC filters (created via eth_newFilter,
   * eth_newBlockFilter and eth_newPendingTransactionFilter). Creation past this limit is rejected.
   *
   * @return the maximum number of concurrently active filters
   */
  @Value.Default
  public Integer getMaxFilterCount() {
    return DEFAULT_MAX_FILTER_COUNT;
  }

  /**
   * Returns the duration a JSON-RPC filter (eth_newFilter, eth_newBlockFilter,
   * eth_newPendingTransactionFilter) stays active without being polled before the expiry monitor
   * sweeps it.
   *
   * @return the filter expiry duration
   */
  @Value.Default
  public Duration getFilterTimeout() {
    return DEFAULT_FILTER_TIMEOUT;
  }
}
