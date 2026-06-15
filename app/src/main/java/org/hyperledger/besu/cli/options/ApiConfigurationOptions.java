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
package org.hyperledger.besu.cli.options;

import static java.util.Arrays.asList;

import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;

import org.slf4j.Logger;
import picocli.CommandLine;

/**
 * Handles configuration options for the API in Besu, including gas price settings, RPC log range,
 * and trace filter range.
 */
// TODO: implement CLIOption<ApiConfiguration>
public class ApiConfigurationOptions {
  /** Default constructor. */
  public ApiConfigurationOptions() {}

  @CommandLine.Option(
      names = {"--api-gas-price-blocks"},
      description =
          "Number of blocks to consider for eth_gasPrice and eth_maxPriorityFeePerGas (default: ${DEFAULT-VALUE}). "
              + "Set to 0 to disable block sampling and use the respective configuration options instead.")
  private final Long apiGasPriceBlocks = 100L;

  @CommandLine.Option(
      names = {"--api-gas-price-percentile"},
      description = "Percentile value to measure for eth_gasPrice (default: ${DEFAULT-VALUE})")
  private final Double apiGasPricePercentile = 50.0;

  @CommandLine.Option(
      names = {"--estimate-gas-tolerance-ratio"},
      description = "Decimal ratio for eth_estimateGas tolerance (default: ${DEFAULT-VALUE})")
  private final Double estimateGasToleranceRatio = 0.015;

  @CommandLine.Option(
      names = {"--api-gas-price-max"},
      description = "Maximum gas price for eth_gasPrice (default: ${DEFAULT-VALUE})")
  private final Long apiGasPriceMax = 500_000_000_000L;

  @CommandLine.Option(
      names = {"--api-gas-and-priority-fee-limiting-enabled"},
      hidden = true,
      description =
          "Set to enable gas price and minimum priority fee limit in eth_getGasPrice and eth_feeHistory (default: ${DEFAULT-VALUE})")
  private final Boolean apiGasAndPriorityFeeLimitingEnabled = false;

  @CommandLine.Option(
      names = {"--api-gas-and-priority-fee-lower-bound-coefficient"},
      hidden = true,
      description =
          "Coefficient for setting the lower limit of gas price and minimum priority fee in eth_getGasPrice and eth_feeHistory (default: ${DEFAULT-VALUE})")
  private final Long apiGasAndPriorityFeeLowerBoundCoefficient =
      ApiConfiguration.DEFAULT_LOWER_BOUND_GAS_AND_PRIORITY_FEE_COEFFICIENT;

  @CommandLine.Option(
      names = {"--api-gas-and-priority-fee-upper-bound-coefficient"},
      hidden = true,
      description =
          "Coefficient for setting the upper limit of gas price and minimum priority fee in eth_getGasPrice and eth_feeHistory (default: ${DEFAULT-VALUE})")
  private final Long apiGasAndPriorityFeeUpperBoundCoefficient =
      ApiConfiguration.DEFAULT_UPPER_BOUND_GAS_AND_PRIORITY_FEE_COEFFICIENT;

  @CommandLine.Option(
      names = {"--rpc-max-logs-range"},
      description =
          "Specifies the maximum number of blocks to retrieve logs from via RPC. Must be >=0. 0 specifies no limit  (default: ${DEFAULT-VALUE})")
  private final Long rpcMaxLogsRange = 5000L;

  @CommandLine.Option(
      names = {"--rpc-gas-cap"},
      description =
          "Specifies the gasLimit cap for transaction simulation RPC methods. Must be >=0. 0 specifies no limit  (default: ${DEFAULT-VALUE})")
  private final Long rpcGasCap = ApiConfiguration.DEFAULT_GAS_CAP;

  @CommandLine.Option(
      names = {"--rpc-max-trace-filter-range"},
      description =
          "Specifies the maximum number of blocks for the trace_filter method. Must be >=0. 0 specifies no limit  (default: ${DEFAULT-VALUE})")
  private final Long maxTraceFilterRange = 1000L;

  /**
   * Validates the API options.
   *
   * @param commandLine CommandLine instance
   * @param logger Logger instance
   */
  public void validate(final CommandLine commandLine, final Logger logger) {
    if (apiGasAndPriorityFeeLimitingEnabled) {
      if (apiGasAndPriorityFeeLowerBoundCoefficient > apiGasAndPriorityFeeUpperBoundCoefficient) {
        throw new CommandLine.ParameterException(
            commandLine,
            "--api-gas-and-priority-fee-lower-bound-coefficient cannot be greater than the value of --api-gas-and-priority-fee-upper-bound-coefficient");
      }
    }
    checkApiOptionsDependencies(commandLine, logger);
  }

  private void checkApiOptionsDependencies(final CommandLine commandLine, final Logger logger) {
    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--api-gas-and-priority-fee-limiting-enabled",
        !apiGasAndPriorityFeeLimitingEnabled,
        asList(
            "--api-gas-and-priority-fee-upper-bound-coefficient",
            "--api-gas-and-priority-fee-lower-bound-coefficient"));
  }

  /**
   * Creates an ApiConfiguration based on the provided options.
   *
   * @return An ApiConfiguration instance
   */
  public ApiConfiguration apiConfiguration() {
    var builder =
        ImmutableApiConfiguration.builder()
            .gasPriceBlocks(apiGasPriceBlocks)
            .gasPricePercentile(apiGasPricePercentile)
            .gasPriceMax(Wei.of(apiGasPriceMax))
            .estimateGasToleranceRatio(estimateGasToleranceRatio)
            .maxLogsRange(rpcMaxLogsRange)
            .gasCap(rpcGasCap)
            .isGasAndPriorityFeeLimitingEnabled(apiGasAndPriorityFeeLimitingEnabled)
            .maxTraceFilterRange(maxTraceFilterRange);
    if (apiGasAndPriorityFeeLimitingEnabled) {
      builder
          .lowerBoundGasAndPriorityFeeCoefficient(apiGasAndPriorityFeeLowerBoundCoefficient)
          .upperBoundGasAndPriorityFeeCoefficient(apiGasAndPriorityFeeUpperBoundCoefficient);
    }
    return builder.build();
  }
}
