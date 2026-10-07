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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.cli.CommandTestAbstract;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ApiConfigurationOptionsTest extends CommandTestAbstract {

  @Test
  public void apiPriorityFeeLimitingEnabledOptionMustBeUsed() {
    parseCommand("--api-gas-and-priority-fee-limiting-enabled");
    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();
    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(
            ImmutableApiConfiguration.builder().isGasAndPriorityFeeLimitingEnabled(true).build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void apiPriorityFeeLowerBoundCoefficientOptionMustBeUsed() {
    final long lowerBound = 150L;
    parseCommand(
        "--api-gas-and-priority-fee-lower-bound-coefficient",
        Long.toString(lowerBound),
        "--api-gas-and-priority-fee-limiting-enabled");
    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();
    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(
            ImmutableApiConfiguration.builder()
                .lowerBoundGasAndPriorityFeeCoefficient(lowerBound)
                .isGasAndPriorityFeeLimitingEnabled(true)
                .build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void
      apiPriorityFeeLowerBoundCoefficients_MustNotBeGreaterThan_apiPriorityFeeUpperBoundCoefficient() {
    final long lowerBound = 200L;
    final long upperBound = 100L;

    parseCommand(
        "--api-gas-and-priority-fee-limiting-enabled",
        "--api-gas-and-priority-fee-lower-bound-coefficient",
        Long.toString(lowerBound),
        "--api-gas-and-priority-fee-upper-bound-coefficient",
        Long.toString(upperBound));
    Mockito.verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "--api-gas-and-priority-fee-lower-bound-coefficient cannot be greater than the value of --api-gas-and-priority-fee-upper-bound-coefficient");
  }

  @Test
  public void apiPriorityFeeUpperBoundCoefficientsOptionMustBeUsed() {
    final long upperBound = 200L;
    parseCommand(
        "--api-gas-and-priority-fee-upper-bound-coefficient",
        Long.toString(upperBound),
        "--api-gas-and-priority-fee-limiting-enabled");
    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();
    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(
            ImmutableApiConfiguration.builder()
                .upperBoundGasAndPriorityFeeCoefficient(upperBound)
                .isGasAndPriorityFeeLimitingEnabled(true)
                .build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcMaxLogsRangeOptionMustBeUsed() {

    final long rpcMaxLogsRange = 150L;
    parseCommand("--rpc-max-logs-range", Long.toString(rpcMaxLogsRange));

    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(ImmutableApiConfiguration.builder().maxLogsRange(rpcMaxLogsRange).build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcGasCapOptionMustBeUsed() {
    final long rpcGasCap = 150L;
    parseCommand("--rpc-gas-cap", Long.toString(rpcGasCap));

    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(ImmutableApiConfiguration.builder().gasCap(rpcGasCap).build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcGasCapDefault() {
    parseCommand();

    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(
            ImmutableApiConfiguration.builder().gasCap(ApiConfiguration.DEFAULT_GAS_CAP).build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcGasCapAcceptsZero() {
    final long rpcGasCap = 0L;
    parseCommand("--rpc-gas-cap", Long.toString(rpcGasCap));

    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(ImmutableApiConfiguration.builder().gasCap(rpcGasCap).build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcMaxTraceFilterOptionMustBeUsed() {
    final long rpcMaxTraceFilterOption = 150L;
    parseCommand("--rpc-max-trace-filter-range", Long.toString(rpcMaxTraceFilterOption));

    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(
            ImmutableApiConfiguration.builder()
                .maxTraceFilterRange(rpcMaxTraceFilterOption)
                .build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcMaxActiveFiltersOptionMustBeUsed() {
    final int rpcMaxActiveFilters = 256;
    parseCommand("--rpc-max-active-filters", Long.toString(rpcMaxActiveFilters));

    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(ImmutableApiConfiguration.builder().maxFilterCount(rpcMaxActiveFilters).build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcFilterTimeoutOptionMustBeUsed() {
    final long rpcFilterTimeoutSeconds = 120L;
    parseCommand("--rpc-filter-timeout-seconds", Long.toString(rpcFilterTimeoutSeconds));

    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(
            ImmutableApiConfiguration.builder()
                .filterTimeout(Duration.ofSeconds(rpcFilterTimeoutSeconds))
                .build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcFilterTimeoutMustBePositive() {
    parseCommand("--rpc-filter-timeout-seconds", "0");

    Mockito.verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("--rpc-filter-timeout-seconds must be > 0");
  }

  @Test
  public void estimateGasToleranceRatioOptionMustBeUsed() {
    final double tolerance = 0.5d;
    parseCommand("--estimate-gas-tolerance-ratio", Double.toString(tolerance));

    verify(mockRunnerBuilder).apiConfiguration(apiConfigurationCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(apiConfigurationCaptor.getValue())
        .isEqualTo(
            ImmutableApiConfiguration.builder().estimateGasToleranceRatio(tolerance).build());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }
}
