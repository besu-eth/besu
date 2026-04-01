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

import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;

import java.time.Duration;

import picocli.CommandLine;

/** Command-line options for configuring Block Access List behaviour. */
public class BalConfigurationOptions {
  /** Default constructor. */
  public BalConfigurationOptions() {}

  @CommandLine.Option(
      names = {"--Xbal-perfect-parallelization-enabled"},
      hidden = true,
      description =
          "Allows disabling BAL-based perfect parallelization even when BALs are present.")
  boolean balPerfectParallelizationEnabled = true;

  @CommandLine.Option(
      names = {"--Xbal-lenient-on-state-root-mismatch"},
      hidden = true,
      description =
          "Log an error instead of throwing when the BAL-computed state root does not match the synchronously computed root.")
  boolean balLenientOnStateRootMismatch = true;

  @CommandLine.Option(
      names = {"--Xbal-trust-state-root"},
      hidden = true,
      description = "Trust the BAL-computed state root without verification.")
  boolean balTrustStateRoot = false;

  @CommandLine.Option(
      names = {"--Xbal-log-bals-on-mismatch"},
      hidden = true,
      description = "Log the constructed and block's BAL when they differ.")
  boolean balLogBalsOnMismatch = false;

  @CommandLine.Option(
      names = {"--Xbal-state-root-timeout"},
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Timeout in milliseconds when waiting for the BAL-computed state root.")
  private long balStateRootTimeoutMs = Duration.ofSeconds(1).toMillis();

  @CommandLine.Option(
      names = {"--Xbal-processing-timeout"},
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Timeout in milliseconds when waiting for BAL transaction processing results.")
  private long balProcessingTimeoutMs = Duration.ofSeconds(1).toMillis();

  /**
   * Builds the immutable {@link BalConfiguration} corresponding to the parsed CLI options.
   *
   * @return an immutable BAL configuration reflecting the current option values
   */
  public BalConfiguration toDomainObject() {
    return ImmutableBalConfiguration.builder()
        .isPerfectParallelizationEnabled(balPerfectParallelizationEnabled)
        .shouldLogBalsOnMismatch(balLogBalsOnMismatch)
        .isBalLenientOnStateRootMismatch(balLenientOnStateRootMismatch)
        .isBalStateRootTrusted(balTrustStateRoot)
        .balStateRootTimeout(Duration.ofMillis(balStateRootTimeoutMs))
        .balProcessingTimeout(Duration.ofMillis(balProcessingTimeoutMs))
        .build();
  }
}
