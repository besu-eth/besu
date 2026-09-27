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

import org.hyperledger.besu.ethereum.chain.ChainDataPruner.ChainPruningStrategy;
import org.hyperledger.besu.ethereum.chain.ChainPrunerConfiguration;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.util.Arrays;
import java.util.List;

import picocli.CommandLine;

/** The Chain pruning CLI options. */
public class ChainPruningOptions implements CLIOptions<ChainPrunerConfiguration> {
  private static final String CHAIN_PRUNING_ENABLED_FLAG = "--Xchain-pruning-enabled";
  private static final String CHAIN_PRUNING_BLOCKS_RETAINED_FLAG =
      "--Xchain-pruning-blocks-retained";
  private static final String CHAIN_PRUNING_RETAINED_MINIMUM_FLAG =
      "--Xchain-pruning-retained-minimum";
  private static final String CHAIN_PRUNING_FREQUENCY_FLAG = "--Xchain-pruning-frequency";

  /**
   * EIP-4444 {@code HISTORY_PRUNE_EPOCHS}: the consensus-layer block retention window, equal to the
   * CL's {@code MIN_EPOCHS_FOR_BLOCK_REQUESTS}.
   */
  public static final long HISTORY_PRUNE_EPOCHS = 33_024L;

  private static final long SLOTS_PER_EPOCH = 32L;

  /**
   * {@link #HISTORY_PRUNE_EPOCHS} in blocks: the default for the retention option and the floor
   * below which it may not be set.
   */
  public static final long CHAIN_DATA_PRUNING_RETAINED_MINIMUM =
      HISTORY_PRUNE_EPOCHS * SLOTS_PER_EPOCH;

  /** The constant DEFAULT_CHAIN_DATA_PRUNING_FREQUENCY. */
  public static final int DEFAULT_CHAIN_DATA_PRUNING_FREQUENCY = 256;

  /**
   * Blocks retained beyond the configured window, keeping the advertised eth/69 range backed by
   * data between the fixed-interval BlockRangeUpdate broadcasts.
   */
  public static final long DEFAULT_PRUNE_SLACK_BLOCKS = 4096;

  @CommandLine.Option(
      hidden = true,
      names = {CHAIN_PRUNING_ENABLED_FLAG},
      description =
          "Enable the chain pruner to actively prune old chain data. Options: ALL (blocks and BALs), BAL (only BALs), NONE (disabled) (default: ${DEFAULT-VALUE})")
  private final ChainPruningStrategy chainPruningStrategy = ChainPruningStrategy.BAL;

  @CommandLine.Option(
      hidden = true,
      names = {CHAIN_PRUNING_BLOCKS_RETAINED_FLAG},
      description =
          "The number of recent blocks for which to keep the chain data. Should be >= "
              + CHAIN_DATA_PRUNING_RETAINED_MINIMUM
              + " (default: ${DEFAULT-VALUE})")
  private final Long chainDataPruningBlocksRetained = CHAIN_DATA_PRUNING_RETAINED_MINIMUM;

  @CommandLine.Option(
      hidden = true,
      names = {CHAIN_PRUNING_RETAINED_MINIMUM_FLAG},
      description =
          "Allows setting the limit below which no more data can be pruned. This prevents setting a retained value lower than this limit."
              + " This flag should be used with caution as reducing the limit may have unintended side effects."
              + " (default: ${DEFAULT-VALUE})")
  private final Long chainDataPruningRetainedMinimum = CHAIN_DATA_PRUNING_RETAINED_MINIMUM;

  @CommandLine.Option(
      hidden = true,
      names = {CHAIN_PRUNING_FREQUENCY_FLAG},
      description =
          "The number of blocks added to the chain between two pruning operations. Must be non-negative (default: ${DEFAULT-VALUE})")
  private final PositiveNumber chainDataPruningFrequency =
      PositiveNumber.fromInt(DEFAULT_CHAIN_DATA_PRUNING_FREQUENCY);

  /** Default Constructor. */
  ChainPruningOptions() {}

  /**
   * Create chain pruning options.
   *
   * @return the chain pruning options
   */
  public static ChainPruningOptions create() {
    return new ChainPruningOptions();
  }

  /**
   * Gets chain pruning strategy.
   *
   * @return the chain pruning strategy
   */
  public ChainPruningStrategy getChainPruningStrategy() {
    return chainPruningStrategy;
  }

  /**
   * Gets chain data pruning blocks retained.
   *
   * @return the chain data pruning blocks retained
   */
  public Long getChainDataPruningBlocksRetained() {
    return chainDataPruningBlocksRetained;
  }

  /**
   * Get the configured retained limit for chain data pruning.
   *
   * @return the retained limit
   */
  public Long getChainDataPruningRetainedMinimum() {
    return chainDataPruningRetainedMinimum;
  }

  @Override
  public ChainPrunerConfiguration toDomainObject() {
    return new ChainPrunerConfiguration(
        chainPruningStrategy,
        getChainDataPruningBlocksRetained(),
        chainDataPruningRetainedMinimum,
        chainDataPruningFrequency.getValue(),
        DEFAULT_PRUNE_SLACK_BLOCKS);
  }

  @Override
  public List<String> getCLIOptions() {
    return Arrays.asList(
        CHAIN_PRUNING_ENABLED_FLAG,
        chainPruningStrategy.toString(),
        CHAIN_PRUNING_BLOCKS_RETAINED_FLAG,
        getChainDataPruningBlocksRetained().toString(),
        CHAIN_PRUNING_RETAINED_MINIMUM_FLAG,
        chainDataPruningRetainedMinimum.toString(),
        CHAIN_PRUNING_FREQUENCY_FLAG,
        chainDataPruningFrequency.toString());
  }
}
