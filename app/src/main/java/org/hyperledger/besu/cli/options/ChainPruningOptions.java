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
  private static final String CHAIN_PRUNING_BALS_RETAINED_FLAG = "--Xchain-pruning-bals-retained";
  private static final String CHAIN_PRUNING_RETAINED_MINIMUM_FLAG =
      "--Xchain-pruning-retained-minimum";
  private static final String CHAIN_PRUNING_FREQUENCY_FLAG = "--Xchain-pruning-frequency";
  private static final String PRE_MERGE_PRUNING_QUANTITY_FLAG = "--Xpre-merge-pruning-quantity";

  /**
   * The "CHAIN_DATA_PRUNING_MIN_RETAINED_LIMIT" field sets the minimum retained limit for chain
   * data pruning values. It follows the EIP-4444 history expiry window, the period during which
   * consensus clients rebuild blinded payloads from the execution client, so block bodies and block
   * access lists must remain available for that long. Reducing this value requires careful
   * consideration and understanding of the potential implications. Lowering this limit may have
   * unintended side effects.
   */
  public static final long CHAIN_DATA_PRUNING_RETAINED_MINIMUM =
      ChainPrunerConfiguration.HISTORY_PRUNE_BLOCKS;

  /** The constant DEFAULT_CHAIN_DATA_PRUNING_FREQUENCY. */
  public static final int DEFAULT_CHAIN_DATA_PRUNING_FREQUENCY = 256;

  /** The constant DEFAULT_PRE_MERGE_PRUNING_QUANTITY. */
  public static final int DEFAULT_PRE_MERGE_PRUNING_QUANTITY = 100;

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
              + " (default: ${DEFAULT-VALUE}). Unused if --history-expiry-prune is enabled")
  private final Long chainDataPruningBlocksRetained = CHAIN_DATA_PRUNING_RETAINED_MINIMUM;

  @CommandLine.Option(
      hidden = true,
      names = {CHAIN_PRUNING_BALS_RETAINED_FLAG},
      description =
          "The number of recent blocks for which to keep block access lists. Must be >= "
              + CHAIN_DATA_PRUNING_RETAINED_MINIMUM
              + ". Defaults to "
              + CHAIN_PRUNING_BLOCKS_RETAINED_FLAG
              + " when not specified.")
  private Long chainDataPruningBalsRetained;

  @CommandLine.Option(
      hidden = true,
      names = {CHAIN_PRUNING_RETAINED_MINIMUM_FLAG},
      description =
          "Allows setting the limit below which no more data can be pruned. This prevents setting a retained value lower than this limit."
              + " This flag should be used with caution as reducing the limit may have unintended side effects."
              + " (default: ${DEFAULT-VALUE}). Unused if --history-expiry-prune is enabled")
  private final Long chainDataPruningRetainedMinimum = CHAIN_DATA_PRUNING_RETAINED_MINIMUM;

  @CommandLine.Option(
      hidden = true,
      names = {CHAIN_PRUNING_FREQUENCY_FLAG},
      description =
          "The number of blocks added to the chain between two pruning operations. Must be non-negative (default: ${DEFAULT-VALUE})")
  private final PositiveNumber chainDataPruningFrequency =
      PositiveNumber.fromInt(DEFAULT_CHAIN_DATA_PRUNING_FREQUENCY);

  @CommandLine.Option(
      hidden = true,
      names = {PRE_MERGE_PRUNING_QUANTITY_FLAG},
      description =
          "The number of pre-merge blocks to prune per pruning operation. Must be non-negative (default: ${DEFAULT-VALUE})")
  private final PositiveNumber preMergePruningBlocksQuantity =
      PositiveNumber.fromInt(DEFAULT_PRE_MERGE_PRUNING_QUANTITY);

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
   * Gets number of block access list retained.
   *
   * @return the number of bals retained
   */
  public Long getChainDataPruningBalsRetained() {
    return chainDataPruningBalsRetained == null
        ? chainDataPruningBlocksRetained
        : chainDataPruningBalsRetained;
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
        getChainDataPruningBalsRetained(),
        chainDataPruningRetainedMinimum,
        chainDataPruningFrequency.getValue(),
        preMergePruningBlocksQuantity.getValue());
  }

  @Override
  public List<String> getCLIOptions() {
    return Arrays.asList(
        CHAIN_PRUNING_ENABLED_FLAG,
        chainPruningStrategy.toString(),
        CHAIN_PRUNING_BLOCKS_RETAINED_FLAG,
        getChainDataPruningBlocksRetained().toString(),
        CHAIN_PRUNING_BALS_RETAINED_FLAG,
        getChainDataPruningBalsRetained().toString(),
        CHAIN_PRUNING_RETAINED_MINIMUM_FLAG,
        chainDataPruningRetainedMinimum.toString(),
        CHAIN_PRUNING_FREQUENCY_FLAG,
        chainDataPruningFrequency.toString(),
        PRE_MERGE_PRUNING_QUANTITY_FLAG,
        preMergePruningBlocksQuantity.toString());
  }
}
