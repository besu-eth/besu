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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.DEFAULT_PLUGIN_BLOCK_TXS_SELECTION_MAX_TIME;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.DEFAULT_POA_BLOCK_TXS_SELECTION_MAX_TIME;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.MutableInitValues.DEFAULT_EXTRA_DATA;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.MutableInitValues.DEFAULT_MIN_PRIORITY_FEE_PER_GAS;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.MutableInitValues.DEFAULT_MIN_TRANSACTION_GAS_PRICE;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.Unstable.DEFAULT_POS_BLOCK_CREATION_MAX_TIME;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.Unstable.DEFAULT_POS_BLOCK_CREATION_REPETITION_MIN_DURATION;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.Unstable.DEFAULT_POS_BLOCK_FINALIZATION_TIMEOUT_MS;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.Unstable.DEFAULT_POS_SLOT_DURATION_SECS;

import org.hyperledger.besu.cli.converter.PositiveNumberConverter;
import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.plugin.services.TransactionSelectionService;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.util.List;
import java.util.OptionalInt;

import jakarta.validation.constraints.Positive;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

/** The Mining CLI options. */
public class MiningOptions implements CLIOptions<MiningConfiguration> {

  private static final String DEPRECATION_PREFIX =
      "Deprecated. PoW consensus is deprecated. See CHANGELOG for alternative options. ";

  @Option(
      names = {"--miner-extra-data"},
      description =
          "A hex string representing the (32) bytes to be included in the extra data "
              + "field of a mined block (default: ${DEFAULT-VALUE})")
  private Bytes extraData = DEFAULT_EXTRA_DATA;

  @Option(
      names = {"--min-block-occupancy-ratio"},
      hidden = true,
      description =
          DEPRECATION_PREFIX
              + "Minimum occupancy ratio for a mined block (default: ${DEFAULT-VALUE})")
  @SuppressWarnings("UnusedVariable")
  @Deprecated
  private Double minBlockOccupancyRatio = null;

  @Option(
      names = {"--min-gas-price"},
      description =
          "Minimum price (in Wei) offered by a transaction for it to be included in a mined "
              + "block (default: ${DEFAULT-VALUE})")
  private Wei minTransactionGasPrice = DEFAULT_MIN_TRANSACTION_GAS_PRICE;

  @Option(
      names = {"--min-priority-fee"},
      description =
          "Minimum priority fee per gas (in Wei) offered by a transaction for it to be included in a "
              + "block (default: ${DEFAULT-VALUE})")
  private Wei minPriorityFeePerGas = DEFAULT_MIN_PRIORITY_FEE_PER_GAS;

  @Option(
      names = {"--target-gas-limit"},
      description =
          "Sets target gas limit per block."
              + " If set, each block's gas limit will approach this setting over time.")
  private Long targetGasLimit = null;

  @Option(
      names = {"--block-txs-selection-max-time"},
      converter = PositiveNumberConverter.class,
      description =
          "Specifies the maximum time, in milliseconds, that could be spent selecting transactions to be included in the block on PoS networks."
              + " Not compatible with PoA networks, see poa-block-txs-selection-max-time. (default: ${DEFAULT-VALUE})")
  private PositiveNumber posBlockTxsSelectionMaxTime = DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME;

  @Option(
      names = {"--poa-block-txs-selection-max-time"},
      converter = PositiveNumberConverter.class,
      description =
          "Specifies the maximum time that could be spent selecting transactions to be included in the block, as a percentage of the fixed block time of the PoA network."
              + " To be only used on PoA networks, for PoS networks see block-txs-selection-max-time."
              + " (default: ${DEFAULT-VALUE})")
  private PositiveNumber poaBlockTxsSelectionMaxTime = DEFAULT_POA_BLOCK_TXS_SELECTION_MAX_TIME;

  @Option(
      names = {"--plugin-block-txs-selection-max-time"},
      converter = PositiveNumberConverter.class,
      description =
          "Specifies the maximum time that plugins could spent selecting transactions to be included in the block, as a percentage of the max block selection time."
              + " (default: ${DEFAULT-VALUE})")
  private PositiveNumber pluginBlockTxsSelectionMaxTime =
      DEFAULT_PLUGIN_BLOCK_TXS_SELECTION_MAX_TIME;

  @Option(
      names = {"--max-blobs-per-transaction"},
      description =
          "Maximum number of blobs allowed per transaction. "
              + "Only applies from Osaka hardfork onwards. (default: 6)",
      arity = "1")
  private Integer maxBlobsPerTransaction = null;

  @Option(
      names = {"--max-blobs-per-block"},
      description =
          "Maximum number of blobs allowed per block when building blocks. "
              + "Only applies from Osaka hardfork onwards. Values above the protocol maximum are clamped. (default: protocol maximum)",
      arity = "1")
  private Integer maxBlobsPerBlock = null;

  @CommandLine.ArgGroup(validate = false)
  private final Unstable unstableOptions = new Unstable();

  static class Unstable {

    @CommandLine.Option(
        hidden = true,
        names = {"--Xpos-block-creation-max-time"},
        description =
            "Specifies the maximum time, in milliseconds, a PoS block creation jobs is allowed to run. Must be positive and ≤ 12000 (default: ${DEFAULT-VALUE} milliseconds)")
    private Long posBlockCreationMaxTime = DEFAULT_POS_BLOCK_CREATION_MAX_TIME;

    @CommandLine.Option(
        hidden = true,
        names = {"--Xpos-block-creation-repetition-min-duration"},
        description =
            "If a PoS block creation repetition takes less than this duration, in milliseconds,"
                + " then it waits before next repetition. Must be positive and ≤ 2000 (default: ${DEFAULT-VALUE} milliseconds)")
    private Long posBlockCreationRepetitionMinDuration =
        DEFAULT_POS_BLOCK_CREATION_REPETITION_MIN_DURATION;

    @CommandLine.Option(
        hidden = true,
        names = {"--Xpos-slot-duration"},
        description = "The slot duration in PoS in seconds (default: ${DEFAULT-VALUE})")
    @Positive
    Integer posSlotDuration = DEFAULT_POS_SLOT_DURATION_SECS;

    @CommandLine.Option(
        hidden = true,
        names = {"--Xpos-block-finalization-timeout-ms"},
        description =
            "Specifies the maximum time, in milliseconds, to wait for block building to complete when only an empty block is available (default: ${DEFAULT-VALUE} milliseconds)")
    private Long posBlockFinalizationTimeoutMs = DEFAULT_POS_BLOCK_FINALIZATION_TIMEOUT_MS;
  }

  private TransactionSelectionService transactionSelectionService;

  private MiningOptions() {}

  /**
   * Create mining options.
   *
   * @return the mining options
   */
  public static MiningOptions create() {
    return new MiningOptions();
  }

  /**
   * Set the transaction selection service
   *
   * @param transactionSelectionService the transaction selection service
   */
  public void setTransactionSelectionService(
      final TransactionSelectionService transactionSelectionService) {
    this.transactionSelectionService = transactionSelectionService;
  }

  /**
   * Validate that there are no inconsistencies in the specified options. For example that the
   * options are valid for the selected implementation.
   *
   * @param commandLine the full commandLine to check all the options specified by the user
   * @param genesisConfigOptions genesis config determines whether we are running PoA or PoS
   * @param isMergeEnabled is the Merge enabled?
   * @param logger the logger
   */
  public void validate(
      final CommandLine commandLine,
      final GenesisConfigOptions genesisConfigOptions,
      final boolean isMergeEnabled,
      final Logger logger) {

    if (unstableOptions.posBlockCreationMaxTime <= 0
        || unstableOptions.posBlockCreationMaxTime > DEFAULT_POS_BLOCK_CREATION_MAX_TIME) {
      throw new ParameterException(
          commandLine,
          "--Xpos-block-creation-max-time must be positive and ≤ "
              + DEFAULT_POS_BLOCK_CREATION_MAX_TIME);
    }

    if (unstableOptions.posBlockCreationRepetitionMinDuration <= 0
        || unstableOptions.posBlockCreationRepetitionMinDuration > 2000) {
      throw new ParameterException(
          commandLine, "--Xpos-block-creation-repetition-min-duration must be positive and ≤ 2000");
    }

    if (unstableOptions.posBlockFinalizationTimeoutMs <= 0
        || unstableOptions.posBlockFinalizationTimeoutMs > 12000) {
      throw new ParameterException(
          commandLine, "--Xpos-block-finalization-timeout-ms must be positive and ≤ 12000");
    }

    CommandLineUtils.failIfOptionDoesntMeetRequirement(
        commandLine,
        "--block-txs-selection-max-time can only be used on networks with PoS support in the genesis file,"
            + " see --poa-block-txs-selection-max-time instead",
        genesisConfigOptions.hasPos(),
        singletonList("--block-txs-selection-max-time"));

    CommandLineUtils.failIfOptionDoesntMeetRequirement(
        commandLine,
        "--poa-block-txs-selection-max-time can be only used with PoA networks,"
            + " see --block-txs-selection-max-time instead",
        genesisConfigOptions.isPoa(),
        singletonList("--poa-block-txs-selection-max-time"));

    if (maxBlobsPerTransaction != null && maxBlobsPerTransaction < 0) {
      throw new ParameterException(
          commandLine, "--max-blobs-per-transaction must be a non-negative value");
    }

    if (maxBlobsPerBlock != null && maxBlobsPerBlock < 0) {
      throw new ParameterException(
          commandLine, "--max-blobs-per-block must be a non-negative value");
    }

    if (maxBlobsPerBlock != null
        && maxBlobsPerTransaction != null
        && maxBlobsPerBlock < maxBlobsPerTransaction) {
      logger.warn(
          "--max-blobs-per-block ({}) is less than --max-blobs-per-transaction ({}). "
              + "The block limit will be the binding constraint during block building.",
          maxBlobsPerBlock,
          maxBlobsPerTransaction);
    }
  }

  static MiningOptions fromConfig(final MiningConfiguration miningConfiguration) {
    final MiningOptions miningOptions = MiningOptions.create();
    miningOptions.setTransactionSelectionService(
        miningConfiguration.getTransactionSelectionService());
    miningOptions.extraData = miningConfiguration.getExtraData();
    miningOptions.minTransactionGasPrice = miningConfiguration.getMinTransactionGasPrice();
    miningOptions.minPriorityFeePerGas = miningConfiguration.getMinPriorityFeePerGas();
    miningOptions.posBlockTxsSelectionMaxTime =
        miningConfiguration.getPosBlockTxsSelectionMaxTime();
    miningOptions.poaBlockTxsSelectionMaxTime =
        miningConfiguration.getPoaBlockTxsSelectionMaxTime();
    miningOptions.pluginBlockTxsSelectionMaxTime =
        miningConfiguration.getPluginBlockTxsSelectionMaxTime();
    miningConfiguration
        .getMaxBlobsPerTransaction()
        .ifPresent(v -> miningOptions.maxBlobsPerTransaction = v);
    miningConfiguration.getMaxBlobsPerBlock().ifPresent(v -> miningOptions.maxBlobsPerBlock = v);

    miningOptions.unstableOptions.posBlockCreationMaxTime =
        miningConfiguration.getUnstable().getPosBlockCreationMaxTime();
    miningOptions.unstableOptions.posBlockCreationRepetitionMinDuration =
        miningConfiguration.getUnstable().getPosBlockCreationRepetitionMinDuration();
    miningOptions.unstableOptions.posSlotDuration =
        miningConfiguration.getUnstable().getPosSlotDuration();
    miningOptions.unstableOptions.posBlockFinalizationTimeoutMs =
        miningConfiguration.getUnstable().getPosBlockFinalizationTimeoutMs();

    miningConfiguration.getTargetGasLimit().ifPresent(tgl -> miningOptions.targetGasLimit = tgl);
    return miningOptions;
  }

  @Override
  public MiningConfiguration toDomainObject() {
    checkNotNull(
        transactionSelectionService,
        "transactionSelectionService must be set before using this object");

    final var updatableInitValuesBuilder =
        MutableInitValues.builder()
            .extraData(extraData)
            .minTransactionGasPrice(minTransactionGasPrice)
            .minPriorityFeePerGas(minPriorityFeePerGas);

    if (maxBlobsPerTransaction != null) {
      updatableInitValuesBuilder.maxBlobsPerTransaction(maxBlobsPerTransaction);
    }

    if (targetGasLimit != null) {
      updatableInitValuesBuilder.targetGasLimit(targetGasLimit);
    }

    return ImmutableMiningConfiguration.builder()
        .transactionSelectionService(transactionSelectionService)
        .mutableInitValues(updatableInitValuesBuilder.build())
        .maxBlobsPerBlock(
            maxBlobsPerBlock != null ? OptionalInt.of(maxBlobsPerBlock) : OptionalInt.empty())
        .posBlockTxsSelectionMaxTime(posBlockTxsSelectionMaxTime)
        .poaBlockTxsSelectionMaxTime(poaBlockTxsSelectionMaxTime)
        .pluginBlockTxsSelectionMaxTime(pluginBlockTxsSelectionMaxTime)
        .unstable(
            ImmutableMiningConfiguration.Unstable.builder()
                .posBlockCreationMaxTime(unstableOptions.posBlockCreationMaxTime)
                .posBlockCreationRepetitionMinDuration(
                    unstableOptions.posBlockCreationRepetitionMinDuration)
                .posSlotDuration(unstableOptions.posSlotDuration)
                .posBlockFinalizationTimeoutMs(unstableOptions.posBlockFinalizationTimeoutMs)
                .build())
        .build();
  }

  @Override
  public List<String> getCLIOptions() {
    return CommandLineUtils.getCLIOptions(this, new MiningOptions());
  }
}
