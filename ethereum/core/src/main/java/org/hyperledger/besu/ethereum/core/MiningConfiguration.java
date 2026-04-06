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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.PendingTransaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;
import org.hyperledger.besu.plugin.services.TransactionSelectionService;
import org.hyperledger.besu.plugin.services.txselection.BlockTransactionSelectionService;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelector;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelectorFactory;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;
import org.hyperledger.besu.util.BesuVersionUtils;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.immutables.value.Value;

@Value.Immutable
@Value.Enclosing
public abstract class MiningConfiguration {
  public static final PositiveNumber DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME =
      PositiveNumber.fromInt((int) Duration.ofSeconds(5).toMillis());
  public static final PositiveNumber DEFAULT_POA_BLOCK_TXS_SELECTION_MAX_TIME =
      PositiveNumber.fromInt(75);
  public static final PositiveNumber DEFAULT_PLUGIN_BLOCK_TXS_SELECTION_MAX_TIME =
      PositiveNumber.fromInt(50);
  public static final MiningConfiguration MINING_DISABLED =
      ImmutableMiningConfiguration.builder()
          .mutableInitValues(
              ImmutableMiningConfiguration.MutableInitValues.builder()
                  .isMiningEnabled(false)
                  .build())
          .build();

  @VisibleForTesting
  public static final MiningConfiguration newDefault() {
    return ImmutableMiningConfiguration.builder().build();
  }

  public boolean isMiningEnabled() {
    return getMutableRuntimeValues().miningEnabled;
  }

  public MiningConfiguration setMiningEnabled(final boolean miningEnabled) {
    getMutableRuntimeValues().miningEnabled = miningEnabled;
    return this;
  }

  public Bytes getExtraData() {
    return getMutableRuntimeValues().extraData;
  }

  public MiningConfiguration setExtraData(final Bytes extraData) {
    getMutableRuntimeValues().extraData = extraData;
    return this;
  }

  public Wei getMinTransactionGasPrice() {
    return getMutableRuntimeValues().minTransactionGasPrice;
  }

  public MiningConfiguration setMinTransactionGasPrice(final Wei minTransactionGasPrice) {
    getMutableRuntimeValues().minTransactionGasPrice = minTransactionGasPrice;
    return this;
  }

  public Wei getMinPriorityFeePerGas() {
    return getMutableRuntimeValues().minPriorityFeePerGas;
  }

  public MiningConfiguration setMinPriorityFeePerGas(final Wei minPriorityFeePerGas) {
    getMutableRuntimeValues().minPriorityFeePerGas = minPriorityFeePerGas;
    return this;
  }

  public Optional<Address> getCoinbase() {
    return getMutableRuntimeValues().coinbase;
  }

  public MiningConfiguration setCoinbase(final Address coinbase) {
    getMutableRuntimeValues().coinbase = Optional.of(coinbase);
    return this;
  }

  public OptionalLong getTargetGasLimit() {
    return getMutableRuntimeValues().targetGasLimit;
  }

  public MiningConfiguration setTargetGasLimit(final long targetGasLimit) {
    getMutableRuntimeValues().targetGasLimit = OptionalLong.of(targetGasLimit);
    return this;
  }

  /**
   * Returns the maximum blobs per transaction. Note: Only applies from Osaka hardfork onwards.
   * Returns empty if not explicitly set by the user.
   *
   * @return the maximum blobs per transaction, or empty if not set
   */
  public OptionalInt getMaxBlobsPerTransaction() {
    return getMutableInitValues().getMaxBlobsPerTransaction();
  }

  /**
   * Returns the maximum blobs per block for block building. Note: Only applies from Osaka hardfork
   * onwards. Returns empty if not explicitly set by the user.
   *
   * @return the maximum blobs per block, or empty if not set
   */
  @Value.Default
  public OptionalInt getMaxBlobsPerBlock() {
    return OptionalInt.empty();
  }

  public Optional<Iterable<Long>> getNonceGenerator() {
    return getMutableRuntimeValues().nonceGenerator;
  }

  public MiningConfiguration setNonceGenerator(final Iterable<Long> nonceGenerator) {
    getMutableRuntimeValues().nonceGenerator = Optional.of(nonceGenerator);
    return this;
  }

  public OptionalInt getBlockPeriodSeconds() {
    return getMutableRuntimeValues().blockPeriodSeconds;
  }

  public MiningConfiguration setBlockPeriodSeconds(final int blockPeriodSeconds) {
    getMutableRuntimeValues().blockPeriodSeconds = OptionalInt.of(blockPeriodSeconds);
    return this;
  }

  public MiningConfiguration setEmptyBlockPeriodSeconds(final int emptyBlockPeriodSeconds) {
    getMutableRuntimeValues().emptyBlockPeriodSeconds = OptionalInt.of(emptyBlockPeriodSeconds);
    return this;
  }

  @Value.Default
  public PositiveNumber getPosBlockTxsSelectionMaxTime() {
    return DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME;
  }

  @Value.Default
  public PositiveNumber getPoaBlockTxsSelectionMaxTime() {
    return DEFAULT_POA_BLOCK_TXS_SELECTION_MAX_TIME;
  }

  @Value.Default
  public PositiveNumber getPluginBlockTxsSelectionMaxTime() {
    return DEFAULT_PLUGIN_BLOCK_TXS_SELECTION_MAX_TIME;
  }

  @Value.Default
  public TransactionSelectionService getTransactionSelectionService() {
    return new TransactionSelectionService() {
      @Override
      public PluginTransactionSelector createPluginTransactionSelector(
          final ProcessableBlockHeader pendingBlockHeader,
          final SelectorsStateManager selectorsStateManager) {
        return PluginTransactionSelector.ACCEPT_ALL;
      }

      @Override
      public void selectPendingTransactions(
          final BlockTransactionSelectionService selectionService,
          final ProcessableBlockHeader pendingBlockHeader,
          final List<? extends PendingTransaction> candidatePendingTransactions) {}

      @Override
      public void registerPluginTransactionSelectorFactory(
          final PluginTransactionSelectorFactory transactionSelectorFactory) {}
    };
  }

  public Duration getBlockTxsSelectionMaxTime(final boolean isPoS) {
    if (!isPoS) {
      final var maybeBlockPeriodSeconds = getMutableRuntimeValues().blockPeriodSeconds;
      if (maybeBlockPeriodSeconds.isPresent()) {
        return Duration.ofMillis(
            (TimeUnit.SECONDS.toMillis(maybeBlockPeriodSeconds.getAsInt())
                    * getPoaBlockTxsSelectionMaxTime().getValue())
                / 100);
      }
    }

    return Duration.ofMillis(getPosBlockTxsSelectionMaxTime().getValue());
  }

  public Duration getPluginTxsSelectionMaxTime(final Duration blockTxsSelectionMaxTime) {
    return Duration.ofMillis(
        (blockTxsSelectionMaxTime.toMillis() * getPluginBlockTxsSelectionMaxTime().getValue())
            / 100);
  }

  @Value.Default
  protected MutableRuntimeValues getMutableRuntimeValues() {
    return new MutableRuntimeValues(getMutableInitValues());
  }

  @Value.Default
  public Unstable getUnstable() {
    return Unstable.DEFAULT;
  }

  @Value.Default
  public MutableInitValues getMutableInitValues() {
    return MutableInitValues.DEFAULT;
  }

  @Value.Immutable
  public interface MutableInitValues {
    // This is the default extra data containing version info, capped at 32 bytes.
    Bytes DEFAULT_EXTRA_DATA = BesuVersionUtils.versionForExtraData();

    Wei DEFAULT_MIN_TRANSACTION_GAS_PRICE = Wei.of(1000);
    Wei DEFAULT_MIN_PRIORITY_FEE_PER_GAS = Wei.ZERO;

    MutableInitValues DEFAULT = ImmutableMiningConfiguration.MutableInitValues.builder().build();

    @Value.Default
    default boolean isMiningEnabled() {
      return false;
    }

    @Value.Default
    default Bytes getExtraData() {
      return DEFAULT_EXTRA_DATA;
    }

    @Value.Default
    default Wei getMinTransactionGasPrice() {
      return DEFAULT_MIN_TRANSACTION_GAS_PRICE;
    }

    @Value.Default
    default Wei getMinPriorityFeePerGas() {
      return DEFAULT_MIN_PRIORITY_FEE_PER_GAS;
    }

    /**
     * Returns the maximum blobs per transaction. Note: Only applies from Osaka hardfork onwards.
     * Empty means use the fork-specific default from the gas limit calculator.
     */
    OptionalInt getMaxBlobsPerTransaction();

    OptionalInt getBlockPeriodSeconds();

    OptionalInt getEmptyBlockPeriodSeconds();

    Optional<Address> getCoinbase();

    OptionalLong getTargetGasLimit();

    Optional<Iterable<Long>> nonceGenerator();
  }

  static class MutableRuntimeValues {
    private volatile boolean miningEnabled;
    private volatile Bytes extraData;
    private volatile Wei minTransactionGasPrice;
    private volatile Wei minPriorityFeePerGas;
    private volatile Optional<Address> coinbase;
    private volatile OptionalLong targetGasLimit;
    private volatile Optional<Iterable<Long>> nonceGenerator;
    private volatile OptionalInt blockPeriodSeconds;
    private volatile OptionalInt emptyBlockPeriodSeconds;

    private MutableRuntimeValues(final MutableInitValues initValues) {
      miningEnabled = initValues.isMiningEnabled();
      extraData = initValues.getExtraData();
      minTransactionGasPrice = initValues.getMinTransactionGasPrice();
      minPriorityFeePerGas = initValues.getMinPriorityFeePerGas();
      coinbase = initValues.getCoinbase();
      targetGasLimit = initValues.getTargetGasLimit();
      nonceGenerator = initValues.nonceGenerator();
      blockPeriodSeconds = initValues.getBlockPeriodSeconds();
      emptyBlockPeriodSeconds = initValues.getEmptyBlockPeriodSeconds();
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      final MutableRuntimeValues that = (MutableRuntimeValues) o;
      return miningEnabled == that.miningEnabled
          && Objects.equals(extraData, that.extraData)
          && Objects.equals(minTransactionGasPrice, that.minTransactionGasPrice)
          && Objects.equals(coinbase, that.coinbase)
          && Objects.equals(minPriorityFeePerGas, that.minPriorityFeePerGas)
          && Objects.equals(targetGasLimit, that.targetGasLimit)
          && Objects.equals(nonceGenerator, that.nonceGenerator)
          && Objects.equals(blockPeriodSeconds, that.blockPeriodSeconds)
          && Objects.equals(emptyBlockPeriodSeconds, that.emptyBlockPeriodSeconds);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          miningEnabled,
          extraData,
          minTransactionGasPrice,
          minPriorityFeePerGas,
          coinbase,
          targetGasLimit,
          nonceGenerator,
          blockPeriodSeconds);
    }

    @Override
    public String toString() {
      return "UpdatableRuntimeValues{"
          + "miningEnabled="
          + miningEnabled
          + ", extraData="
          + extraData
          + ", minTransactionGasPrice="
          + minTransactionGasPrice
          + ", minPriorityFeePerGas="
          + minPriorityFeePerGas
          + ", coinbase="
          + coinbase
          + ", targetGasLimit="
          + targetGasLimit
          + ", nonceGenerator="
          + nonceGenerator
          + ", blockPeriodSeconds="
          + blockPeriodSeconds
          + '}';
    }
  }

  @Value.Immutable
  public interface Unstable {
    int DEFAULT_REMOTE_SEALERS_LIMIT = 1000;
    long DEFAULT_REMOTE_SEALERS_TTL = Duration.ofMinutes(10).toMinutes();
    long DEFAULT_POW_JOB_TTL = Duration.ofMinutes(5).toMillis();
    int DEFAULT_MAX_OMMERS_DEPTH = 8;
    long DEFAULT_POS_BLOCK_CREATION_MAX_TIME = Duration.ofSeconds(12).toMillis();
    long DEFAULT_POS_BLOCK_CREATION_REPETITION_MIN_DURATION = Duration.ofMillis(500).toMillis();
    long DEFAULT_POS_BLOCK_FINALIZATION_TIMEOUT_MS = 800L;
    Integer DEFAULT_POS_SLOT_DURATION_SECS = 12;

    MiningConfiguration.Unstable DEFAULT = ImmutableMiningConfiguration.Unstable.builder().build();

    @Value.Default
    default int getRemoteSealersLimit() {
      return DEFAULT_REMOTE_SEALERS_LIMIT;
    }

    @Value.Default
    default long getRemoteSealersTimeToLive() {
      return DEFAULT_REMOTE_SEALERS_TTL;
    }

    @Value.Default
    default long getPowJobTimeToLive() {
      return DEFAULT_POW_JOB_TTL;
    }

    @Value.Default
    default int getMaxOmmerDepth() {
      return DEFAULT_MAX_OMMERS_DEPTH;
    }

    @Value.Default
    default long getPosBlockCreationMaxTime() {
      return DEFAULT_POS_BLOCK_CREATION_MAX_TIME;
    }

    @Value.Default
    default long getPosBlockCreationRepetitionMinDuration() {
      return DEFAULT_POS_BLOCK_CREATION_REPETITION_MIN_DURATION;
    }

    @Value.Default
    default Integer getPosSlotDuration() {
      return DEFAULT_POS_SLOT_DURATION_SECS;
    }

    @Value.Default
    default long getPosBlockFinalizationTimeoutMs() {
      return DEFAULT_POS_BLOCK_FINALIZATION_TIMEOUT_MS;
    }
  }
}
