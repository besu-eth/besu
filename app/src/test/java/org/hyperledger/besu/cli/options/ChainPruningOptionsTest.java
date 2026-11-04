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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.chain.ChainDataPruner.ChainPruningStrategy;
import org.hyperledger.besu.ethereum.chain.ChainPrunerConfiguration;
import org.hyperledger.besu.util.number.PositiveNumber;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import picocli.CommandLine;

public class ChainPruningOptionsTest {

  /** Conservative default retention (33024 epochs of 32 slots) in blocks. */
  private static final long HISTORY_PRUNE_BLOCKS = 33_024L * 32L;

  @Test
  public void retainedMinimumMatchesHistoryPruneEpochs() {
    assertThat(ChainPruningOptions.DEFAULT_RETENTION_EPOCHS).isEqualTo(33_024L);
    assertThat(ChainPruningOptions.CHAIN_DATA_PRUNING_RETAINED_MINIMUM)
        .isEqualTo(HISTORY_PRUNE_BLOCKS)
        .isEqualTo(1_056_768L);
  }

  @Test
  public void defaultsUseHistoryPruneEpochsForAllRetentionValues() {
    final ChainPrunerConfiguration config = ChainPruningOptions.create().toDomainObject();

    assertThat(config.pruningMode()).isEqualTo(ChainPruningStrategy.BAL);
    assertThat(config.chainPruningBlocksRetained()).isEqualTo(HISTORY_PRUNE_BLOCKS);
    assertThat(config.chainPruningBalsRetained()).isEqualTo(HISTORY_PRUNE_BLOCKS);
    assertThat(config.chainPruningRetainedMinimum()).isEqualTo(HISTORY_PRUNE_BLOCKS);
  }

  @Test
  public void domainDefaultMatchesCliDefaults() {
    final ChainPrunerConfiguration cliDefaults = ChainPruningOptions.create().toDomainObject();
    final ChainPrunerConfiguration domainDefault = ChainPrunerConfiguration.DEFAULT;

    assertThat(domainDefault.chainPruningBlocksRetained())
        .isEqualTo(cliDefaults.chainPruningBlocksRetained());
    assertThat(domainDefault.chainPruningBalsRetained())
        .isEqualTo(cliDefaults.chainPruningBalsRetained());
    assertThat(domainDefault.chainPruningRetainedMinimum())
        .isEqualTo(cliDefaults.chainPruningRetainedMinimum());
    assertThat(domainDefault.chainPruningFrequency())
        .isEqualTo(cliDefaults.chainPruningFrequency());
  }

  @ParameterizedTest
  @EnumSource(
      value = ChainPruningStrategy.class,
      names = {"ALL", "BAL"})
  public void parsesIndependentRetentionValues(final ChainPruningStrategy strategy) {
    final ChainPruningOptions options = ChainPruningOptions.create();
    new CommandLine(options)
        .registerConverter(PositiveNumber.class, PositiveNumber::fromString)
        .parseArgs(
            "--Xchain-pruning-enabled=" + strategy,
            "--Xchain-pruning-blocks-retained=2000000",
            "--Xchain-pruning-bals-retained=100",
            "--Xchain-pruning-retained-minimum=1000",
            "--Xchain-pruning-frequency=64");

    assertThat(options.toDomainObject())
        .isEqualTo(new ChainPrunerConfiguration(strategy, 2_000_000, 100, 1000, 64));

    final ChainPruningOptions reparsed = ChainPruningOptions.create();
    new CommandLine(reparsed)
        .registerConverter(PositiveNumber.class, PositiveNumber::fromString)
        .parseArgs(options.getCLIOptions().toArray(String[]::new));
    assertThat(reparsed.toDomainObject()).isEqualTo(options.toDomainObject());
  }

  @Test
  public void changingBlockRetentionDoesNotChangeDefaultBalRetention() {
    final ChainPruningOptions options = ChainPruningOptions.create();
    new CommandLine(options).parseArgs("--Xchain-pruning-blocks-retained=2000000");

    assertThat(options.toDomainObject().chainPruningBlocksRetained()).isEqualTo(2_000_000);
    assertThat(options.toDomainObject().chainPruningBalsRetained()).isEqualTo(HISTORY_PRUNE_BLOCKS);
  }

  @Test
  public void changingBalRetentionDoesNotChangeDefaultBlockRetention() {
    final ChainPruningOptions options = ChainPruningOptions.create();
    new CommandLine(options).parseArgs("--Xchain-pruning-bals-retained=100");

    assertThat(options.toDomainObject().chainPruningBlocksRetained())
        .isEqualTo(HISTORY_PRUNE_BLOCKS);
    assertThat(options.toDomainObject().chainPruningBalsRetained()).isEqualTo(100);
  }
}
