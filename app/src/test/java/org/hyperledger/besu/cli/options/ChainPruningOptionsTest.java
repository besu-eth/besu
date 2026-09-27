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

import org.junit.jupiter.api.Test;

public class ChainPruningOptionsTest {

  /** EIP-4444 HISTORY_PRUNE_EPOCHS (33024 epochs of 32 slots) in blocks. */
  private static final long HISTORY_PRUNE_BLOCKS = 33_024L * 32L;

  @Test
  public void retainedMinimumMatchesHistoryPruneEpochs() {
    assertThat(ChainPruningOptions.HISTORY_PRUNE_EPOCHS).isEqualTo(33_024L);
    assertThat(ChainPruningOptions.CHAIN_DATA_PRUNING_RETAINED_MINIMUM)
        .isEqualTo(HISTORY_PRUNE_BLOCKS)
        .isEqualTo(1_056_768L);
  }

  @Test
  public void defaultsUseHistoryPruneEpochsForAllRetentionValues() {
    final ChainPrunerConfiguration config = ChainPruningOptions.create().toDomainObject();

    assertThat(config.pruningMode()).isEqualTo(ChainPruningStrategy.BAL);
    assertThat(config.chainPruningBlocksRetained()).isEqualTo(HISTORY_PRUNE_BLOCKS);
    assertThat(config.chainPruningRetainedMinimum()).isEqualTo(HISTORY_PRUNE_BLOCKS);
    assertThat(config.pruneSlackBlocks()).isEqualTo(ChainPruningOptions.DEFAULT_PRUNE_SLACK_BLOCKS);
    assertThat(config.pruneSlackBlocks()).isEqualTo(4096L);
  }

  @Test
  public void domainDefaultMatchesCliDefaults() {
    final ChainPrunerConfiguration cliDefaults = ChainPruningOptions.create().toDomainObject();
    final ChainPrunerConfiguration domainDefault = ChainPrunerConfiguration.DEFAULT;

    assertThat(domainDefault.chainPruningBlocksRetained())
        .isEqualTo(cliDefaults.chainPruningBlocksRetained());
    assertThat(domainDefault.chainPruningRetainedMinimum())
        .isEqualTo(cliDefaults.chainPruningRetainedMinimum());
    assertThat(domainDefault.pruneSlackBlocks()).isEqualTo(cliDefaults.pruneSlackBlocks());
  }
}
