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
package org.hyperledger.besu.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.controller.MergeBesuControllerBuilder.isPostMergeAtGenesis;

import org.hyperledger.besu.config.GenesisConfig;

import org.junit.jupiter.api.Test;

class PostMergeAtGenesisTest {

  @Test
  void hoodiIsPostMergeAtGenesisDespiteNonZeroGenesisDifficulty() {
    // Hoodi sets difficulty 0x01 with terminalTotalDifficulty 0. Genesis' own total difficulty
    // therefore already satisfies the terminal condition, so the chain has no pre-merge blocks.
    assertThat(isPostMergeAtGenesis(GenesisConfig.fromResource("/hoodi.json"))).isTrue();
  }

  @Test
  void mainnetIsNotPostMergeAtGenesis() {
    assertThat(isPostMergeAtGenesis(GenesisConfig.mainnet())).isFalse();
  }

  @Test
  void sepoliaIsNotPostMergeAtGenesis() {
    assertThat(isPostMergeAtGenesis(GenesisConfig.fromResource("/sepolia.json"))).isFalse();
  }

  @Test
  void zeroGenesisDifficultyWithZeroTerminalTotalDifficultyIsPostMergeAtGenesis() {
    // The case the old predicate already handled; the new one must still return true.
    final GenesisConfig genesis =
        GenesisConfig.fromConfig(
            """
            {
              "config": { "terminalTotalDifficulty": 0 },
              "nonce": "0x0",
              "timestamp": "0x0",
              "difficulty": "0x0",
              "gasLimit": "0x1388",
              "alloc": {}
            }
            """);

    assertThat(isPostMergeAtGenesis(genesis)).isTrue();
  }

  @Test
  void absentTerminalTotalDifficultyIsNotPostMergeAtGenesis() {
    final GenesisConfig genesis =
        GenesisConfig.fromConfig(
            """
            {
              "config": {},
              "nonce": "0x0",
              "timestamp": "0x0",
              "difficulty": "0x1",
              "gasLimit": "0x1388",
              "alloc": {}
            }
            """);

    assertThat(isPostMergeAtGenesis(genesis)).isFalse();
  }

  @Test
  void cliqueMigratedToPoSIsNotPostMergeAtGenesis() {
    // A Clique chain accumulates 1-2 difficulty per block, so its merge-point TTD sits far above
    // the genesis difficulty of 0x1. The predicate must not strand its pre-merge blocks.
    final GenesisConfig genesis =
        GenesisConfig.fromConfig(
            """
            {
              "config": {
                "clique": { "period": 5, "epoch": 30000 },
                "terminalTotalDifficulty": 1000000
              },
              "nonce": "0x0",
              "timestamp": "0x0",
              "difficulty": "0x1",
              "gasLimit": "0x1388",
              "alloc": {}
            }
            """);

    assertThat(isPostMergeAtGenesis(genesis)).isFalse();
  }

  @Test
  void malformedDifficultyIsReportedAgainstTheGenesisField() {
    // This predicate is the first thing to parse difficulty, ahead of GenesisState, so it has to
    // produce the same error a malformed value would get there.
    final GenesisConfig genesis =
        GenesisConfig.fromConfig(
            """
            {
              "config": { "terminalTotalDifficulty": 0 },
              "nonce": "0x0",
              "timestamp": "0x0",
              "difficulty": "0xnothex",
              "gasLimit": "0x1388",
              "alloc": {}
            }
            """);

    assertThatThrownBy(() -> isPostMergeAtGenesis(genesis))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid difficulty in genesis block configuration: 0xnothex");
  }
}
