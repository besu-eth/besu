/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.eth.sync.common.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.CheckpointConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Difficulty;

import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

class CheckpointTest {

  private static final String VALID_HASH =
      "0x0000000000000000000000000000000000000000000000000000000000000001";

  @Test
  void buildsFromDecimalTotalDifficulty() {
    final Checkpoint checkpoint = Checkpoint.of(VALID_HASH, 100L, "1000");

    assertThat(checkpoint.blockHash()).isEqualTo(Hash.fromHexString(VALID_HASH));
    assertThat(checkpoint.blockNumber()).isEqualTo(100L);
    assertThat(checkpoint.totalDifficulty()).isEqualTo(Difficulty.of(1000));
  }

  @Test
  void buildsFromHexTotalDifficulty() {
    final Checkpoint checkpoint = Checkpoint.of(VALID_HASH, 100L, "0x3e8");

    assertThat(checkpoint.totalDifficulty()).isEqualTo(Difficulty.of(1000));
  }

  @Test
  void rejectsInvalidHash() {
    assertThatThrownBy(() -> Checkpoint.of("0xnothex", 100L, "1000"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("block hash");
  }

  @Test
  void rejectsNegativeBlockNumber() {
    assertThatThrownBy(() -> Checkpoint.of(VALID_HASH, -1L, "1000"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
  }

  @Test
  void rejectsInvalidTotalDifficulty() {
    assertThatThrownBy(() -> Checkpoint.of(VALID_HASH, 100L, "notanumber"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("total difficulty");
  }

  @Test
  void fromConfigBuildsGenesisCheckpoint() {
    final CheckpointConfigOptions options = mock(CheckpointConfigOptions.class);
    when(options.isValid()).thenReturn(true);
    when(options.getHash()).thenReturn(Optional.of(VALID_HASH));
    when(options.getNumber()).thenReturn(OptionalLong.of(50L));
    when(options.getTotalDifficulty()).thenReturn(Optional.of("0x64"));

    assertThat(Checkpoint.fromConfig(options)).contains(Checkpoint.of(VALID_HASH, 50L, "0x64"));
  }

  @Test
  void fromConfigEmptyWhenNoCheckpointConfigured() {
    final CheckpointConfigOptions options = mock(CheckpointConfigOptions.class);
    when(options.isValid()).thenReturn(false);

    assertThat(Checkpoint.fromConfig(options)).isEmpty();
  }

  @Test
  void fromConfigRejectsMalformedCheckpoint() {
    final CheckpointConfigOptions options = mock(CheckpointConfigOptions.class);
    when(options.isValid()).thenReturn(true);
    when(options.getHash()).thenReturn(Optional.of(VALID_HASH));
    when(options.getNumber()).thenReturn(OptionalLong.of(50L));
    when(options.getTotalDifficulty()).thenReturn(Optional.of("notanumber"));

    assertThatThrownBy(() -> Checkpoint.fromConfig(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("total difficulty");
  }
}
