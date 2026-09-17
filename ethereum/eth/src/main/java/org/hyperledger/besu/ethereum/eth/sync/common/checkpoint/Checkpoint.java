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
package org.hyperledger.besu.ethereum.eth.sync.common.checkpoint;

import org.hyperledger.besu.config.CheckpointConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Difficulty;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface Checkpoint {

  long blockNumber();

  Hash blockHash();

  Difficulty totalDifficulty();

  /**
   * Validates the raw components of a checkpoint and builds a {@link Checkpoint}. This is the
   * single validation entry point shared by the CLI {@code --checkpoint} option and the
   * genesis-file checkpoint, so both sources enforce identical rules.
   *
   * @param blockHash the block hash, a 32-byte hex string
   * @param blockNumber the block number, must be non-negative
   * @param totalDifficulty the total difficulty, a decimal or {@code 0x}-prefixed hex value
   * @return the validated checkpoint
   * @throws IllegalArgumentException if any component is invalid; callers are expected to wrap this
   *     in a source-appropriate exception type
   */
  static Checkpoint of(
      final String blockHash, final long blockNumber, final String totalDifficulty) {
    final Hash hash;
    try {
      hash = Hash.fromHexString(blockHash);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid checkpoint block hash '" + blockHash + "': must be a 32-byte hex string.", e);
    }

    if (blockNumber < 0) {
      throw new IllegalArgumentException(
          "Invalid checkpoint block number '" + blockNumber + "': must be a non-negative integer.");
    }

    final Difficulty difficulty;
    try {
      difficulty = Difficulty.fromHexOrDecimalString(totalDifficulty);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid checkpoint total difficulty '"
              + totalDifficulty
              + "': must be a decimal or 0x-prefixed hex value.",
          e);
    }

    return ImmutableCheckpoint.builder()
        .blockHash(hash)
        .blockNumber(blockNumber)
        .totalDifficulty(difficulty)
        .build();
  }

  /**
   * Builds a {@link Checkpoint} from the checkpoint configured in a genesis file, if one is fully
   * specified. Delegates to {@link #of} so the same validation rules apply.
   *
   * @param checkpointConfigOptions the genesis-file checkpoint config
   * @return the genesis checkpoint, or empty if the genesis file does not fully configure one
   * @throws IllegalArgumentException if a configured checkpoint has malformed values
   */
  static Optional<Checkpoint> fromConfig(final CheckpointConfigOptions checkpointConfigOptions) {
    if (!checkpointConfigOptions.isValid()) {
      return Optional.empty();
    }
    return Optional.of(
        of(
            checkpointConfigOptions.getHash().get(),
            checkpointConfigOptions.getNumber().getAsLong(),
            checkpointConfigOptions.getTotalDifficulty().get()));
  }
}
