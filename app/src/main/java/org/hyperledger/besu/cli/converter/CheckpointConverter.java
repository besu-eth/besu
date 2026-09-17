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
package org.hyperledger.besu.cli.converter;

import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;

import picocli.CommandLine;

/**
 * Converts a {@code <blockHash>:<blockNumber>:<totalDifficulty>} CLI value into a {@link
 * Checkpoint}. The block hash must be a 32-byte hex string, the block number a non-negative decimal
 * integer, and the total difficulty either a decimal or {@code 0x}-prefixed hex value.
 */
public class CheckpointConverter implements CommandLine.ITypeConverter<Checkpoint> {

  private static final String FORMAT_HINT =
      "expected format is <blockHash>:<blockNumber>:<totalDifficulty>";

  /** Default constructor. */
  public CheckpointConverter() {}

  @Override
  public Checkpoint convert(final String value) {
    final String[] parts = value.split(":", -1);
    if (parts.length != 3) {
      throw new CommandLine.TypeConversionException(
          "Invalid checkpoint '" + value + "': " + FORMAT_HINT + ".");
    }

    final long blockNumber;
    try {
      blockNumber = Long.parseLong(parts[1]);
    } catch (final NumberFormatException e) {
      throw new CommandLine.TypeConversionException(
          "Invalid checkpoint block number '" + parts[1] + "': must be a non-negative integer.");
    }

    try {
      return Checkpoint.of(parts[0], blockNumber, parts[2]);
    } catch (final IllegalArgumentException e) {
      throw new CommandLine.TypeConversionException(e.getMessage());
    }
  }
}
