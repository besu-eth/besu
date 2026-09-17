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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

public class CheckpointConverterTest {

  private static final String VALID_HASH =
      "0x0000000000000000000000000000000000000000000000000000000000000001";

  private final CheckpointConverter converter = new CheckpointConverter();

  @Test
  public void convertsValidTripleWithDecimalTotalDifficulty() throws Exception {
    final Checkpoint checkpoint = converter.convert(VALID_HASH + ":100:1000");

    assertThat(checkpoint.blockHash()).isEqualTo(Hash.fromHexString(VALID_HASH));
    assertThat(checkpoint.blockNumber()).isEqualTo(100L);
    assertThat(checkpoint.totalDifficulty()).isEqualTo(Difficulty.of(1000));
  }

  @Test
  public void convertsValidTripleWithHexTotalDifficulty() throws Exception {
    final Checkpoint checkpoint = converter.convert(VALID_HASH + ":100:0x3e8");

    assertThat(checkpoint.totalDifficulty()).isEqualTo(Difficulty.of(1000));
  }

  @Test
  public void rejectsWrongNumberOfParts() {
    assertThat(catchThrowable(() -> converter.convert(VALID_HASH + ":100")))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        ":100:1000", // block hash missing
        VALID_HASH + "::1000", // block number missing
        VALID_HASH + ":100:", // total difficulty missing
        "::", // all three missing
      })
  public void rejectsWhenAnElementIsMissing(final String value) {
    assertThat(catchThrowable(() -> converter.convert(value)))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  @Test
  public void rejectsInvalidHash() {
    assertThat(catchThrowable(() -> converter.convert("0xnothex:100:1000")))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  @Test
  public void rejectsNonNumericBlockNumber() {
    assertThat(catchThrowable(() -> converter.convert(VALID_HASH + ":abc:1000")))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  @Test
  public void rejectsNegativeBlockNumber() {
    assertThat(catchThrowable(() -> converter.convert(VALID_HASH + ":-1:1000")))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  @Test
  public void rejectsInvalidTotalDifficulty() {
    assertThat(catchThrowable(() -> converter.convert(VALID_HASH + ":100:notanumber")))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }
}
