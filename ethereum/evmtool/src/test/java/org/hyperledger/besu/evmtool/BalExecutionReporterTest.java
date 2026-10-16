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
package org.hyperledger.besu.evmtool;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class BalExecutionReporterTest {

  private static final String HASH =
      "0x00000000000000000000000000000000000000000000000000000000000000ab";

  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private final BalExecutionReporter reporter =
      new BalExecutionReporter(new PrintStream(err, true, UTF_8));
  private final BlockHeader header = mock(BlockHeader.class);

  BalExecutionReporterTest() {
    when(header.getNumber()).thenReturn(12L);
    when(header.getHash()).thenReturn(Hash.fromHexString(HASH));
  }

  @Test
  void parallelBlockIsOneLineWithAnEmptyReason() {
    reporter.onParallel(header, "bal");

    assertThat(lines())
        .containsExactly(
            "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"path\":\"parallel\",\"reason\":\"\",\"scheduler\":\"bal\"}");
  }

  @Test
  void sequentialBlockCarriesItsReason() {
    reporter.onSequential(header, "disabled");

    assertThat(lines())
        .containsExactly(
            "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"path\":\"sequential\",\"reason\":\"disabled\"}");
  }

  @Test
  void droppedAccessListIsTheReasonOnEitherPathOnlyDuringItsImport() {
    BalExecutionReporter.importing(
        BalExecutionReporter.BAD_ACCESS_LIST,
        () -> {
          reporter.onParallel(header, "optimistic");
          reporter.onSequential(header, "disabled");
          return null;
        });
    reporter.onSequential(header, "disabled");

    assertThat(lines())
        .containsExactly(
            "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"path\":\"parallel\",\"reason\":\"bad-access-list\","
                + "\"scheduler\":\"optimistic\"}",
            "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"path\":\"sequential\",\"reason\":\"bad-access-list\"}",
            "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"path\":\"sequential\",\"reason\":\"disabled\"}");
  }

  @Test
  void fallbackCarriesBothResults() {
    reporter.onSequentialFallback(
        header,
        new BlockProcessingResult(Optional.empty(), "parallel failed"),
        new BlockProcessingResult(Optional.empty(), "sequential failed"));
    reporter.onSequentialFallback(
        header,
        new BlockProcessingResult(Optional.empty(), "parallel failed"),
        new BlockProcessingResult(Optional.empty()));

    assertThat(lines())
        .containsExactly(
            "{\"event\":\"balFallback\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"parallelError\":\"parallel failed\",\"sequentialResult\":\"invalid\","
                + "\"sequentialError\":\"sequential failed\"}",
            "{\"event\":\"balFallback\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"parallelError\":\"parallel failed\",\"sequentialResult\":\"valid\","
                + "\"sequentialError\":\"\"}");
  }

  private String[] lines() {
    return err.toString(UTF_8).split("\\R");
  }
}
