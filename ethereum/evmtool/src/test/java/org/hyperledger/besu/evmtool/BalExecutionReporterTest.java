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
import org.hyperledger.besu.ethereum.BlockProcessingResult.ExecutionPath;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.evmtool.BalExecutionReporter.AccessListMissing;

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
    reporter.report(header, executedOn(ExecutionPath.PARALLEL_BAL), Optional.empty());

    assertThat(lines())
        .containsExactly(
            "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"path\":\"parallel\",\"reason\":\"\",\"scheduler\":\"bal\"}");
  }

  @Test
  void sequentialBlockCarriesItsReason() {
    reporter.report(header, executedOn(ExecutionPath.SEQUENTIAL), Optional.empty());
    reporter.report(header, executedOn(ExecutionPath.SEQUENTIAL_NOT_PATH_BASED), Optional.empty());

    assertThat(lines())
        .containsExactly(
            "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"path\":\"sequential\",\"reason\":\"disabled\"}",
            "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"path\":\"sequential\",\"reason\":\"not-path-based\"}");
  }

  @Test
  void droppedAccessListIsTheReasonOnEitherPath() {
    reporter.report(
        header,
        executedOn(ExecutionPath.PARALLEL_OPTIMISTIC),
        Optional.of(AccessListMissing.BAD_ACCESS_LIST));
    reporter.report(
        header, executedOn(ExecutionPath.SEQUENTIAL), Optional.of(AccessListMissing.WITHHELD));

    assertThat(lines())
        .containsExactly(
            "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"path\":\"parallel\",\"reason\":\"bad-access-list\","
                + "\"scheduler\":\"optimistic\"}",
            "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"path\":\"sequential\",\"reason\":\"withheld\"}");
  }

  @Test
  void blockRejectedBeforeExecutionIsNotReported() {
    reporter.report(header, new BlockProcessingResult("header invalid"), Optional.empty());

    assertThat(err.toString(UTF_8)).isEmpty();
  }

  @Test
  void fallbackReportsTheParallelAttemptAndBothResults() {
    reporter.report(
        header,
        new BlockProcessingResult(Optional.empty(), "sequential failed")
            .withSequentialFallback(Optional.of("parallel failed")),
        Optional.empty());
    reporter.report(
        header,
        new BlockProcessingResult(Optional.empty())
            .withSequentialFallback(Optional.of("parallel failed")),
        Optional.empty());

    final String parallelLine =
        "{\"event\":\"balExecution\",\"block\":12,\"hash\":\""
            + HASH
            + "\",\"path\":\"parallel\",\"reason\":\"\",\"scheduler\":\"optimistic\"}";
    assertThat(lines())
        .containsExactly(
            parallelLine,
            "{\"event\":\"balFallback\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"parallelError\":\"parallel failed\",\"sequentialResult\":\"invalid\","
                + "\"sequentialError\":\"sequential failed\"}",
            parallelLine,
            "{\"event\":\"balFallback\",\"block\":12,\"hash\":\""
                + HASH
                + "\",\"parallelError\":\"parallel failed\",\"sequentialResult\":\"valid\","
                + "\"sequentialError\":\"\"}");
  }

  private static BlockProcessingResult executedOn(final ExecutionPath path) {
    return new BlockProcessingResult(Optional.empty()).withExecutionPath(path);
  }

  private String[] lines() {
    return err.toString(UTF_8).split("\\R");
  }
}
