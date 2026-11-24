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

import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.io.PrintStream;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Under --bal-report, prints one JSON line to stderr for each block block-test and engine-test
 * execute, naming the executor that ran it, and one more when the parallel executor failed a block
 * and it was run again sequentially, so a fixture's verdict can be tied to the path that produced
 * it. Lines from different workers interleave; the block hash ties each to its block.
 */
final class BalExecutionReporter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Why the runner imported a block without its delivered access list. */
  enum AccessListMissing {
    /** --bal-withhold kept the list from the block. */
    WITHHELD("withheld"),
    /** The list did not hash to the header's commitment, so it was dropped. */
    BAD_ACCESS_LIST("bad-access-list");

    private final String reason;

    AccessListMissing(final String reason) {
      this.reason = reason;
    }
  }

  private final PrintStream err;

  BalExecutionReporter(final PrintStream err) {
    this.err = err;
  }

  /**
   * Reports the executor that ran a block, read from the result of processing it. A block rejected
   * before its transactions ran has no execution path and is not reported.
   *
   * @param header the block's header
   * @param result the result of processing the block
   * @param accessListMissing why the runner imported the block without its delivered access list,
   *     or empty when it did not
   */
  void report(
      final BlockHeader header,
      final BlockProcessingResult result,
      final Optional<AccessListMissing> accessListMissing) {
    final String missing = accessListMissing.map(m -> m.reason).orElse("");
    result
        .getExecutionPath()
        .ifPresent(
            path -> {
              switch (path) {
                case PARALLEL_BAL -> printParallel(header, "bal", missing);
                case PARALLEL_OPTIMISTIC -> printParallel(header, "optimistic", missing);
                case SEQUENTIAL -> printSequential(header, "disabled", missing);
                case SEQUENTIAL_NOT_PATH_BASED ->
                    printSequential(header, "not-path-based", missing);
                case SEQUENTIAL_FALLBACK -> {
                  // Only an optimistic parallel attempt is rerun sequentially.
                  printParallel(header, "optimistic", missing);
                  printFallback(header, result);
                }
              }
            });
  }

  private void printParallel(
      final BlockHeader header, final String scheduler, final String accessListMissing) {
    final ObjectNode line = event("balExecution", header);
    line.put("path", "parallel");
    line.put("reason", accessListMissing);
    line.put("scheduler", scheduler);
    print(line);
  }

  private void printSequential(
      final BlockHeader header, final String reason, final String accessListMissing) {
    final ObjectNode line = event("balExecution", header);
    line.put("path", "sequential");
    line.put("reason", accessListMissing.isEmpty() ? reason : accessListMissing);
    print(line);
  }

  private void printFallback(final BlockHeader header, final BlockProcessingResult result) {
    final ObjectNode line = event("balFallback", header);
    line.put("parallelError", result.getParallelErrorMessage().orElse(""));
    line.put("sequentialResult", result.isSuccessful() ? "valid" : "invalid");
    line.put("sequentialError", result.isSuccessful() ? "" : result.errorMessage.orElse(""));
    print(line);
  }

  private static ObjectNode event(final String name, final BlockHeader header) {
    final ObjectNode line = MAPPER.createObjectNode();
    line.put("event", name);
    line.put("block", header.getNumber());
    line.put("hash", header.getHash().toHexString());
    return line;
  }

  private void print(final ObjectNode line) {
    // One println per line, so lines from concurrent workers never split each other.
    err.println(line.toString());
  }
}
