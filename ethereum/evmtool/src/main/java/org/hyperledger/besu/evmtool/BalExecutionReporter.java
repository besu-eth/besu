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
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionPathListener;

import java.io.PrintStream;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Under --bal-report, prints one JSON line to stderr for each block block-test and engine-test
 * execute, naming the executor that ran it, and one more when the parallel executor failed a block
 * and it was run again sequentially, so a fixture's verdict can be tied to the path that produced
 * it. Lines from different workers interleave; the block hash ties each to its block.
 */
final class BalExecutionReporter implements BlockExecutionPathListener {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final String BAD_ACCESS_LIST = "bad-access-list";

  static final String WITHHELD = "withheld";

  // Set while block-test imports a block without its delivered access list, to the reason it went
  // without. The listener is called on the importing thread, so a worker's reason never reaches
  // another worker's block.
  private static final ThreadLocal<String> ACCESS_LIST_MISSING = ThreadLocal.withInitial(() -> "");

  private final PrintStream err;

  BalExecutionReporter(final PrintStream err) {
    this.err = err;
  }

  /**
   * Runs a block import, reporting its block with the given reason whatever path runs it: {@code
   * bad-access-list} when the runner dropped the block's delivered access list, {@code withheld}
   * when it withheld it under --bal-withhold.
   *
   * @param accessListMissing why the runner imports the block without its delivered access list, or
   *     empty when it does not
   * @param importBlock the import
   * @return the import's result
   */
  static <T> T importing(final String accessListMissing, final Supplier<T> importBlock) {
    ACCESS_LIST_MISSING.set(accessListMissing);
    try {
      return importBlock.get();
    } finally {
      ACCESS_LIST_MISSING.remove();
    }
  }

  @Override
  public void onParallel(final BlockHeader header, final String scheduler) {
    final ObjectNode line = event("balExecution", header);
    line.put("path", "parallel");
    line.put("reason", ACCESS_LIST_MISSING.get());
    line.put("scheduler", scheduler);
    print(line);
  }

  @Override
  public void onSequential(final BlockHeader header, final String reason) {
    final ObjectNode line = event("balExecution", header);
    line.put("path", "sequential");
    final String accessListMissing = ACCESS_LIST_MISSING.get();
    line.put("reason", accessListMissing.isEmpty() ? reason : accessListMissing);
    print(line);
  }

  @Override
  public void onSequentialFallback(
      final BlockHeader header,
      final BlockProcessingResult parallelResult,
      final BlockProcessingResult sequentialResult) {
    final ObjectNode line = event("balFallback", header);
    line.put("parallelError", parallelResult.errorMessage.orElse(""));
    line.put("sequentialResult", sequentialResult.isSuccessful() ? "valid" : "invalid");
    line.put(
        "sequentialError",
        sequentialResult.isSuccessful() ? "" : sequentialResult.errorMessage.orElse(""));
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
