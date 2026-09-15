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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Fixture handling shared by {@code block-test} and {@code state-test}: expanding paths into
 * fixture files, running those files over a worker pool, tallying results, and emitting {@code
 * --json-array}.
 */
final class FixtureRunner {

  private static final ObjectMapper JSON_ARRAY_MAPPER = JsonUtils.createObjectMapper();

  private FixtureRunner() {}

  /**
   * Whether {@code path} lies under a {@code .meta} directory, which fixture trees use for
   * generation metadata rather than runnable fixtures.
   *
   * <p>Compared element by element rather than by searching the rendered path for {@code "/.meta/"}
   * so the check holds wherever the platform separator is not {@code '/'}.
   */
  private static boolean isUnderMetaDirectory(final Path path) {
    for (final Path element : path) {
      if (".meta".equals(element.toString())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Collects the fixture files named by {@code paths}, expanding directories recursively.
   *
   * @param paths the paths given on the command line
   * @return the {@code .json} fixture files to run, directories expanded and sorted
   * @throws IOException if a path resolves to nothing, or a directory cannot be walked
   */
  static List<Path> collectFiles(final List<Path> paths) throws IOException {
    final List<Path> files = new ArrayList<>();
    for (final Path path : paths) {
      if ("stdin".equals(path.toString())) {
        files.add(path);
      } else if (path.toFile().isDirectory()) {
        try (Stream<Path> stream = Files.walk(path)) {
          stream
              .filter(p -> p.toString().endsWith(".json"))
              .filter(p -> !isUnderMetaDirectory(p))
              .sorted()
              .forEach(files::add);
        }
      } else if (path.toFile().isFile()) {
        files.add(path);
      } else {
        // An empty file list means "read filenames from stdin", so dropping an unresolvable path
        // here would leave the command blocked on stdin instead of reporting the bad path.
        throw new FileNotFoundException("File not found: " + path);
      }
    }
    return files;
  }

  /**
   * Applies {@code task} to every file, on a pool of {@code workers} threads when there is more
   * than one of each, and on the calling thread otherwise.
   *
   * @param files the fixture files to run
   * @param workers the requested worker count
   * @param task the per-file action
   * @throws InterruptedException if interrupted while awaiting a worker
   * @throws ExecutionException if a worker raised an exception
   */
  static void runFiles(final List<Path> files, final int workers, final Consumer<Path> task)
      throws InterruptedException, ExecutionException {
    if (workers > 1 && files.size() > 1) {
      final ExecutorService executor = Executors.newFixedThreadPool(workers);
      try {
        final List<Future<?>> futures = new ArrayList<>();
        for (final Path file : files) {
          futures.add(executor.submit(() -> task.accept(file)));
        }
        for (final Future<?> future : futures) {
          future.get();
        }
      } finally {
        executor.shutdown();
      }
    } else {
      for (final Path file : files) {
        task.accept(file);
      }
    }
  }

  /**
   * Prints the collected {@code --json-array} payload, falling back to an empty array so the output
   * stays parseable if serialisation fails.
   *
   * @param out where to print
   * @param results the per-test result nodes
   */
  static void printJsonArray(final PrintWriter out, final List<ObjectNode> results) {
    try {
      out.println(JSON_ARRAY_MAPPER.writeValueAsString(results));
    } catch (final JsonProcessingException e) {
      out.println("[]");
    }
  }

  /**
   * Creates a node for one row of the {@code --json-array} payload.
   *
   * @return an empty object node
   */
  static ObjectNode newResultNode() {
    return JSON_ARRAY_MAPPER.createObjectNode();
  }

  /**
   * Pass/fail tallies and the end-of-run summary block.
   *
   * <p>Unreadable fixtures are counted separately from failures. A file we cannot build a test from
   * is a fixture problem rather than a Besu one, and counting it as a failure would make a fixture
   * format change look like a regression.
   */
  static final class TestResults {
    private static final String SEPARATOR = "=".repeat(80);

    private final AtomicInteger passedTests = new AtomicInteger(0);
    private final AtomicInteger failedTests = new AtomicInteger(0);
    private final Map<String, String> failures = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, String> unreadable =
        Collections.synchronizedMap(new LinkedHashMap<>());

    void recordPass() {
      passedTests.incrementAndGet();
    }

    void recordFailure(final String testName, final String reason) {
      failedTests.incrementAndGet();
      failures.put(testName, reason);
    }

    void recordUnreadable(final String file, final String reason) {
      unreadable.put(file, reason);
    }

    boolean hasTests() {
      return passedTests.get() + failedTests.get() > 0;
    }

    int failed() {
      return failedTests.get();
    }

    void printSummary(final PrintWriter out) {
      final int totalTests = passedTests.get() + failedTests.get();
      out.println();
      out.println(SEPARATOR);
      out.println("TEST SUMMARY");
      out.println(SEPARATOR);
      out.printf("Total tests:  %d%n", totalTests);
      out.printf("Passed:       %d%n", passedTests.get());
      out.printf("Failed:       %d%n", failedTests.get());
      if (!unreadable.isEmpty()) {
        out.printf("Unreadable:   %d file(s)%n", unreadable.size());
      }
      if (!failures.isEmpty()) {
        out.println("\nFailed tests:");
        failures.forEach((name, reason) -> out.printf("  - %s: %s%n", name, reason));
      }
      if (!unreadable.isEmpty()) {
        out.println("\nUnreadable files (no test in them ran):");
        unreadable.forEach((file, reason) -> out.printf("  - %s: %s%n", file, reason));
      }
      out.println(SEPARATOR);
    }
  }
}
