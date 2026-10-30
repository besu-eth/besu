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
import java.io.InputStream;
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
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jspecify.annotations.Nullable;

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
   * The test cases a fixture declares that its typed read could not reach, split by whether the
   * name filter selected them.
   *
   * @param notRun cases the run selected but never executed
   * @param filtered cases the name filter excluded anyway, so they count as filtered, not missing
   */
  record UnreadableCases(int notRun, int filtered) {

    /** Every case the fixture declares, selected or not. */
    int declared() {
      return notRun + filtered;
    }
  }

  /** Receives a fixture that could not be read as the expected test type. */
  @FunctionalInterface
  interface UnreadableFixtureHandler {
    /**
     * Records the failed read.
     *
     * @param cases the cases the fixture declared
     * @param error why the read failed
     */
    void accept(UnreadableCases cases, Exception error);
  }

  /**
   * Reads one fixture file into its typed test map, or reports it to {@code onUnreadable}.
   *
   * <p>The raw content is buffered rather than streamed into the typed read so that, if that read
   * fails, {@link #countUnreadableTestCases} can still count the cases from the same content:
   * standard input cannot be read twice. It is only reachable from this method, though, so it is
   * released as soon as the read returns rather than held for the whole of the file's run. That
   * matters on the largest fixtures, where the raw bytes outweigh the parsed tests.
   *
   * @param file the fixture file, or the literal {@code stdin} that {@link #collectFiles} passes
   *     through to mean standard input
   * @param stdin the command's standard input, or {@code null} if {@code file} always names a file.
   *     Pass {@code null} when the file names were themselves read from standard input, where an
   *     entry named {@code stdin} is a file, and the stream is the list still being read.
   * @param mapper the mapper to read with
   * @param javaType the {@code Map<String, T>} type to read
   * @param selects whether the name filter selects a test, so an unreadable file's unselected cases
   *     count as filtered
   * @param onUnreadable called, instead of returning tests, when the file cannot be read
   * @param <T> the test case type
   * @return the tests, or {@code null} if the file was reported unreadable
   */
  static <T> @Nullable Map<String, T> readTests(
      final Path file,
      final @Nullable InputStream stdin,
      final ObjectMapper mapper,
      final JavaType javaType,
      final Predicate<String> selects,
      final UnreadableFixtureHandler onUnreadable) {
    byte @Nullable [] content = null;
    try {
      content =
          stdin != null && "stdin".equals(file.toString())
              ? stdin.readAllBytes()
              : Files.readAllBytes(file);
      return mapper.readValue(content, javaType);
    } catch (final Exception e) {
      // Count what the file declared even though it could not be read, so the cases it
      // contributes show up as unaccounted rather than vanishing from the totals.
      onUnreadable.accept(countUnreadableTestCases(content, selects), e);
      return null;
    }
  }

  /**
   * Counts the test cases a fixture declares, by reading its top-level object fields.
   *
   * <p>Deliberately generic: this has to work on a fixture whose typed deserialization failed,
   * which is the case the run reconciliation exists to catch. For the same reason it never answers
   * 0: a fixture too broken to count still stands for at least one case that did not run, and
   * counting it as none would let it vanish behind a sibling that passed.
   *
   * <p>The top-level fields are the test names, so the name filter applies to them as it would have
   * to the tests: a run filtered to one fork is not failed by an unreadable fixture whose cases it
   * would never have selected.
   *
   * @param content the fixture content, or {@code null} if even reading it failed
   * @param selects whether the name filter selects a test
   * @return the declared cases, split into selected and filtered
   */
  static UnreadableCases countUnreadableTestCases(
      final byte @Nullable [] content, final Predicate<String> selects) {
    if (content == null) {
      return new UnreadableCases(1, 0);
    }
    final JsonNode root;
    try {
      root = JSON_ARRAY_MAPPER.readTree(content);
    } catch (final IOException e) {
      return new UnreadableCases(1, 0);
    }
    if (root == null || !root.isObject() || root.isEmpty()) {
      return new UnreadableCases(1, 0);
    }
    int notRun = 0;
    int filtered = 0;
    for (final String name : (Iterable<String>) root::fieldNames) {
      if (selects.test(name)) {
        notRun++;
      } else {
        filtered++;
      }
    }
    return new UnreadableCases(notRun, filtered);
  }

  /**
   * Creates a failing {@code --json-array} row for something the run could not execute: an
   * unreadable fixture, or the declared cases that never ran. Without these rows a run could exit 1
   * while every row in its array passed, leaving a consumer of the array nothing to explain it.
   *
   * @param name what the row stands for
   * @param error why it did not run
   * @param emptyFields the command's other row fields, set empty so every row has the same shape
   * @return the row
   */
  static ObjectNode notRunRow(final String name, final String error, final String... emptyFields) {
    final ObjectNode row = newResultNode();
    row.put("name", name);
    row.put("pass", false);
    for (final String field : emptyFields) {
      row.put(field, "");
    }
    row.put("error", error);
    return row;
  }

  /**
   * The message for declared cases that never ran, shared by the summary and the {@code
   * --json-array} row.
   */
  static String notRunMessage(final int unaccounted, final int declared) {
    return String.format("%d of %d declared case(s) never executed", unaccounted, declared);
  }

  /**
   * Pass/fail tallies and the end-of-run summary block.
   *
   * <p>Unreadable fixtures are counted separately from failures. A file we cannot build a test from
   * is a fixture problem rather than a Besu one, and counting it as a failure would make a fixture
   * format change look like a regression.
   *
   * <p>They are still reconciled against the case count the fixtures declare, though. An unreadable
   * file used to be a footnote under a "0 failed" summary, so a fixture Besu could not even
   * deserialize silently removed its cases from the run while the suite still reported success.
   */
  static final class TestResults {
    private static final String SEPARATOR = "=".repeat(80);

    private final AtomicInteger passedTests = new AtomicInteger(0);
    private final AtomicInteger failedTests = new AtomicInteger(0);
    private final AtomicInteger declaredCases = new AtomicInteger(0);
    private final AtomicInteger filteredCases = new AtomicInteger(0);
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

    /** Records how many cases a fixture file declares, whether or not it could be deserialized. */
    void recordDeclaredCases(final int count) {
      declaredCases.addAndGet(count);
    }

    /** Records a case deliberately skipped by {@code --test-name} / {@code --test-name-regex}. */
    void recordFiltered() {
      filteredCases.incrementAndGet();
    }

    /**
     * Records a fixture that could not be read, with the cases it declared.
     *
     * @param file the fixture file
     * @param reason why it could not be read
     * @param cases the cases it declared, so the selected ones count as not run
     */
    void recordUnreadable(final String file, final String reason, final UnreadableCases cases) {
      unreadable.put(file, reason);
      declaredCases.addAndGet(cases.declared());
      filteredCases.addAndGet(cases.filtered());
    }

    /**
     * Appends a failing {@code --json-array} row for each unreadable fixture and for the declared
     * cases that never ran, so the array accounts for a non-zero exit on its own.
     *
     * @param rows the array being built
     * @param emptyFields the command's other row fields
     */
    void addNotRunRows(final List<ObjectNode> rows, final String... emptyFields) {
      synchronized (unreadable) {
        unreadable.forEach((file, reason) -> rows.add(notRunRow(file, reason, emptyFields)));
      }
      final int unaccounted = unaccountedCases();
      if (unaccounted > 0) {
        rows.add(
            notRunRow("NOT RUN", notRunMessage(unaccounted, declaredCases.get()), emptyFields));
      }
    }

    boolean hasTests() {
      return passedTests.get() + failedTests.get() > 0;
    }

    /**
     * The cases that were declared by the fixtures but neither run nor deliberately filtered out.
     *
     * @return the shortfall, or 0 when every declared case is accounted for
     */
    int unaccountedCases() {
      final int accounted = passedTests.get() + failedTests.get() + filteredCases.get();
      return Math.max(0, declaredCases.get() - accounted);
    }

    int failed() {
      return failedTests.get();
    }

    void printSummary(final PrintWriter out) {
      final int totalTests = passedTests.get() + failedTests.get();
      final int unaccounted = unaccountedCases();
      out.println();
      out.println(SEPARATOR);
      out.println("TEST SUMMARY");
      out.println(SEPARATOR);
      out.printf("Total tests:  %d%n", totalTests);
      out.printf("Passed:       %d%n", passedTests.get());
      out.printf("Failed:       %d%n", failedTests.get());
      if (filteredCases.get() > 0) {
        out.printf("Filtered out: %d%n", filteredCases.get());
      }
      if (!unreadable.isEmpty()) {
        out.printf("Unreadable:   %d file(s)%n", unreadable.size());
      }
      if (unaccounted > 0) {
        out.printf("NOT RUN:      %s%n", notRunMessage(unaccounted, declaredCases.get()));
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
