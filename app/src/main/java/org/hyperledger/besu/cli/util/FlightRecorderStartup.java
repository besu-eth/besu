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
package org.hyperledger.besu.cli.util;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import jdk.jfr.Configuration;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Recording;
import jdk.jfr.RecordingState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts one continuous JFR recording under {@code data-path/jfr} and removes recordings a previous
 * killed run left there.
 *
 * <p>The repository is set with the same {@code JFR.configure repositorypath} command {@code jcmd}
 * uses, before the recording starts, so chunks are not written to the temporary directory. The
 * recording is named {@code besu}. {@code jcmd <pid> JFR.dump name=besu filename=...} copies it.
 */
public final class FlightRecorderStartup {
  private static final Logger LOG = LoggerFactory.getLogger(FlightRecorderStartup.class);

  /** Recording name used by {@code jcmd <pid> JFR.dump name=besu}. */
  public static final String RECORDING_NAME = "besu";

  /** Directory under the data path that holds the repository. */
  public static final String DIRECTORY_NAME = "jfr";

  /**
   * Default cap the JDK applies when {@code -XX:StartFlightRecording} is set with no max size ("No
   * limit specified, using maxsize=250MB as default.").
   */
  static final long MAX_REPOSITORY_BYTES = 250L * 1024L * 1024L;

  /** Repository subdirectory JFR creates: {@code yyyy_MM_dd_HH_mm_ss_pid}. */
  static final Pattern REPOSITORY_DIRECTORY =
      Pattern.compile("\\d{4}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_\\d+");

  private static final String DIAGNOSTIC_COMMAND = "com.sun.management:type=DiagnosticCommand";

  private static Recording active;

  private FlightRecorderStartup() {}

  /**
   * Repository directory for a data directory.
   *
   * @param dataDirectory Besu data directory
   * @return {@code dataDirectory/jfr}
   */
  public static Path repositoryDirectory(final Path dataDirectory) {
    return dataDirectory.toAbsolutePath().resolve(DIRECTORY_NAME);
  }

  /**
   * Starts the recording when {@code enabled} is true. A second call in this process does nothing,
   * so an Ephemery restart does not delete the recording it just started.
   *
   * @param enabled operator opt-in
   * @param dataDirectory Besu data directory
   */
  public static void startIfEnabled(final boolean enabled, final Path dataDirectory) {
    if (!enabled) {
      return;
    }
    start(dataDirectory);
  }

  /**
   * Deletes leftover recordings, points JFR at {@code data-path/jfr}, and starts the {@code besu}
   * recording. A failure is logged and does not propagate: an error inside JFR itself stops the
   * JVM, which is the crash loop this avoids.
   *
   * @param dataDirectory Besu data directory
   */
  public static synchronized void start(final Path dataDirectory) {
    if (active != null) {
      return;
    }
    final Path repository = repositoryDirectory(dataDirectory);
    Recording recording = null;
    try {
      final int removed = deleteLeftoverRecordings(repository);
      if (removed > 0) {
        LOG.info("Deleted {} leftover JFR recording(s) from {}", removed, repository);
      }
      if (!FlightRecorder.isAvailable()) {
        LOG.warn("JFR is not available in this JVM. Continuing without a recording.");
        return;
      }
      Files.createDirectories(repository);
      configureRepository(repository);
      recording = openRecording();
      recording.start();
      active = recording;
      recording = null;
      LOG.info(
          "Started continuous JFR recording '{}' under {}. Dump with: jcmd <pid> JFR.dump name={}"
              + " filename=besu.jfr",
          RECORDING_NAME,
          repository,
          RECORDING_NAME);
    } catch (final Exception e) {
      LOG.error(
          "Could not start the JFR recording under {}. Continuing without it.", repository, e);
      if (recording != null) {
        recording.close();
      }
    }
  }

  /**
   * Deletes JFR repository directories and {@code .jfr} files left in {@code repository} by a
   * previous run. Other files in the directory are left in place.
   *
   * @param repository {@code data-path/jfr}
   * @return how many files and repository directories were deleted
   * @throws IOException when a leftover cannot be deleted
   */
  static int deleteLeftoverRecordings(final Path repository) throws IOException {
    if (!Files.isDirectory(repository)) {
      return 0;
    }
    int removed = 0;
    try (Stream<Path> children = Files.list(repository)) {
      for (final Path child : children.toList()) {
        if (isLeftoverRecording(child)) {
          deleteRecursively(child);
          removed++;
        }
      }
    }
    return removed;
  }

  static boolean isLeftoverRecording(final Path child) {
    final String name = child.getFileName().toString();
    if (REPOSITORY_DIRECTORY.matcher(name).matches()) {
      return true;
    }
    return name.endsWith(".jfr") && !Files.isDirectory(child);
  }

  /** Stops the recording started by this process so tests can retarget the repository. */
  static synchronized void stopRecording() {
    final Recording recording = active;
    active = null;
    if (recording == null) {
      return;
    }
    try {
      if (recording.getState() == RecordingState.RUNNING) {
        recording.stop();
      }
    } catch (final Exception e) {
      LOG.debug("Stopping the JFR recording failed", e);
    } finally {
      recording.close();
    }
  }

  private static Recording openRecording() throws Exception {
    final Recording recording = new Recording(Configuration.getConfiguration("default"));
    recording.setName(RECORDING_NAME);
    recording.setToDisk(true);
    recording.setMaxSize(MAX_REPOSITORY_BYTES);
    recording.setDumpOnExit(false);
    return recording;
  }

  private static void configureRepository(final Path repository) throws Exception {
    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    final ObjectName command = new ObjectName(DIAGNOSTIC_COMMAND);
    final String argument =
        "repositorypath=\"" + repository.toString().replace("\"", "\\\"") + "\"";
    server.invoke(
        command,
        "jfrConfigure",
        new Object[] {new String[] {argument}},
        new String[] {"[Ljava.lang.String;"});
  }

  private static void deleteRecursively(final Path path) throws IOException {
    if (Files.isDirectory(path) && !Files.isSymbolicLink(path)) {
      try (Stream<Path> walk = Files.walk(path)) {
        for (final Path child : walk.sorted(Comparator.reverseOrder()).toList()) {
          Files.deleteIfExists(child);
        }
      }
      return;
    }
    Files.deleteIfExists(path);
  }
}
