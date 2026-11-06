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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import jdk.jfr.FlightRecorder;
import jdk.jfr.Recording;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FlightRecorderStartupTest {

  @TempDir Path tempDir;

  @AfterEach
  void stopRecording() {
    FlightRecorderStartup.stopRecording();
  }

  @Test
  void missingRepositoryIsLeftUncreated() throws IOException {
    final Path repository = tempDir.resolve("absent");

    assertThat(FlightRecorderStartup.deleteLeftoverRecordings(repository)).isZero();
    assertThat(repository).doesNotExist();
  }

  @Test
  void deletesRepositoryDirectoriesAndJfrFilesButKeepsOtherFiles() throws IOException {
    final Path repository = tempDir.resolve("jfr");
    final Path killedRun = repository.resolve("2026_11_04_06_16_22_6395");
    Files.createDirectories(killedRun);
    Files.writeString(killedRun.resolve("2026_11_04_06_16_22.jfr"), "leftover-chunk");
    Files.writeString(repository.resolve("orphan.jfr"), "orphan");
    Files.writeString(repository.resolve("notes.txt"), "keep");
    Files.createDirectories(repository.resolve("cache"));
    Files.writeString(repository.resolve("cache").resolve("state"), "keep");

    final Path outside = tempDir.resolve("outside");
    Files.createDirectories(outside);
    Files.writeString(outside.resolve("secret.txt"), "keep");
    Files.createSymbolicLink(repository.resolve("2020_01_02_03_04_05_9"), outside);

    assertThat(FlightRecorderStartup.deleteLeftoverRecordings(repository)).isEqualTo(3);

    assertThat(killedRun).doesNotExist();
    assertThat(repository.resolve("orphan.jfr")).doesNotExist();
    assertThat(repository.resolve("2020_01_02_03_04_05_9")).doesNotExist();
    assertThat(repository.resolve("notes.txt")).hasContent("keep");
    assertThat(repository.resolve("cache").resolve("state")).hasContent("keep");
    assertThat(outside.resolve("secret.txt")).hasContent("keep");
  }

  @Test
  void disabledOptInDoesNotTouchLeftoversOrTheTempDirectory() throws IOException {
    final Path dataDirectory = tempDir.resolve("data");
    final Path killedRun = dataDirectory.resolve("jfr").resolve("2026_11_04_06_16_22_6395");
    Files.createDirectories(killedRun);
    Files.writeString(killedRun.resolve("chunk.jfr"), "leftover");
    final Set<String> tempRecordingsBefore =
        recordingDirectoryNames(Path.of(System.getProperty("java.io.tmpdir")));

    FlightRecorderStartup.startIfEnabled(false, dataDirectory);

    assertThat(killedRun.resolve("chunk.jfr")).hasContent("leftover");
    assertThat(recordingDirectoryNames(Path.of(System.getProperty("java.io.tmpdir"))))
        .isEqualTo(tempRecordingsBefore);
  }

  @Test
  void enabledRecordingLivesUnderTheDataDirectoryAndCanBeDumpedByName() throws Exception {
    final Path dataDirectory = tempDir.resolve("data dir");
    Files.createDirectories(dataDirectory);
    final Path repository = FlightRecorderStartup.repositoryDirectory(dataDirectory);
    final Path killedRun = repository.resolve("2026_11_04_06_16_22_6395");
    Files.createDirectories(killedRun);
    Files.writeString(killedRun.resolve("2026_11_04_06_16_22.jfr"), "leftover");
    Files.writeString(repository.resolve("notes.txt"), "keep");
    final Path tempDirectory = Path.of(System.getProperty("java.io.tmpdir"));
    final Set<String> tempRecordingsBefore = recordingDirectoryNames(tempDirectory);

    FlightRecorderStartup.start(dataDirectory);

    assertThat(killedRun).doesNotExist();
    assertThat(repository.resolve("notes.txt")).hasContent("keep");
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(chunkFiles(repository)).isNotEmpty());
    assertThat(recordingDirectoryNames(repository)).isNotEmpty();
    assertThat(recordingDirectoryNames(tempDirectory)).isEqualTo(tempRecordingsBefore);

    final Recording recording = besuRecording();
    assertThat(recording.getName()).isEqualTo(FlightRecorderStartup.RECORDING_NAME);
    assertThat(recording.isToDisk()).isTrue();
    assertThat(recording.getDumpOnExit()).isFalse();
    assertThat(recording.getMaxSize()).isEqualTo(FlightRecorderStartup.MAX_REPOSITORY_BYTES);

    final Path dumpFile = tempDir.resolve("besu.jfr");
    final String dumped = jfrDump(dumpFile);
    assertThat(dumped).contains("besu");
    assertThat(dumpFile).isRegularFile();
    assertThat(Files.size(dumpFile)).isGreaterThan(0);

    final Path plantedDuringRun = repository.resolve("2020_01_01_00_00_00_1");
    Files.createDirectories(plantedDuringRun);
    final Path liveChunk = chunkFiles(repository).get(0);
    FlightRecorderStartup.start(dataDirectory);
    assertThat(liveChunk).exists();
    assertThat(plantedDuringRun).exists();
  }

  private static Recording besuRecording() {
    return FlightRecorder.getFlightRecorder().getRecordings().stream()
        .filter(recording -> FlightRecorderStartup.RECORDING_NAME.equals(recording.getName()))
        .findFirst()
        .orElseThrow();
  }

  private static String jfrDump(final Path dumpFile) throws Exception {
    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    final ObjectName command = new ObjectName("com.sun.management:type=DiagnosticCommand");
    final String filename = "filename=\"" + dumpFile.toAbsolutePath() + "\"";
    return (String)
        server.invoke(
            command,
            "jfrDump",
            new Object[] {new String[] {"name=besu", filename}},
            new String[] {"[Ljava.lang.String;"});
  }

  private static List<Path> chunkFiles(final Path repository) throws IOException {
    if (!Files.exists(repository)) {
      return List.of();
    }
    try (Stream<Path> walk = Files.walk(repository)) {
      return walk.filter(path -> path.getFileName().toString().endsWith(".jfr")).toList();
    }
  }

  private static Set<String> recordingDirectoryNames(final Path directory) throws IOException {
    if (!Files.isDirectory(directory)) {
      return Set.of();
    }
    try (Stream<Path> children = Files.list(directory)) {
      return children
          .map(path -> path.getFileName().toString())
          .filter(name -> FlightRecorderStartup.REPOSITORY_DIRECTORY.matcher(name).matches())
          .collect(Collectors.toSet());
    }
  }
}
