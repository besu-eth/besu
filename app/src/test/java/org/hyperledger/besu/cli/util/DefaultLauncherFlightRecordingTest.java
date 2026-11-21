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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * The default {@code besu} launcher must not pass {@code -XX:StartFlightRecording}. That option
 * writes a continuous recording under {@code java.io.tmpdir}, and the JVM deletes it only on a
 * clean shutdown. Killed runs leave the files behind until the temp directory fills and JFR stops
 * the next JVM.
 */
public class DefaultLauncherFlightRecordingTest {

  @Test
  public void defaultLauncherDoesNotStartFlightRecordingInTmpdir() throws IOException {
    final String buildGradle = Files.readString(rootBuildGradle());
    final String defaultJvmOpts =
        stripLineComments(block(buildGradle, "def defaultJvmOpts25", "def untunedJvmOpts"));
    final String applicationJvmArgs =
        stripLineComments(block(buildGradle, "applicationDefaultJvmArgs = [", "]\n}"));

    assertThat(defaultJvmOpts)
        .as(
            "defaultJvmOpts25 is embedded in the generated besu start script "
                + "(startScripts.defaultJvmOpts). -XX:StartFlightRecording with no repository "
                + "writes the continuous recording under java.io.tmpdir")
        .doesNotContain("StartFlightRecording")
        .doesNotContain("default.jfc");
    assertThat(applicationJvmArgs)
        .as("applicationDefaultJvmArgs are included in defaultJvmOpts25")
        .doesNotContain("StartFlightRecording");

    final String unixTemplate =
        Files.readString(
            rootBuildGradle().getParent().resolve("app/src/main/scripts/unixStartScript.txt"));
    final String windowsTemplate =
        Files.readString(
            rootBuildGradle().getParent().resolve("app/src/main/scripts/windowsStartScript.txt"));
    assertThat(unixTemplate).doesNotContain("StartFlightRecording");
    assertThat(windowsTemplate).doesNotContain("StartFlightRecording");
    assertThat(unixTemplate).contains("DEFAULT_JVM_OPTS=${defaultJvmOpts}");
    assertThat(windowsTemplate).contains("DEFAULT_JVM_OPTS=${defaultJvmOpts}");
  }

  private static String block(final String text, final String startMarker, final String endMarker) {
    final int start = text.indexOf(startMarker);
    assertThat(start).as("missing '%s'", startMarker).isNotNegative();
    final int end = text.indexOf(endMarker, start + startMarker.length());
    assertThat(end).as("missing '%s' after '%s'", endMarker, startMarker).isNotNegative();
    return text.substring(start, end);
  }

  private static String stripLineComments(final String text) {
    return text.replaceAll("(?m)//.*$", "");
  }

  private static Path rootBuildGradle() throws IOException {
    Path dir = Path.of("").toAbsolutePath();
    while (dir != null) {
      final Path candidate = dir.resolve("build.gradle");
      if (Files.isRegularFile(candidate)
          && Files.readString(candidate).contains("def defaultJvmOpts25")) {
        return candidate;
      }
      dir = dir.getParent();
    }
    throw new AssertionError("Could not find the root build.gradle that defines defaultJvmOpts25");
  }
}
