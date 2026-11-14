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
package org.hyperledger.besu.tests.acceptance.plugins;

import org.hyperledger.besu.plugin.CoreConfiguration;
import org.hyperledger.besu.plugin.RegistrationContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;

/** The directory the acceptance tests watch for signal files: {@code <data-path>/plugins}. */
final class PluginCallbackDir {
  private PluginCallbackDir() {}

  static File resolve(final RegistrationContext context) {
    return context
        .getBesuService(CoreConfiguration.class)
        .getDataPath()
        .resolve("plugins")
        .toFile();
  }

  /** Writes the lines as a signal file, each followed by a line separator. */
  static void write(final File dir, final String name, final List<String> lines) {
    write(dir, name, String.join(System.lineSeparator(), lines) + System.lineSeparator());
  }

  /**
   * Writes the content as is into the directory, creating the directory if needed; both go at JVM
   * exit.
   */
  static void write(final File dir, final String name, final String content) {
    try {
      final File file = new File(dir, name);
      if (!dir.exists()) {
        dir.mkdirs();
        dir.deleteOnExit();
      }
      Files.writeString(file.toPath(), content);
      file.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Writes the lines as a signal file that outlives the JVM, for a signal written from {@code
   * stop()}: a file registered for deletion at exit, as {@link #write} does, is gone by the time a
   * test can look at it. The node's data directory is temporary, so nothing is left behind.
   */
  static void writeKept(final File dir, final String name, final List<String> lines) {
    writeKept(
        dir, name, String.join(System.lineSeparator(), lines) + System.lineSeparator(), false);
  }

  /**
   * Appends the line to a file that outlives the JVM, creating both if needed. For signals several
   * plugins write to one file, in the order they write them.
   */
  static void append(final File dir, final String name, final String line) {
    writeKept(dir, name, line + System.lineSeparator(), true);
  }

  private static void writeKept(
      final File dir, final String name, final String content, final boolean append) {
    try {
      if (!dir.exists()) {
        dir.mkdirs();
      }
      Files.writeString(
          new File(dir, name).toPath(),
          content,
          StandardOpenOption.CREATE,
          append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING);
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
