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
package org.hyperledger.besu.cli.subcommands.storage;

import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_ARCHIVE_WITH_CODE_FORMAT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_WITH_CODE_FORMAT;

import org.hyperledger.besu.cli.DefaultCommandValues;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.VersionedStorageFormat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tells the operator how to undo the upgrade to the versioned code format, once it has happened,
 * with a command that can be pasted as it is: the same Besu executable and arguments this process
 * was started with, run as the same user, followed by the revert subcommand.
 */
public final class CodeFormatDowngradeWarning {
  private static final Pattern SAFE_ARGUMENT = Pattern.compile("[A-Za-z0-9_@%+=:,./-]+");

  private CodeFormatDowngradeWarning() {}

  /**
   * Reads the storage format the database metadata records, to be compared before and after the
   * storage is opened.
   *
   * @param dataDir the data directory
   * @return the recorded format, empty when there is no database yet
   */
  public static Optional<VersionedStorageFormat> storageFormat(final Path dataDir) {
    try {
      if (!DatabaseMetadata.isPresent(dataDir)) {
        return Optional.empty();
      }
      return Optional.of(DatabaseMetadata.lookUpFrom(dataDir).getVersionedStorageFormat());
    } catch (final IOException | RuntimeException e) {
      return Optional.empty();
    }
  }

  /**
   * The warning to log when opening the storage upgraded the database to the versioned code format.
   *
   * @param dataDir the data directory
   * @param before the storage format recorded before the storage was opened
   * @param arguments the arguments this process was started with
   * @return the warning, empty when nothing was upgraded
   */
  public static Optional<String> after(
      final Path dataDir,
      final Optional<VersionedStorageFormat> before,
      final List<String> arguments) {
    return after(
        dataDir,
        before,
        storageFormat(dataDir),
        executable(System.getProperty(DefaultCommandValues.BESU_HOME_PROPERTY_NAME)),
        System.getProperty("user.name"),
        arguments);
  }

  static Optional<String> after(
      final Path dataDir,
      final Optional<VersionedStorageFormat> before,
      final Optional<VersionedStorageFormat> after,
      final String executable,
      final String user,
      final List<String> arguments) {
    if (before.isEmpty() || after.isEmpty() || before.get().equals(after.get())) {
      return Optional.empty();
    }
    if (after.get() != BONSAI_WITH_CODE_FORMAT && after.get() != BONSAI_ARCHIVE_WITH_CODE_FORMAT) {
      return Optional.empty();
    }
    return Optional.of(
        String.format(
            "The database in %s was upgraded from %s to %s, which Besu versions before this one"
                + " cannot open. To downgrade Besu, stop it and revert the database first with:%n%s",
            dataDir,
            describe(before.get()),
            describe(after.get()),
            command(executable, user, arguments)));
  }

  static String executable(final String besuHome) {
    return besuHome == null ? "besu" : Path.of(besuHome, "bin", "besu").toString();
  }

  static String command(final String executable, final String user, final List<String> arguments) {
    return Stream.concat(
            Stream.of("sudo", "-u", user, executable),
            Stream.concat(arguments.stream(), Stream.of("storage", "revert-code-format")))
        .map(CodeFormatDowngradeWarning::quote)
        .collect(Collectors.joining(" "));
  }

  private static String describe(final VersionedStorageFormat format) {
    return format.getFormat() + " version " + format.getVersion();
  }

  private static String quote(final String argument) {
    if (SAFE_ARGUMENT.matcher(argument).matches()) {
      return argument;
    }
    return "'" + argument.replace("'", "'\\''") + "'";
  }
}
