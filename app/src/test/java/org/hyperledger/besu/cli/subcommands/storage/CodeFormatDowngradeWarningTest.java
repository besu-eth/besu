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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_ARCHIVE_WITH_CODE_FORMAT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_ARCHIVE_WITH_RECEIPT_COMPACTION;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_WITH_CODE_FORMAT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_WITH_RECEIPT_COMPACTION;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.FOREST_WITH_RECEIPT_COMPACTION;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.FOREST_WITH_VARIABLES;

import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CodeFormatDowngradeWarningTest {

  private static final Path DATA_DIR = Path.of("/data/besu");
  private static final String EXECUTABLE = "/opt/besu/current/bin/besu";
  private static final List<String> ARGUMENTS = List.of("--config-file=/etc/besu/config.toml");

  @Test
  void warnsWithAPastableCommandAfterTheUpgradeToTheCodeFormat() {
    final Optional<String> warning =
        CodeFormatDowngradeWarning.after(
            DATA_DIR,
            Optional.of(BONSAI_WITH_RECEIPT_COMPACTION),
            Optional.of(BONSAI_WITH_CODE_FORMAT),
            EXECUTABLE,
            "besu",
            ARGUMENTS);

    assertThat(warning)
        .contains(
            "The database in /data/besu was upgraded from BONSAI version 3 to BONSAI version 4,"
                + " which Besu versions before this one cannot open. To downgrade Besu, stop it and"
                + " revert the database first with:"
                + System.lineSeparator()
                + "sudo -u besu /opt/besu/current/bin/besu --config-file=/etc/besu/config.toml"
                + " storage revert-code-format");
  }

  @Test
  void warnsForTheArchiveFormatToo() {
    assertThat(
            CodeFormatDowngradeWarning.after(
                DATA_DIR,
                Optional.of(BONSAI_ARCHIVE_WITH_RECEIPT_COMPACTION),
                Optional.of(BONSAI_ARCHIVE_WITH_CODE_FORMAT),
                EXECUTABLE,
                "besu",
                ARGUMENTS))
        .hasValueSatisfying(
            warning ->
                assertThat(warning)
                    .contains("X_BONSAI_ARCHIVE version 2 to X_BONSAI_ARCHIVE version 3"));
  }

  @Test
  void staysSilentWhenNothingWasUpgraded() {
    assertThat(
            CodeFormatDowngradeWarning.after(
                DATA_DIR,
                Optional.of(BONSAI_WITH_CODE_FORMAT),
                Optional.of(BONSAI_WITH_CODE_FORMAT),
                EXECUTABLE,
                "besu",
                ARGUMENTS))
        .isEmpty();
  }

  @Test
  void staysSilentForANewDatabase() {
    assertThat(
            CodeFormatDowngradeWarning.after(
                DATA_DIR,
                Optional.empty(),
                Optional.of(BONSAI_WITH_CODE_FORMAT),
                EXECUTABLE,
                "besu",
                ARGUMENTS))
        .isEmpty();
  }

  @Test
  void staysSilentForUpgradesToOtherFormats() {
    assertThat(
            CodeFormatDowngradeWarning.after(
                DATA_DIR,
                Optional.of(FOREST_WITH_VARIABLES),
                Optional.of(FOREST_WITH_RECEIPT_COMPACTION),
                EXECUTABLE,
                "besu",
                ARGUMENTS))
        .isEmpty();
  }

  @Test
  void quotesArgumentsTheShellWouldSplitOrExpand() {
    final String command =
        CodeFormatDowngradeWarning.command(
            "/opt/besu 1/bin/besu",
            "besu",
            List.of("--data-path", "/data/my node", "--identity=it's", "--network=mainnet"));

    assertThat(command)
        .isEqualTo(
            "sudo -u besu '/opt/besu 1/bin/besu' --data-path '/data/my node' '--identity=it'\\''s'"
                + " --network=mainnet storage revert-code-format");
  }

  @Test
  void fallsBackToTheExecutableOnThePathWithoutAnInstallDirectory() {
    assertThat(CodeFormatDowngradeWarning.executable(null)).isEqualTo("besu");
    assertThat(CodeFormatDowngradeWarning.executable("/opt/besu/current"))
        .isEqualTo(Path.of("/opt/besu/current/bin/besu").toString());
  }

  @Test
  void readsTheStorageFormatFromTheMetadata(@TempDir final Path dataDir) throws IOException {
    assertThat(CodeFormatDowngradeWarning.storageFormat(dataDir)).isEmpty();

    new DatabaseMetadata(BONSAI_WITH_RECEIPT_COMPACTION).writeToDirectory(dataDir);

    assertThat(CodeFormatDowngradeWarning.storageFormat(dataDir))
        .contains(BONSAI_WITH_RECEIPT_COMPACTION);
  }
}
