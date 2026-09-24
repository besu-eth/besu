/*
 * Copyright contributors to Hyperledger Besu.
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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_ARCHIVE_WITH_CODE_FORMAT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_ARCHIVE_WITH_RECEIPT_COMPACTION;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_WITH_CODE_FORMAT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_WITH_RECEIPT_COMPACTION;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code.CodeStorageMigration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.VersionedStorageFormat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/** Reverts the contract code storage to the format older Besu versions read. */
@Command(
    name = "revert-code-format",
    description =
        "Revert the contract code storage to the format read by Besu versions before the versioned"
            + " code format, so that the database can be opened by them again.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class RevertCodeFormatSubCommand implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RevertCodeFormatSubCommand.class);

  @SuppressWarnings("unused")
  @ParentCommand
  private StorageSubCommand parentCommand;

  /** Default constructor. */
  public RevertCodeFormatSubCommand() {}

  @Override
  public void run() {
    checkNotNull(parentCommand);
    // opening the database migrates a code storage that is not in the current format, so a
    // reverted one is migrated first and then reverted again
    try (final BesuController controller = parentCommand.besuCommand.buildController()) {
      CodeStorageMigration.revert(
          controller.getStorageProvider().getStorageBySegmentIdentifiers(List.of(CODE_STORAGE)));
    }
    revertMetadata(parentCommand.besuCommand.dataDir());
    LOG.info("The contract code storage can be read by older Besu versions again");
  }

  private static void revertMetadata(final Path dataDir) {
    try {
      final VersionedStorageFormat current =
          DatabaseMetadata.lookUpFrom(dataDir).getVersionedStorageFormat();
      final VersionedStorageFormat reverted;
      if (current == BONSAI_WITH_CODE_FORMAT) {
        reverted = BONSAI_WITH_RECEIPT_COMPACTION;
      } else if (current == BONSAI_ARCHIVE_WITH_CODE_FORMAT) {
        reverted = BONSAI_ARCHIVE_WITH_RECEIPT_COMPACTION;
      } else {
        LOG.info("Database metadata is {}, which needs no revert", current);
        return;
      }
      new DatabaseMetadata(reverted).writeToDirectory(dataDir);
      LOG.info("Reverted database metadata from {} to {}", current, reverted);
    } catch (final IOException e) {
      throw new IllegalStateException("Could not revert the database metadata in " + dataDir, e);
    }
  }
}
