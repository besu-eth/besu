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

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** The preimage subcommand. */
@CommandLine.Command(
    name = "preimage",
    description = "import/export hash preimage data",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    subcommands = {PreimageSubCommand.Export.class, PreimageSubCommand.Import.class})
public class PreimageSubCommand implements Runnable {

  /** Default constructor. */
  public PreimageSubCommand() {}

  private static final Logger LOG = LoggerFactory.getLogger(PreimageSubCommand.class);

  @SuppressWarnings("UnusedVariable")
  @CommandLine.ParentCommand
  private static StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec; // Picocli injects reference to command spec

  @Override
  public void run() {
    final PrintWriter out = spec.commandLine().getOut();
    spec.commandLine().usage(out);
  }

  private static BesuController createBesuController() {
    final DataStorageConfiguration config = parentCommand.besuCommand.getDataStorageConfiguration();
    // disable limit trie logs to avoid preloading during subcommand execution
    return parentCommand
        .besuCommand
        .setupControllerBuilder()
        .dataStorageConfiguration(
            ImmutableDataStorageConfiguration.copyOf(config)
                .withPathBasedExtraStorageConfiguration(
                    ImmutablePathBasedExtraStorageConfiguration.copyOf(
                            config.getPathBasedExtraStorageConfiguration())
                        .withLimitTrieLogsEnabled(false)))
        .build();
  }

  private static Path orDataDir() {
    return Paths.get(
        PreimageSubCommand.parentCommand
            .besuCommand
            .dataDir()
            .resolve("preimages.dat")
            .toAbsolutePath()
            .toString());
  }

  @CommandLine.Command(
      name = "export",
      description = "This command exports the preimages from persistent storage to a flat file",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  static class Export implements Runnable {

    @VisibleForTesting
    @CommandLine.Option(
        names = "--preimage-file-path",
        description = "The flat file to which to export preimage data",
        arity = "1..1")
    Path preimageFilePath = null;

    @VisibleForTesting
    @CommandLine.Option(
        names = "--hex",
        description = "export data in hex (human readable) format",
        arity = "1..1")
    boolean hex = false;

    StorageProvider storageProvider = null;

    StorageProvider getStorageProvider() {
      if (storageProvider == null) {
        storageProvider = createBesuController().getStorageProvider();
      }
      return storageProvider;
    }

    @Override
    public void run() {

      final StorageProvider storageProvider = getStorageProvider();
      var preimagesPath =
          Optional.ofNullable(preimageFilePath).orElseGet(PreimageSubCommand::orDataDir);

      var preimageStorage =
          storageProvider.getStorageBySegmentIdentifier(
              KeyValueSegmentIdentifier.HASH_PREIMAGE_STORE);

      try {
        if (hex) {
          // HEX MODE: Write each preimage as a hex string line
          try (var writer = Files.newBufferedWriter(preimagesPath, StandardCharsets.UTF_8)) {
            preimageStorage.stream()
                .forEach(
                    pair -> {
                      byte[] value = pair.getValue();
                      String hexString = Bytes.wrap(value).toHexString();
                      try {
                        writer.write(hexString);
                        writer.newLine();
                      } catch (IOException e) {
                        throw new UncheckedIOException("Failed to write hex preimage", e);
                      }
                    });
          }
        } else {
          // BINARY MODE: Write [1-byte length][N-byte value]
          try (var out = Files.newOutputStream(preimagesPath)) {
            preimageStorage.stream()
                .forEach(
                    pair -> {
                      byte[] value = pair.getValue();
                      int length = value.length;
                      if (length != 20 && length != 32) {
                        throw new IllegalArgumentException(
                            "Unsupported preimage length: " + length);
                      }
                      try {
                        out.write(length);
                        out.write(value);
                      } catch (IOException e) {
                        throw new UncheckedIOException("Failed to write binary preimage", e);
                      }
                    });
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to open preimages file: " + preimagesPath, e);
      }
    }
  }

  @CommandLine.Command(
      name = "import",
      description = "This command imports preimage data from a flat file to persistent storage",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  static class Import implements Runnable {

    @VisibleForTesting
    @CommandLine.Option(
        names = "--preimage-file-path",
        description = "The flat file from which to load preimage data",
        arity = "1..1")
    Path preimageFilePath = null;

    @VisibleForTesting
    @CommandLine.Option(
        names = "--hex",
        description = "import data from a hex formatted, line delimited input file",
        arity = "1..1")
    boolean hex = false;

    StorageProvider storageProvider = null;

    StorageProvider getStorageProvider() {
      if (storageProvider == null) {
        storageProvider = createBesuController().getStorageProvider();
      }
      return storageProvider;
    }

    @Override
    public void run() {
      final StorageProvider storageProvider = getStorageProvider();
      var preimageStorage =
          storageProvider.getStorageBySegmentIdentifier(
              KeyValueSegmentIdentifier.HASH_PREIMAGE_STORE);

      var preimagesPath =
          Optional.ofNullable(preimageFilePath).orElseGet(PreimageSubCommand::orDataDir);

      int batchSize = 1_000_000;
      int counter = 0;
      var preimageTx = preimageStorage.startTransaction();

      try {
        if (hex) {
          // HEX MODE: read line-by-line
          try (var lines = Files.lines(preimagesPath)) {
            var filteredLines =
                lines
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList(); // force evaluation while stream is open

            for (String line : filteredLines) {
              if (line.length() > 66) {
                LOG.error("Discarding input with length > 32 byte hex value: {}", line);
                continue;
              }

              Bytes byteVal = Bytes.fromHexString(line);
              preimageTx.put(Hash.hash(byteVal).getBytes().toArrayUnsafe(), byteVal.toArrayUnsafe());

              if (++counter % batchSize == 0) {
                preimageTx.commit();
                preimageTx = preimageStorage.startTransaction();
              }
            }
          }
        } else {
          // BINARY MODE: read [1-byte length][N-byte value] format
          try (var in = Files.newInputStream(preimagesPath)) {
            while (true) {
              int lenByte = in.read();
              if (lenByte == -1) break; // EOF

              if (lenByte != 20 && lenByte != 32) {
                throw new IOException("Invalid preimage length prefix: " + lenByte);
              }

              byte[] value = in.readNBytes(lenByte);
              if (value.length != lenByte) {
                throw new IOException("Incomplete record: expected " + lenByte + " bytes");
              }

              Bytes byteVal = Bytes.wrap(value);
              preimageTx.put(Hash.hash(byteVal).getBytes().toArrayUnsafe(), byteVal.toArrayUnsafe());

              if (++counter % batchSize == 0) {
                preimageTx.commit();
                preimageTx = preimageStorage.startTransaction();
              }
            }
          }
        }

        if (counter % batchSize != 0) {
          preimageTx.commit();
        }

      } catch (IOException e) {
        throw new UncheckedIOException("Failed to load preimages from file: " + preimagesPath, e);
      }
    }
  }
}
