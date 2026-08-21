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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.cli.subcommands.storage.RocksDbHelper.formatOutputSize;
import static org.hyperledger.besu.controller.BesuController.DATABASE_PATH;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_LOG_STORAGE;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogPruner;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/** The Trie Log subcommand. */
@Command(
    name = "trie-log",
    aliases = "x-trie-log",
    description = "Manipulate trie logs",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    subcommands = {
      TrieLogSubCommand.CountTrieLog.class,
      TrieLogSubCommand.PruneTrieLog.class,
      TrieLogSubCommand.ExportTrieLog.class,
      TrieLogSubCommand.ImportTrieLog.class
    })
public class TrieLogSubCommand implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(TrieLogSubCommand.class);

  @SuppressWarnings("UnusedVariable")
  @ParentCommand
  private static StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec; // Picocli injects reference to command spec

  /** Default Constructor. */
  TrieLogSubCommand() {}

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

  @Command(
      name = "count",
      description = "This command counts all the trie logs",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  static class CountTrieLog implements Runnable {

    @SuppressWarnings("unused")
    @ParentCommand
    private TrieLogSubCommand parentCommand;

    @SuppressWarnings("unused")
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec; // Picocli injects reference to command spec

    @Override
    public void run() {
      final TrieLogContext context = getTrieLogContext();

      final PrintWriter out = spec.commandLine().getOut();

      out.println("Counting trie logs...");
      final TrieLogHelper trieLogHelper = new TrieLogHelper();
      trieLogHelper.printCount(
          out,
          trieLogHelper.getCount(
              context.rootWorldStateStorage, Integer.MAX_VALUE, context.blockchain));
    }
  }

  @Command(
      name = "prune",
      description =
          "This command prunes all trie log layers below the retention limit, including orphaned trie logs.",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  static class PruneTrieLog implements Runnable {

    @SuppressWarnings("unused")
    @ParentCommand
    private TrieLogSubCommand parentCommand;

    @SuppressWarnings("unused")
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec; // Picocli injects reference to command spec

    @Override
    public void run() {
      final TrieLogContext context = getTrieLogContext();
      final Path dataDirectoryPath =
          Paths.get(
              TrieLogSubCommand.parentCommand.besuCommand.dataDir().toAbsolutePath().toString());

      LOG.info("Estimating trie logs size before pruning...");
      long sizeBefore = estimatedSizeOfTrieLogs();
      LOG.info("Estimated trie logs size before pruning: {}", formatOutputSize(sizeBefore));
      LOG.info("Starting pruning...");
      final TrieLogHelper trieLogHelper = new TrieLogHelper();
      boolean success =
          trieLogHelper.prune(
              context.config(),
              context.rootWorldStateStorage(),
              context.blockchain(),
              dataDirectoryPath);

      if (success) {
        LOG.info("Finished pruning. Re-estimating trie logs size...");
        final long sizeAfter = estimatedSizeOfTrieLogs();
        LOG.info(
            "Estimated trie logs size after pruning: {} (0 B estimate is normal when using default settings)",
            formatOutputSize(sizeAfter));
        long estimatedSaving = sizeBefore - sizeAfter;
        LOG.info(
            "Prune ran successfully. We estimate you freed up {}! \uD83D\uDE80",
            formatOutputSize(estimatedSaving));
        spec.commandLine()
            .getOut()
            .printf(
                "Prune ran successfully. We estimate you freed up %s! \uD83D\uDE80\n",
                formatOutputSize(estimatedSaving));
      }
    }

    private long estimatedSizeOfTrieLogs() {
      final String dbPath =
          TrieLogSubCommand.parentCommand
              .besuCommand
              .dataDir()
              .toString()
              .concat("/")
              .concat(DATABASE_PATH);

      AtomicLong estimatedSaving = new AtomicLong(0L);
      try {
        RocksDbHelper.forEachColumnFamily(
            dbPath,
            (rocksdb, cfHandle) -> {
              try {
                if (Arrays.equals(cfHandle.getName(), TRIE_LOG_STORAGE.getId())) {

                  final long sstSize =
                      Long.parseLong(rocksdb.getProperty(cfHandle, "rocksdb.total-sst-files-size"));
                  final long blobSize =
                      Long.parseLong(rocksdb.getProperty(cfHandle, "rocksdb.total-blob-file-size"));

                  estimatedSaving.set(sstSize + blobSize);
                }
              } catch (RocksDBException | NumberFormatException e) {
                throw new RuntimeException(e);
              }
            });
      } catch (Exception e) {
        LOG.warn("Error while estimating trie log size, returning 0 for estimate", e);
        return 0L;
      }

      return estimatedSaving.get();
    }
  }

  @Command(
      name = "export",
      description = "This command exports the trie log of a determined block to a binary file",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  static class ExportTrieLog implements Runnable {

    @SuppressWarnings("unused")
    @ParentCommand
    private TrieLogSubCommand parentCommand;

    @SuppressWarnings("unused")
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec; // Picocli injects reference to command spec

    @SuppressWarnings("unused")
    @CommandLine.Option(
        names = "--trie-log-block-hash",
        description =
            "Comma separated list of hashes from the blocks you want to export the trie logs of",
        split = " {0,1}, {0,1}",
        arity = "1..*")
    private List<String> trieLogBlockHashList;

    @CommandLine.Option(
        names = "--trie-log-file-path",
        description = "The file you want to export the trie logs to",
        arity = "1..1")
    private Path trieLogFilePath = null;

    @Override
    public void run() {
      if (trieLogFilePath == null) {
        trieLogFilePath =
            Paths.get(
                TrieLogSubCommand.parentCommand
                    .besuCommand
                    .dataDir()
                    .resolve("trie-logs.bin")
                    .toAbsolutePath()
                    .toString());
      }

      final TrieLogContext context = getTrieLogContext();

      final List<Hash> listOfBlockHashes =
          trieLogBlockHashList.stream().map(Hash::fromHexString).toList();

      final TrieLogHelper trieLogHelper = new TrieLogHelper();

      try {
        trieLogHelper.exportTrieLog(
            context.rootWorldStateStorage(), listOfBlockHashes, trieLogFilePath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Command(
      name = "import",
      description = "This command imports a trie log exported by another besu node",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  static class ImportTrieLog implements Runnable {

    @SuppressWarnings("unused")
    @ParentCommand
    private TrieLogSubCommand parentCommand;

    @SuppressWarnings("unused")
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec; // Picocli injects reference to command spec

    @CommandLine.Option(
        names = "--trie-log-file-path",
        description = "The file you want to import the trie logs from",
        arity = "1..1")
    private Path trieLogFilePath = null;

    @Override
    public void run() {
      if (trieLogFilePath == null) {
        trieLogFilePath =
            Paths.get(
                TrieLogSubCommand.parentCommand
                    .besuCommand
                    .dataDir()
                    .resolve("trie-logs.bin")
                    .toAbsolutePath()
                    .toString());
      }

      TrieLogContext context = getTrieLogContext();
      final TrieLogHelper trieLogHelper = new TrieLogHelper();
      trieLogHelper.importTrieLog(context.rootWorldStateStorage(), trieLogFilePath);
    }
  }

  record TrieLogContext(
      DataStorageConfiguration config,
      BonsaiWorldStateKeyValueStorage rootWorldStateStorage,
      MutableBlockchain blockchain) {}

  private static TrieLogContext getTrieLogContext() {
    Configurator.setLevel(LoggerFactory.getLogger(TrieLogPruner.class).getName(), Level.DEBUG);
    checkNotNull(parentCommand);
    BesuController besuController = createBesuController();
    final DataStorageConfiguration config = besuController.getDataStorageConfiguration();
    checkArgument(
        config.getDataStorageFormat().isBonsaiFormat(),
        "Subcommand only works with data-storage-format=BONSAI or X_BONSAI_ARCHIVE");

    final StorageProvider storageProvider = besuController.getStorageProvider();
    final BonsaiWorldStateKeyValueStorage rootWorldStateStorage =
        (BonsaiWorldStateKeyValueStorage) storageProvider.createWorldStateStorage(config);
    final MutableBlockchain blockchain = besuController.getProtocolContext().getBlockchain();
    return new TrieLogContext(config, rootWorldStateStorage, blockchain);
  }
}
