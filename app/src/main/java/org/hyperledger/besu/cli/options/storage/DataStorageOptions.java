/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.cli.options.storage;

import static org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration.DEFAULT_RECEIPT_COMPACTION_ENABLED;

import org.hyperledger.besu.cli.options.CLIOptions;
import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/** The Data storage CLI options. */
public class DataStorageOptions implements CLIOptions<DataStorageConfiguration> {

  private static final String DATA_STORAGE_FORMAT = "--data-storage-format";

  // Use Bonsai DB
  @Option(
      names = {DATA_STORAGE_FORMAT},
      description =
          "Format to store trie data in.  Either FOREST or BONSAI (default: ${DEFAULT-VALUE}).")
  private DataStorageFormat dataStorageFormat = DataStorageFormat.BONSAI;

  @Option(
      names = "--receipt-compaction-enabled",
      description = "Enables compact storing of receipts (default: ${DEFAULT-VALUE})",
      fallbackValue = "true")
  private Boolean receiptCompactionEnabled = DEFAULT_RECEIPT_COMPACTION_ENABLED;

  @CommandLine.Option(
      names = {"--history-expiry-prune"},
      description =
          "Convenience option to enable online history pruning and configure BlobDB garbage collection settings (default: ${DEFAULT-VALUE}). \"--history-expiry-prune\" is deprecated and will be removed in a future release.")
  private Boolean historyExpiryPrune = false;

  /**
   * Options specific to path-based storage modes. Holds the necessary parameters to configure
   * path-based storage, such as the Bonsai mode or Verkle in the future.
   */
  @Mixin
  private PathBasedExtraStorageOptions pathBasedExtraStorageOptions =
      PathBasedExtraStorageOptions.create();

  /** Default Constructor. */
  DataStorageOptions() {}

  /**
   * Create data storage options.
   *
   * @return the data storage options
   */
  public static DataStorageOptions create() {
    return new DataStorageOptions();
  }

  /**
   * Validates the data storage options
   *
   * @param commandLine the full commandLine to check all the options specified by the user
   */
  public void validate(final CommandLine commandLine) {
    pathBasedExtraStorageOptions.validate(commandLine, dataStorageFormat);
  }

  /**
   * Converts to options from the configuration
   *
   * @param domainObject to be reversed
   * @return the options that correspond to the configuration
   */
  public static DataStorageOptions fromConfig(final DataStorageConfiguration domainObject) {
    final DataStorageOptions dataStorageOptions = DataStorageOptions.create();
    dataStorageOptions.dataStorageFormat = domainObject.getDataStorageFormat();
    dataStorageOptions.receiptCompactionEnabled = domainObject.getReceiptCompactionEnabled();
    dataStorageOptions.pathBasedExtraStorageOptions =
        PathBasedExtraStorageOptions.fromConfig(
            domainObject.getPathBasedExtraStorageConfiguration());
    dataStorageOptions.historyExpiryPrune = domainObject.getHistoryExpiryPruneEnabled();
    return dataStorageOptions;
  }

  @Override
  public DataStorageConfiguration toDomainObject() {
    final ImmutableDataStorageConfiguration.Builder builder =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(dataStorageFormat)
            .receiptCompactionEnabled(receiptCompactionEnabled)
            .historyExpiryPruneEnabled(historyExpiryPrune)
            .pathBasedExtraStorageConfiguration(pathBasedExtraStorageOptions.toDomainObject());
    return builder.build();
  }

  @Override
  public List<String> getCLIOptions() {
    final List<String> cliOptions = CommandLineUtils.getCLIOptions(this, new DataStorageOptions());
    cliOptions.addAll(pathBasedExtraStorageOptions.getCLIOptions());
    return cliOptions;
  }

  /**
   * Normalize data storage format string.
   *
   * @return the normalized string
   */
  public String normalizeDataStorageFormat() {
    return StringUtils.capitalize(dataStorageFormat.toString().toLowerCase(Locale.ROOT));
  }
}
