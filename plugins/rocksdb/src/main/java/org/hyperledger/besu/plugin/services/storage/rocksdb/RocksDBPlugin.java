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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.StorageService;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The RocksDb plugin. */
public class RocksDBPlugin implements BesuPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBPlugin.class);
  private static final String NAME = "rocksdb";

  private final RocksDBCLIOptions options;
  private final List<SegmentIdentifier> ignorableSegments = new ArrayList<>();
  private RocksDBKeyValueStorageFactory factory;

  /** Instantiates a newRocksDb plugin. */
  public RocksDBPlugin() {
    this.options = RocksDBCLIOptions.create();
  }

  /**
   * Add ignorable segment identifier.
   *
   * @param ignorable the ignorable
   */
  public void addIgnorableSegmentIdentifier(final SegmentIdentifier ignorable) {
    ignorableSegments.add(ignorable);
  }

  @Override
  public void defineOptions(final PicoCLIOptions cmdlineOptions) {
    cmdlineOptions.addPicoCLIOptions(NAME, options);
  }

  @Override
  public void register(final RegistrationContext context) {
    LOG.debug("Registering plugin");
    createAndRegister(context.getBesuService(StorageService.class));
    LOG.debug("Plugin registered.");
  }

  @Override
  public void start(final StartContext context) {
    LOG.debug("Starting plugin.");
    if (LOG.isTraceEnabled()) {
      LOG.trace("Applied configuration: {}", options.toString());
    }
  }

  @Override
  public void stop() {
    LOG.debug("Stopping plugin.");

    try {
      if (factory != null) {
        factory.close();
        factory = null;
      }
    } catch (final IOException e) {
      LOG.error("Failed to stop plugin: {}", e.getMessage(), e);
    }
  }

  /** Resets the factory for Ephemery automatic restart. */
  public void reset() {
    if (factory != null) {
      factory.reset();
    }
  }

  /**
   * Is high spec enabled.
   *
   * @return the boolean
   */
  public boolean isHighSpecEnabled() {
    return options.isHighSpec();
  }

  /**
   * Gets blob db settings.
   *
   * @return the blob db settings
   */
  public RocksDBCLIOptions.BlobDBSettings getBlobDBSettings() {
    return options.getBlobDBSettings();
  }

  /**
   * Returns the max open files value that will be used, either explicitly set or derived from
   * available memory.
   *
   * @return the resolved max open files value
   */
  public int getResolvedMaxOpenFiles() {
    return options.getResolvedMaxOpenFiles();
  }

  /**
   * Returns whether max open files was explicitly set via CLI.
   *
   * @return true if max open files was set via CLI, false if derived from available memory
   */
  public boolean isMaxOpenFilesExplicitlySet() {
    return options.isMaxOpenFilesExplicitlySet();
  }

  private void createAndRegister(final StorageService service) {
    final List<SegmentIdentifier> segments = service.getAllSegmentIdentifiers();

    final RocksDBFactoryConfiguration configuration = options.toDomainObject();
    factory =
        new RocksDBKeyValueStorageFactory(
            () -> configuration,
            segments,
            ignorableSegments,
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    service.registerKeyValueStorage(factory);
  }
}
