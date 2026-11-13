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
import org.hyperledger.besu.plugin.storage.StorageConfiguration;

import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The RocksDb plugin. */
public class RocksDBPlugin implements BesuPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBPlugin.class);
  private static final String NAME = "rocksdb";

  private final RocksDBCLIOptions options;
  private @Nullable RocksDBKeyValueStorageFactory factory;

  /** Instantiates a newRocksDb plugin. */
  public RocksDBPlugin() {
    this.options = RocksDBCLIOptions.create();
  }

  @Override
  public void defineOptions(final PicoCLIOptions cmdlineOptions) {
    cmdlineOptions.addPicoCLIOptions(NAME, options);
  }

  @Override
  public void register(final RegistrationContext context) {
    LOG.debug("Registering plugin");
    createAndRegister(
        context.getBesuService(StorageService.class),
        context.getBesuService(StorageConfiguration.class).getIgnorableSegments());
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
    // Besu closes the storages it opened through the factory, after the plugins have stopped; the
    // plugin only lets go of the factory, so that register() creates a fresh one on a restart
    factory = null;
  }

  @Override
  public List<String> configurationOverviewLines() {
    final List<String> lines = new ArrayList<>();
    lines.add(
        "RocksDB max open files: "
            + options.getResolvedMaxOpenFiles()
            + (options.isMaxOpenFilesExplicitlySet()
                ? " (set)"
                : " (derived from available resources)"));
    if (options.isHighSpec()) {
      lines.add("Experimental high spec configuration enabled");
    }
    final RocksDBCLIOptions.BlobDBSettings blobDBSettings = options.getBlobDBSettings();
    if (blobDBSettings.isBlockchainGarbageCollectionEnabled()) {
      lines.add("Experimental BlobDB BLOCKCHAIN Garbage Collection enabled");
    }
    if (blobDBSettings.blobGarbageCollectionAgeCutoff().isPresent()
        || blobDBSettings.blobGarbageCollectionForceThreshold().isPresent()) {
      final StringBuilder blobDBString = new StringBuilder("Experimental BlobDB GC ");
      blobDBSettings
          .blobGarbageCollectionAgeCutoff()
          .ifPresent(cutoff -> blobDBString.append("age cutoff: ").append(cutoff).append("; "));
      blobDBSettings
          .blobGarbageCollectionForceThreshold()
          .ifPresent(threshold -> blobDBString.append("force threshold: ").append(threshold));
      lines.add(blobDBString.toString());
    }
    return lines;
  }

  private void createAndRegister(
      final StorageService service, final List<SegmentIdentifier> ignorableSegments) {
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
