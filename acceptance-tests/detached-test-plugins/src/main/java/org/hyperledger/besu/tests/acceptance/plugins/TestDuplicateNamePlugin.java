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

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.RpcEndpointRegistry;
import org.hyperledger.besu.plugin.services.StorageService;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.List;

import com.google.auto.service.AutoService;
import picocli.CommandLine.Option;

/**
 * Registers, on request, a name another registrant already holds. {@code rpc} registers the RPC
 * method {@code tests_isStarted}, which {@link TestStartBeforeServingPlugin} registers too and
 * after this plugin, since the plugins of one jar register in class name order. {@code rocksdb}
 * registers a storage factory of that name, the built-in RocksDB plugin's, which registers ahead of
 * every discovered plugin. Off by default, so the other plugin tests, which load this plugin too,
 * are not affected.
 */
@AutoService(BesuPlugin.class)
public class TestDuplicateNamePlugin implements BesuPlugin {

  @Option(names = "--Xplugin-duplicate-name-test", hidden = true)
  String duplicate = "";

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("duplicate-name", this);
  }

  @Override
  public void register(final RegistrationContext context) {
    switch (duplicate) {
      case "rpc" ->
          context
              .getBesuService(RpcEndpointRegistry.class)
              .registerRPCEndpoint("tests", "isStarted", request -> "duplicate");
      case "rocksdb" ->
          context
              .getBesuService(StorageService.class)
              .registerKeyValueStorage(new RocksDbNamedFactory());
      default -> {}
    }
  }

  @Override
  public void start(final StartContext context) {}

  @Override
  public void stop() {}

  /** A factory that only carries the built-in name; it is never asked to create a storage. */
  private static final class RocksDbNamedFactory implements KeyValueStorageFactory {
    @Override
    public String getName() {
      return "rocksdb";
    }

    @Override
    public KeyValueStorage create(
        final SegmentIdentifier segment,
        final BesuConfiguration configuration,
        final MetricsSystem metricsSystem) {
      throw new UnsupportedOperationException("test factory");
    }

    @Override
    public SegmentedKeyValueStorage create(
        final List<SegmentIdentifier> segments,
        final BesuConfiguration configuration,
        final MetricsSystem metricsSystem) {
      throw new UnsupportedOperationException("test factory");
    }

    @Override
    public boolean isSegmentIsolationSupported() {
      return false;
    }

    @Override
    public boolean isSnapshotIsolationSupported() {
      return false;
    }

    @Override
    public void close() {}
  }
}
