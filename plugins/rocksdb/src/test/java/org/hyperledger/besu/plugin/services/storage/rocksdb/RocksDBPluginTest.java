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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.StorageService;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.storage.StorageConfiguration;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

class RocksDBPluginTest {

  /** A plugin whose options are bound from the given command line. */
  private static RocksDBPlugin pluginWithOptions(final String... args) {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptions picoCLIOptions = commandLine::addMixin;
    final RocksDBPlugin plugin = new RocksDBPlugin();
    plugin.defineOptions(picoCLIOptions);
    commandLine.parseArgs(args);
    return plugin;
  }

  private static RegistrationContext registrationContext(final StorageService storageService) {
    final StorageConfiguration storageConfiguration = mock(StorageConfiguration.class);
    when(storageConfiguration.getIgnorableSegments()).thenReturn(List.of());
    final RegistrationContext context = mock(RegistrationContext.class);
    when(context.getBesuService(StorageService.class)).thenReturn(storageService);
    when(context.getBesuService(StorageConfiguration.class)).thenReturn(storageConfiguration);
    return context;
  }

  @Test
  void overviewReportsTheDerivedMaxOpenFilesAndNothingElseByDefault() {
    final List<String> lines = pluginWithOptions().configurationOverviewLines();

    assertThat(lines).hasSize(1);
    assertThat(lines.getFirst())
        .startsWith("RocksDB max open files: ")
        .endsWith(" (derived from available resources)");
  }

  @Test
  void overviewReportsTheConfiguredSettings() {
    final RocksDBPlugin plugin =
        pluginWithOptions(
            "--Xplugin-rocksdb-max-open-files=2048",
            "--Xplugin-rocksdb-high-spec-enabled=true",
            "--Xplugin-rocksdb-blockchain-blob-garbage-collection-enabled=true",
            "--Xplugin-rocksdb-blob-garbage-collection-age-cutoff=0.5",
            "--Xplugin-rocksdb-blob-garbage-collection-force-threshold=0.75");

    assertThat(plugin.configurationOverviewLines())
        .containsExactly(
            "RocksDB max open files: 2048 (set)",
            "Experimental high spec configuration enabled",
            "Experimental BlobDB BLOCKCHAIN Garbage Collection enabled",
            "Experimental BlobDB GC age cutoff: 0.5; force threshold: 0.75");
  }

  @Test
  void stopLetsGoOfTheFactoryAndRegisterCreatesAFreshOne() {
    final StorageService storageService = mock(StorageService.class);
    when(storageService.getAllSegmentIdentifiers()).thenReturn(List.of());
    final RegistrationContext context = registrationContext(storageService);
    final RocksDBPlugin plugin = pluginWithOptions();

    plugin.register(context);
    plugin.stop();
    plugin.register(context);

    final ArgumentCaptor<KeyValueStorageFactory> factories =
        ArgumentCaptor.forClass(KeyValueStorageFactory.class);
    verify(storageService, times(2)).registerKeyValueStorage(factories.capture());
    assertThat(factories.getAllValues()).hasSize(2);
    assertThat(factories.getAllValues().get(0)).isNotSameAs(factories.getAllValues().get(1));
    assertThat(factories.getAllValues())
        .allSatisfy(factory -> assertThat(factory.getName()).isEqualTo("rocksdb"));
  }
}
