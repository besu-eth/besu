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
package org.hyperledger.besu.tests.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.WaitUtils;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.util.BesuVersionUtils;

import java.io.IOException;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

public class RunVersionTest extends AcceptanceTestBase {

  /** The built-in plugins and the jar each ships in, in the order Besu adds them. */
  private static final List<String> BUILT_IN_PLUGINS =
      List.of(
          "org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBPlugin/besu-plugins-rocksdb",
          "org.hyperledger.besu.services.kvstore.InMemoryStoragePlugin/besu-services-kvstore",
          "org.hyperledger.besu.plugin.services.health.LivenessCheckPlugin/besu-plugins-health",
          "org.hyperledger.besu.plugin.services.health.ReadinessCheckPlugin/besu-plugins-health");

  @Test
  public void testShowsVersionAndExits() throws IOException {
    final BesuNode node =
        besu.createPluginsNode(
            "run --version",
            List.of("testPlugins"),
            besuNodeConfigurationBuilder -> besuNodeConfigurationBuilder.run("--version"));
    cluster.startConsoleCapture();
    cluster.runNodeStart(node);

    final var serviceLoader = ServiceLoader.load(BesuPlugin.class);
    final var pluginIds =
        serviceLoader.stream().map(provider -> provider.type().getName()).toList();

    // the built-in plugins define their options first, so they are listed first
    final var versionOutput =
        BesuVersionUtils.version()
            + System.lineSeparator()
            + Stream.concat(
                    BUILT_IN_PLUGINS.stream(), pluginIds.stream().map(id -> id + "/testPlugins"))
                .map(line -> line + "/" + BesuVersionUtils.shortVersion())
                .collect(Collectors.joining(System.lineSeparator()));

    WaitUtils.waitFor(5000, () -> node.verify(exitedSuccessfully));

    final String consoleContents = cluster.getConsoleContents();
    assertThat(consoleContents).startsWith(versionOutput);
  }
}
