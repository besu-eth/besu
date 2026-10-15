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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

public class PluginConfigurationReloadTest extends AcceptanceTestBase {
  private BesuNode pluginNode;

  private void setupNode(final int... failingIds) throws IOException {
    pluginNode =
        besu.createQbftPluginsNode(
            "pluginNode",
            Collections.singletonList("testPlugins"),
            PluginConfiguration.DEFAULT,
            Arrays.stream(failingIds).mapToObj("--plugin-reload-conf%d-fail"::formatted).toList(),
            "PLUGINS");

    cluster.start(pluginNode);

    pluginNode.verify(net.netServicesAllActive());
  }

  @Test
  public void reloadSinglePluginConfigurationSuccessful() throws IOException {
    setupNode();

    final var reloadConfTx =
        plugins.reloadConfiguration(TestReloadConfigurationPlugin1.class.getName());

    final var response = pluginNode.execute(reloadConfTx);

    assertThat(response).isEqualTo("Success");
    assertConfigReloadCalledForPlugins(1);
  }

  @Test
  public void reloadSinglePluginConfigurationBySimpleClassName() throws IOException {
    setupNode();

    final var reloadConfTx = plugins.reloadConfiguration("TestReloadConfigurationPlugin1");

    final var response = pluginNode.execute(reloadConfTx);

    assertThat(response).isEqualTo("Success");
    assertConfigReloadCalledForPlugins(1);
  }

  @Test
  public void reloadSinglePluginConfigurationFailure() throws IOException {
    setupNode(1);

    final var reloadConfTx =
        plugins.reloadConfiguration(TestReloadConfigurationPlugin1.class.getName());

    assertThatThrownBy(() -> pluginNode.execute(reloadConfTx))
        .hasMessage(
            "org.hyperledger.besu.tests.acceptance.plugins.TestReloadConfigurationPlugin1:failure(Failed to reload configuration)");

    assertConfigReloadCalledForPlugins(1);
  }

  @Test
  public void reloadAllPluginConfigurationsSuccessful() throws IOException {
    setupNode();

    final var reloadConfTx = plugins.reloadConfiguration();

    final var response = pluginNode.execute(reloadConfTx);

    assertThat(response).isEqualTo("Success");
    assertConfigReloadCalledForPlugins(1, 2);
  }

  @Test
  public void reloadAllPluginConfigurationsOneFails() throws IOException {
    setupNode(1);

    final var reloadConfTx = plugins.reloadConfiguration();

    assertThatThrownBy(() -> pluginNode.execute(reloadConfTx))
        .hasMessageContainingAll(
            "org.hyperledger.besu.tests.acceptance.plugins.TestReloadConfigurationPlugin1:failure(Failed to reload configuration)",
            "org.hyperledger.besu.tests.acceptance.plugins.TestReloadConfigurationPlugin2:success");

    assertConfigReloadCalledForPlugins(1, 2);
  }

  @Test
  public void reloadAllPluginConfigurationsAllFail() throws IOException {
    setupNode(1, 2);

    final var reloadConfTx = plugins.reloadConfiguration();

    assertThatThrownBy(() -> pluginNode.execute(reloadConfTx))
        .hasMessageContainingAll(
            "org.hyperledger.besu.tests.acceptance.plugins.TestReloadConfigurationPlugin1:failure(Failed to reload configuration)",
            "org.hyperledger.besu.tests.acceptance.plugins.TestReloadConfigurationPlugin2:failure(Failed to reload configuration)");

    assertConfigReloadCalledForPlugins(1, 2);
  }

  private void assertConfigReloadCalledForPlugins(final int... pluginIds) {
    final var reloadConfFiles =
        pluginNode
            .homeDirectory()
            .resolve("plugins")
            .toFile()
            .listFiles((dir, name) -> name.startsWith("reloadConfiguration."));

    assertThat(reloadConfFiles).isNotNull().hasSize(pluginIds.length);

    final var foundFilenames = Arrays.stream(reloadConfFiles).map(File::getName).toList();
    assertThat(foundFilenames)
        .containsExactlyInAnyOrderElementsOf(
            Arrays.stream(pluginIds).mapToObj(i -> "reloadConfiguration." + i).toList());
  }
}
