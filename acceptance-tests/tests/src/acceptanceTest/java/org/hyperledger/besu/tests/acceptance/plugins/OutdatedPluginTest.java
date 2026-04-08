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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.ethereum.core.plugins.ImmutablePluginConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginsVerificationMode;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.util.BesuVersionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

public class OutdatedPluginTest extends AcceptanceTestBase {
  private BesuNode pluginNode;

  @Test
  public void warnOnOutdatedPluginByDefault() throws IOException {
    pluginNode =
        besu.createQbftPluginsNode(
            "pluginNode",
            List.of("outdatedTestPlugins"),
            PluginConfiguration.DEFAULT,
            Collections.emptyList(),
            "DEBUG");

    cluster.startConsoleCapture();
    cluster.runNodeStart(pluginNode);

    pluginNode.verify(net.netServicesAllActive());

    assertTrue(
        cluster
            .getConsoleContents()
            .lines()
            .filter(line -> line.contains("WARN"))
            .filter(line -> line.contains("outdatedTestPlugins.jar"))
            .anyMatch(
                line ->
                    line.contains(
                        "is built against Besu version 25.12.0 while current running Besu version is "
                            + BesuVersionUtils.shortVersion())));
  }

  @Test
  public void failsToStartOnOutdatedPluginIfToldSo() throws IOException {
    pluginNode =
        besu.createQbftPluginsNode(
            "pluginNode",
            List.of("outdatedTestPlugins"),
            ImmutablePluginConfiguration.builder()
                .pluginsVerificationMode(PluginsVerificationMode.FULL)
                .build(),
            Collections.emptyList(),
            "DEBUG");

    cluster.startConsoleCapture();
    cluster.runNodeStart(pluginNode);

    final var exitCode = pluginNode.exitCode();

    // exit code != 0 means Besu failed to start
    assertThat(exitCode).isPresent();
    assertThat(exitCode.get()).isNotZero();

    assertTrue(
        cluster
            .getConsoleContents()
            .lines()
            .filter(line -> line.contains("ERROR"))
            .filter(line -> line.contains("outdatedTestPlugins.jar"))
            .anyMatch(
                line ->
                    line.contains(
                        "is built against Besu version 25.12.0 while current running Besu version is "
                            + BesuVersionUtils.shortVersion())));
  }
}
