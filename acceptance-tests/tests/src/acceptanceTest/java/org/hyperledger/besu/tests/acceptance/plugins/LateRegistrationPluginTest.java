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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Covers the registration window through a real plugin: a registration-phase service kept from
 * register() rejects every registration once the window has closed, and a rejection the plugin does
 * not catch stops the node.
 */
public class LateRegistrationPluginTest extends AcceptanceTestBase {
  private BesuNode node;

  @Test
  public void everyRegistrationServiceRejectsALateRegistration() throws IOException {
    node =
        besu.createPluginsNode(
            "node1", List.of("testPlugins"), List.of("--Xplugin-late-registration=report"));
    cluster.start(node);

    final Path rejected = node.homeDirectory().resolve("plugins/lateRegistration.rejected");
    waitForFile(rejected);
    assertThat(Files.readAllLines(rejected))
        .containsExactlyInAnyOrder(
            "registerKeyValueStorage",
            "register",
            "registerRPCEndpoint",
            "registerPluginTransactionSelectorFactory",
            "registerPluginTransactionValidatorFactory",
            "registerTransactionValidatorRule",
            "registerNodePermissioningProvider",
            "registerTransactionPermissioningProvider",
            "registerNodeMessagePermissioningProvider",
            "registerHealthCheck",
            "unregisterHealthCheck",
            "addMetricCategory");
  }

  @Test
  public void aLateRegistrationStopsTheNode() throws IOException {
    node =
        besu.createPluginsNode(
            "node1",
            List.of("testPlugins"),
            builder ->
                builder
                    .jsonRpcEnabled()
                    .extraCLIOptions(List.of("--Xplugin-late-registration=fatal")));

    cluster.startConsoleCapture();

    // the plugin fails in start(), before the node is up, so runNodeStart throws
    final Throwable thrown = catchThrowable(() -> cluster.runNodeStart(node));
    assertThat(thrown).isInstanceOf(IllegalStateException.class);

    // exit code is set asynchronously via process.onExit(); wait for it
    await().atMost(10, SECONDS).until(() -> node.exitCode().isPresent());
    assertThat(node.exitCode().get()).isNotZero();

    assertThat(cluster.getConsoleContents())
        .contains("TestLateRegistrationPlugin")
        .contains("StorageService.registerKeyValueStorage was called after registration closed");
  }
}
