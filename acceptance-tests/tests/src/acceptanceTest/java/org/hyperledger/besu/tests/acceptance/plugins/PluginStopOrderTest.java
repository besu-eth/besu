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
 * How Besu stops the plugins of a node, read from the stop-order plugins' signal files: in the
 * reverse of their start order on a shutdown, the started ones when a later plugin fails to start,
 * and the remaining ones when one plugin's {@code stop()} throws an {@link Error}.
 */
public class PluginStopOrderTest extends AcceptanceTestBase {
  private static final String PLUGIN_B =
      "org.hyperledger.besu.tests.acceptance.plugins.TestStopOrderPluginB";

  private BesuNode node;

  /** A node running only the two stop-order plugins. */
  private BesuNode nodeWith(final String... extraCLIOptions) throws IOException {
    return besu.createPluginsNode(
        "node1",
        List.of("testPlugins"),
        builder ->
            builder
                .jsonRpcEnabled()
                .jsonRpcAdmin()
                .requestedPlugins(List.of("TestStopOrderPluginA", "TestStopOrderPluginB"))
                .extraCLIOptions(List.of(extraCLIOptions)));
  }

  private Path signal(final String name) {
    return node.homeDirectory().resolve("plugins").resolve(name);
  }

  private List<String> stopOrder() throws IOException {
    return Files.readAllLines(signal("stopOrder"));
  }

  @Test
  public void pluginsStopInTheReverseOfTheirStartOrder() throws IOException {
    node = nodeWith();
    cluster.start(node);
    waitForFile(signal("stopOrder.A.started"));
    waitForFile(signal("stopOrder.B.started"));

    // the process runner sends SIGTERM and waits for the process to exit
    cluster.stopNode(node);

    assertThat(stopOrder()).containsExactly("B", "A");
  }

  @Test
  public void pluginsStartedBeforeAFailingStartAreStopped() throws IOException {
    node = nodeWith("--Xplugin-stop-order-b-test=failStart");
    cluster.startConsoleCapture();

    // runNodeStart throws when the process exits before writing its ports file
    final Throwable thrown = catchThrowable(() -> cluster.runNodeStart(node));
    assertThat(thrown).isInstanceOf(IllegalStateException.class);
    await().atMost(10, SECONDS).until(() -> node.exitCode().isPresent());
    assertThat(node.exitCode().orElseThrow()).isNotZero();

    assertThat(cluster.getConsoleContents())
        .contains("Error starting plugin of type " + PLUGIN_B, "I was told to fail at start");
    assertThat(signal("stopOrder.A.started")).exists();
    assertThat(signal("stopOrder.B.started")).doesNotExist();
    assertThat(stopOrder()).containsExactly("A");
  }

  @Test
  public void anErrorFromOneStopDoesNotSkipTheRemainingPlugins() throws IOException {
    node = nodeWith("--Xplugin-stop-order-b-test=errorOnStop");
    cluster.startConsoleCapture();
    cluster.start(node);
    waitForFile(signal("stopOrder.A.started"));
    waitForFile(signal("stopOrder.B.started"));

    cluster.stopNode(node);

    // B stops first and throws before writing its letter; A, stopped after it, still writes its own
    assertThat(stopOrder()).containsExactly("A");
    assertThat(cluster.getConsoleContents())
        .contains("Error stopping plugin of type " + PLUGIN_B, "NoClassDefFoundError");
  }
}
