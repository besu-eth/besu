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
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Two registrants claiming the same name in a keyed registration service. {@code
 * TestDuplicateNamePlugin} registers, on request, the RPC method another test plugin registers or a
 * storage factory with the built-in RocksDB plugin's name; by default the node stops and the error
 * names both registrants, and with {@code --plugin-continue-on-error} the plugin whose registration
 * collided is dropped and the node starts.
 */
public class DuplicateNamePluginTest extends AcceptanceTestBase {
  private static final String DUPLICATE_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.TestDuplicateNamePlugin";
  private static final String START_BEFORE_SERVING_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.TestStartBeforeServingPlugin";
  private static final String ROCKSDB_PLUGIN =
      "org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBPlugin";

  private BesuNode node;

  private BesuNode nodeWith(final String... extraCLIOptions) throws IOException {
    return besu.createPluginsNode("node1", List.of("testPlugins"), List.of(extraCLIOptions));
  }

  private void assertStartupFails(final String... expectedConsoleText) {
    cluster.startConsoleCapture();

    // runNodeStart throws when the process exits before writing its ports file
    final Throwable thrown = catchThrowable(() -> cluster.runNodeStart(node));
    assertThat(thrown).isInstanceOf(IllegalStateException.class);

    // the exit code is set asynchronously via process.onExit(); wait for it
    await().atMost(10, SECONDS).until(() -> node.exitCode().isPresent());
    assertThat(node.exitCode().orElseThrow()).isNotZero();

    assertThat(cluster.getConsoleContents()).contains(expectedConsoleText);
  }

  @Test
  public void twoPluginsRegisteringTheSameRpcMethodStopTheNodeNamingBoth() throws IOException {
    node = nodeWith("--Xplugin-duplicate-name-test=rpc");

    // the duplicate plugin registers first, in class name order, so the other one is rejected
    assertStartupFails(
        "Error registering plugin of type " + START_BEFORE_SERVING_PLUGIN,
        "cannot register RPC method tests_isStarted: plugin "
            + DUPLICATE_PLUGIN
            + " already registered it");
  }

  @Test
  public void aPluginClaimingABuiltInStorageFactoryNameStopsTheNodeNamingBoth() throws IOException {
    node = nodeWith("--Xplugin-duplicate-name-test=rocksdb");

    assertStartupFails(
        "Error registering plugin of type " + DUPLICATE_PLUGIN,
        "cannot register storage factory rocksdb: plugin "
            + ROCKSDB_PLUGIN
            + " already registered it");
  }

  @Test
  public void theCollidingPluginIsDroppedWithContinueOnError() throws IOException {
    node = nodeWith("--Xplugin-duplicate-name-test=rpc", "--plugin-continue-on-error=true");

    cluster.startConsoleCapture();
    cluster.start(node);

    node.verify(net.netServicesAllActive());
    assertThat(cluster.getConsoleContents())
        .contains(
            "Error registering plugin of type " + START_BEFORE_SERVING_PLUGIN,
            "cannot register RPC method tests_isStarted: plugin "
                + DUPLICATE_PLUGIN
                + " already registered it",
            "start and stop will not be called");
  }
}
