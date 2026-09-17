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

import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class BundleSelectorPluginTest extends AcceptanceTestBase {
  private BesuNode node;

  @Test
  public void bundleIsMined() throws IOException {
    node =
        besu.createQbftPluginsNode(
            "node",
            Collections.singletonList("testPlugins"),
            PluginConfiguration.DEFAULT,
            List.of(
                "--plugin-bundle-test-enabled=true",
                "--plugin-bundle-size=2",
                "--plugin-block-txs-selection-max-time=75"));
    cluster.start(node);

    assertEventSequence("SELECTED:0", "SELECTED:1");
  }

  @Test
  public void bundleIsNotMinedFirstTxFails() throws IOException {
    node =
        besu.createQbftPluginsNode(
            "node",
            Collections.singletonList("testPlugins"),
            PluginConfiguration.DEFAULT,
            List.of(
                "--plugin-bundle-test-enabled=true",
                "--plugin-bundle-size=2",
                "--plugin-bundle-failing-nonce=0",
                "--plugin-block-txs-selection-max-time=75"));
    cluster.start(node);

    // since the first tx of the bundle fails, the following are not even tried, so we only expect
    // one event
    assertEventSequence("INVALID(failing nonce 0):0");
  }

  @Test
  public void bundleIsNotMinedLastTxFails() throws IOException {
    node =
        besu.createQbftPluginsNode(
            "node",
            Collections.singletonList("testPlugins"),
            PluginConfiguration.DEFAULT,
            List.of(
                "--plugin-bundle-test-enabled=true",
                "--plugin-bundle-size=2",
                "--plugin-bundle-failing-nonce=1",
                "--plugin-block-txs-selection-max-time=75",
                "--poa-block-txs-selection-max-time=95"));
    cluster.start(node);

    // since the last tx of the bundle fails, the first was initially selected, but eventually not
    // selected
    assertEventSequence("SELECTED_ROLLBACK:0", "INVALID(failing nonce 1):1");
  }

  @Test
  public void bundleIsNotMinedMiddleTxFails() throws IOException {
    node =
        besu.createQbftPluginsNode(
            "node",
            Collections.singletonList("testPlugins"),
            PluginConfiguration.DEFAULT,
            List.of(
                "--plugin-bundle-test-enabled=true",
                "--plugin-bundle-size=3",
                "--plugin-bundle-failing-nonce=1",
                "--plugin-block-txs-selection-max-time=75",
                "--poa-block-txs-selection-max-time=95"));
    cluster.start(node);

    // since the last tx of the bundle fails, the first was initially selected, but eventually not
    // selected,
    // and the one after the failing tx is not even tried
    assertEventSequence("SELECTED_ROLLBACK:0", "INVALID(failing nonce 1):1");
  }

  private void assertEventSequence(final String... expectedEvents) throws IOException {
    final Path eventsFile = node.homeDirectory().resolve("plugins/bundle.events");
    waitForFile(eventsFile);
    final var fileContents = Files.readAllLines(eventsFile);

    Assertions.assertThat(fileContents).containsExactly(expectedEvents);
  }
}
