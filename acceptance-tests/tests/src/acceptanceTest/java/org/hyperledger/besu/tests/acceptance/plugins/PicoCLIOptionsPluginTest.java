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
package org.hyperledger.besu.tests.acceptance.plugins;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class PicoCLIOptionsPluginTest extends AcceptanceTestBase {
  private BesuNode node;

  // context: https://en.wikipedia.org/wiki/The_Magic_Words_are_Squeamish_Ossifrage
  private static final String MAGIC_WORDS = "Squemish Ossifrage";

  @BeforeEach
  public void setUp() throws Exception {
    node =
        besu.createPluginsNode(
            "node1",
            Collections.singletonList("testPlugins"),
            Collections.singletonList("--Xplugin-test-option=" + MAGIC_WORDS));
    cluster.start(node);
  }

  @Test
  public void shouldRegister() throws IOException {
    final Path registrationFile =
        node.homeDirectory().resolve("plugins/pluginLifecycle.registered");
    waitForFile(registrationFile);

    // this assert is true as CLI will be parsed at this point
    assertThat(Files.readAllLines(registrationFile).stream().anyMatch(s -> s.contains(MAGIC_WORDS)))
        .isTrue();
  }

  @Test
  public void shouldStart() throws IOException {
    final Path registrationFile = node.homeDirectory().resolve("plugins/pluginLifecycle.started");
    waitForFile(registrationFile);

    // this assert is true as CLI will be parsed at this point
    assertThat(Files.readAllLines(registrationFile).stream().anyMatch(s -> s.contains(MAGIC_WORDS)))
        .isTrue();
  }

  @Test
  @Disabled("No way to do a graceful shutdown of Besu at the moment.")
  public void shouldStop() {
    cluster.stopNode(node);
    waitForFile(node.homeDirectory().resolve("plugins/pluginLifecycle.stopped"));
  }
}
