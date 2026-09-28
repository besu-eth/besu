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

import java.nio.file.Path;
import java.util.Collections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class BadCLIOptionsPluginTest extends AcceptanceTestBase {
  private BesuNode node;

  @BeforeEach
  public void setUp() throws Exception {
    System.setProperty("TEST_BAD_CLI", "true");

    node =
        besu.createPluginsNode(
            "node1", Collections.singletonList("testPlugins"), Collections.emptyList());
    cluster.start(node);
  }

  @AfterEach
  public void tearDown() {
    System.setProperty("TEST_BAD_CLI", "false");
  }

  @Test
  @DisabledOnOs(OS.MAC)
  public void shouldNotRegister() {
    // depend on the good PicoCLIOptions plugin to tell us when registration is over
    final Path registrationFile =
        node.homeDirectory().resolve("plugins/pluginLifecycle.registered");
    waitForFile(registrationFile);
    assertThat(node.homeDirectory().resolve("plugins/badCliOptions.register")).doesNotExist();
  }

  @Test
  @DisabledOnOs(OS.MAC)
  public void shouldNotStart() {
    // depend on the good PicoCLIOptions to tell us when it should be up
    final Path registrationFile = node.homeDirectory().resolve("plugins/pluginLifecycle.started");
    waitForFile(registrationFile);

    assertThat(node.homeDirectory().resolve("plugins/badCliOptions.start")).doesNotExist();
  }

  @Test
  @Disabled("No way to do a graceful shutdown of Besu at the moment.")
  public void shouldNotStop() {
    cluster.stopNode(node);
    waitForFile(node.homeDirectory().resolve("plugins/pluginLifecycle.stopped"));
    assertThat(node.homeDirectory().resolve("plugins/badCliOptions.start")).doesNotExist();
    assertThat(node.homeDirectory().resolve("plugins/badCliOptions.stop")).doesNotExist();
  }
}
