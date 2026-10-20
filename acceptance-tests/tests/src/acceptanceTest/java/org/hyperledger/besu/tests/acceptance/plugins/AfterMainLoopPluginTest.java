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

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@code afterMainLoop()} runs once the main loop has started: the test plugin records from inside
 * the hook whether the synchronizer's start had already delivered the initial-sync-completion event
 * it subscribed to in {@code start()}.
 */
public class AfterMainLoopPluginTest extends AcceptanceTestBase {
  private BesuNode node;

  @BeforeEach
  public void setUp() throws Exception {
    node = besu.createPluginsNode("node1", List.of("testPlugins"), List.of());
    cluster.start(node);
  }

  @Test
  public void afterMainLoopRunsAfterTheSynchronizerHasStarted() throws IOException {
    final Path callbackFile = node.homeDirectory().resolve("plugins/afterMainLoop");
    waitForFile(callbackFile);

    // a full-sync node delivers the completion event from DefaultSynchronizer.start(), which only
    // runs with the main loop, so the value is false if the hook ran before it
    assertThat(Files.readAllLines(callbackFile))
        .contains("started=true", "p2pEnabled=true", "initialSyncCompletionSeen=true")
        .anyMatch(line -> line.startsWith("peerCount="));
  }
}
