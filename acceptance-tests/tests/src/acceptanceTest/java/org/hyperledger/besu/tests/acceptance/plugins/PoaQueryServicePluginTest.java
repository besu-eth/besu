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

import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@code PoaQueryService} and {@code BftQueryService} are registered on BFT networks
 * and answer consistently, via the TestPoaQueryPlugin which writes what the services report to a
 * callback file. Covers both mechanisms because they register differently: QBFT registers one
 * instance under both interfaces, IBFT registers two distinct objects.
 */
public class PoaQueryServicePluginTest extends AcceptanceTestBase {

  // besu.plugins.dir is JVM-global; clear it so each test's node gets its own plugins dir
  @AfterEach
  public void clearPluginsDirProperty() {
    System.clearProperty("besu.plugins.dir");
  }

  @Test
  public void qbftNodeExposesConsensusQueryServices() throws Exception {
    final BesuNode node =
        besu.createQbftPluginsNode(
            "qbftnode",
            Collections.singletonList("testPlugins"),
            PluginConfiguration.DEFAULT,
            List.of());
    cluster.start(node);

    final Map<String, String> report = readReport(node);
    assertQueryServiceReport(report, "qbft");
    assertThat(report.get("sameInstance")).isEqualTo("true");
  }

  @Test
  public void ibftNodeExposesConsensusQueryServices() throws Exception {
    final BesuNode node =
        besu.createIbft2PluginsNode(
            "ibftnode",
            Collections.singletonList("testPlugins"),
            PluginConfiguration.DEFAULT,
            List.of());
    cluster.start(node);

    final Map<String, String> report = readReport(node);
    assertQueryServiceReport(report, "ibft");
    assertThat(report.get("sameInstance")).isEqualTo("false");
  }

  private void assertQueryServiceReport(
      final Map<String, String> report, final String expectedMechanism) {
    assertThat(report.get("mechanism")).isEqualTo(expectedMechanism);

    // single-validator network: the node is the sole validator, proposer and signer
    final String localSigner = report.get("localSigner");
    assertThat(localSigner).isNotBlank();
    assertThat(report.get("validators").split(",")).contains(localSigner);
    assertThat(report.get("proposer")).isEqualTo(localSigner);
    assertThat(report.get("round")).isEqualTo("0");
    assertThat(report.get("signers").split(",")).contains(localSigner);
  }

  private Map<String, String> readReport(final BesuNode node) throws IOException {
    final Path reportFile = node.homeDirectory().resolve("plugins/poaQueryService.report");
    waitForFile(reportFile);
    final Map<String, String> report = new HashMap<>();
    for (final String line : Files.readAllLines(reportFile)) {
      final int separator = line.indexOf('=');
      report.put(line.substring(0, separator), line.substring(separator + 1));
    }
    return report;
  }
}
