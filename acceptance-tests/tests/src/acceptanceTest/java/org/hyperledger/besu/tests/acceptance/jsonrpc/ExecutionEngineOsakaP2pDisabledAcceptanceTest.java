/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.tests.acceptance.jsonrpc;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Replays the existing Osaka engine API test cases against a node started with {@code
 * --p2p-enabled=false}, to simulate the environment of benchmark and test harnesses (e.g. hive,
 * benchmarkoor) that drive Besu standalone over the engine API with no peers.
 *
 * <p>Sync mode defaults to FULL — matching how those harnesses run Besu — so this also exercises
 * the FULL-sync + p2p-disabled path that the controller-level fix targets.
 */
public class ExecutionEngineOsakaP2pDisabledAcceptanceTest extends AbstractJsonRpcTest {
  private static final String GENESIS_FILE = "/jsonrpc/engine/osaka/genesis.json";
  private static final String TEST_CASE_PATH = "/jsonrpc/engine/osaka/test-cases/";

  private static JsonRpcTestsContext testsContext;

  public ExecutionEngineOsakaP2pDisabledAcceptanceTest() {
    super(testsContext);
  }

  @BeforeAll
  public static void init() throws IOException {
    testsContext = new JsonRpcTestsContext(GENESIS_FILE, false);
  }

  public static Stream<Arguments> testCases() throws URISyntaxException {
    return testCasesFromPath(TEST_CASE_PATH);
  }

  @AfterAll
  public static void tearDown() {
    testsContext.cluster.close();
  }
}
