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
package org.hyperledger.besu.tests.acceptance.ethereum;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Acceptance test for the {@code targetGasLimit} payload attribute on {@code
 * engine_forkchoiceUpdatedV4}.
 *
 * <p>From Amsterdam onwards the consensus layer must supply {@code targetGasLimit} when requesting
 * a payload; a payload-building fcU that omits it is rejected with {@code
 * INVALID_TARGET_GAS_LIMIT_PARAMS} (-32602).
 */
public class EngineForkchoiceUpdatedV4TargetGasLimitAcceptanceTest extends AcceptanceTestBase {
  private static final String GENESIS_FILE = "/dev/dev_amsterdam.json";
  private static final int INVALID_TARGET_GAS_LIMIT_PARAMS_CODE = -32602;

  private BesuNode besuNode;
  private AmsterdamAcceptanceTestHelper testHelper;

  @BeforeEach
  void setUp() throws IOException {
    besuNode = besu.createExecutionEngineGenesisNode("besuNode", GENESIS_FILE);
    cluster.start(besuNode);
    testHelper = new AmsterdamAcceptanceTestHelper(besuNode, ethTransactions);
  }

  @AfterEach
  void tearDown() {
    besuNode.close();
  }

  @Test
  public void forkchoiceUpdatedV4WithoutTargetGasLimitIsRejected() throws IOException {
    final JsonNode response = testHelper.forkChoiceUpdatedWithoutTargetGasLimit();

    assertThat(response.get("result"))
        .withFailMessage("Expected an error response but got a result: %s", response)
        .isNull();

    final JsonNode error = response.get("error");
    assertThat(error).withFailMessage("Missing error object in response: %s", response).isNotNull();
    assertThat(error.get("code").asInt()).isEqualTo(INVALID_TARGET_GAS_LIMIT_PARAMS_CODE);
    assertThat(error.get("message").asText()).contains("target gas limit");
  }
}
