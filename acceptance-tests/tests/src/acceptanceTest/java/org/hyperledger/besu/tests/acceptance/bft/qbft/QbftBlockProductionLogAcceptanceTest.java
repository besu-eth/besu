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
package org.hyperledger.besu.tests.acceptance.bft.qbft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNodeRunner;

import java.time.Duration;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public class QbftBlockProductionLogAcceptanceTest extends AcceptanceTestBase {

  private static final Pattern PRODUCED_LOG_PATTERN =
      Pattern.compile(
          "Produced #.*Timing\\(started at .*duplicateWorldState=\\d+ms.*preTxsSelection=\\d+ms"
              + ".*selectedTxsEvaluation=\\d+ms.*blockAssembled=\\d+ms.*blockImported=\\d+ms\\)");

  @Test
  public void producedBlockLogReportsBlockCreationTimings() throws Exception {
    Assumptions.assumeTrue(
        BesuNodeRunner.isProcessBesuNodeRunner(),
        "Console capture is only available when Besu runs as a process");

    final BesuNode minerNode = besu.createQbftNode("miner1");
    cluster.startConsoleCapture();
    cluster.start(minerNode);

    cluster.verify(blockchain.reachesHeight(minerNode, 1));

    final Account sender = accounts.createAccount("account1");
    minerNode.execute(accountTransactions.createTransfer(sender, 50));
    cluster.verify(sender.balanceEquals(50));

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(PRODUCED_LOG_PATTERN.matcher(cluster.peekConsoleContents()).find())
                    .as("a 'Produced #...' log line with Timing information")
                    .isTrue());

    assertThat(cluster.peekConsoleContents()).doesNotContain("Produced block", "Produced empty");
  }
}
