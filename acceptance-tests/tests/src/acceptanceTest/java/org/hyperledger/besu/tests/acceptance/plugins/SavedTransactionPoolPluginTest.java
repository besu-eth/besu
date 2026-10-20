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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.account.TransferTransaction;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.web3j.utils.Numeric;

/**
 * A saved transaction pool is restored only once the plugins have started and their registrations
 * are wired in. Two test plugins tell which side of the restore it was on. The permissioning
 * plugin's transaction provider rejects a gas limit of 22000 and accepts 21000, so of a pre-written
 * {@code txpool.dump} with one transaction of each, the accepted one must be in the pool and the
 * rejected one must not. The pool validator plugin rejects every transaction until its {@code
 * start()} has run, so a restore before the plugins started would keep both out. On a full-sync
 * node the pool used to be enabled while the node was built, before any {@code start()} and before
 * the provider was wired in, so the rejected transaction was restored too.
 */
public class SavedTransactionPoolPluginTest extends AcceptanceTestBase {
  private static final long ACCEPTED_GAS_LIMIT = 21000L;
  private static final long REJECTED_GAS_LIMIT = 22000L;

  @Test
  public void savedTransactionsAreRestoredThroughThePluginsOnceTheyHaveStarted()
      throws IOException {
    final BesuNode node =
        besu.createPluginsNode(
            "node1",
            List.of("testPlugins"),
            List.of(
                "--plugin-permissioning-test-enabled=true",
                "--plugin-txpool-validator1-test-enabled=true",
                "--tx-pool-enable-save-restore"),
            "TXPOOL");

    final Account recipient = accounts.createAccount("recipient");
    final TransferTransaction accepted =
        accountTransactions.createTransferWithGasLimit(recipient, 1, ACCEPTED_GAS_LIMIT);
    final TransferTransaction rejected =
        accountTransactions.createTransferWithGasLimit(recipient, 1, REJECTED_GAS_LIMIT);
    // one transaction per line: optional score, 'l' for local, then the Base64 of the RLP. The
    // rejected one goes first: lines are restored in order and the absence check below does not
    // wait, so the accepted one being in the pool means the rejected one has been decided
    Files.write(
        node.homeDirectory().resolve("txpool.dump"),
        List.of(dumpLine(rejected), dumpLine(accepted)));

    cluster.start(node);

    node.verify(txPoolConditions.inTransactionPool(Hash.fromHexString(accepted.transactionHash())));
    node.verify(
        txPoolConditions.notInTransactionPool(Hash.fromHexString(rejected.transactionHash())));
  }

  private static String dumpLine(final TransferTransaction transaction) {
    return "l"
        + Base64.getEncoder()
            .encodeToString(Numeric.hexStringToByteArray(transaction.signedTransactionData()));
  }
}
