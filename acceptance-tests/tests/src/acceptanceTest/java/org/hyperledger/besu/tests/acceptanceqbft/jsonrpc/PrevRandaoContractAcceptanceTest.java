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
package org.hyperledger.besu.tests.acceptanceqbft.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.web3j.generated.PrevRandaoContract;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

/**
 * Acceptance test for QBFT blocks that contain transactions reading block.prevrandao.
 *
 * <p>In BFT consensus the block proposer executes transactions using a ProcessableBlockHeader
 * whose mixHashOrPrevRandao field is Bytes32.ZERO, then overwrites the final block header's
 * mixHash with BftHelpers.EXPECTED_MIX_HASH. Validator nodes receive the final block and
 * re-execute transactions using EXPECTED_MIX_HASH as the PREVRANDAO value. When a contract
 * emits an indexed log whose value is block.prevrandao the log topics differ between proposer
 * and validator, causing a bloom-filter / receipts-root mismatch that causes validators to
 * reject the block and the chain to stall.
 */
public class PrevRandaoContractAcceptanceTest extends AcceptanceTestBase {

  private BesuNode validator1;
  private BesuNode validator2;
  private BesuNode validator3;

  @BeforeEach
  public void setUp() throws Exception {
    validator1 = besu.createQbftNode("validator1");
    validator2 = besu.createQbftNode("validator2");
    validator3 = besu.createQbftNode("validator3");
    cluster.start(validator1, validator2, validator3);
  }

  /**
   * Deploys PrevRandaoContract on a 3-node QBFT cluster and calls logPrevRandao(), which emits
   * PrevRandaoValue(block.prevrandao) as an indexed event. The test verifies that all validators
   * accept the block (receipts root matches) and the chain continues to produce blocks.
   *
   * <p>Before the fix in BftBlockCreator this test stalls: the proposer executes with
   * prevrandao=Bytes32.ZERO while validators re-execute with prevrandao=EXPECTED_MIX_HASH,
   * producing different log topics, different bloom filters, and ultimately a receipts-root
   * mismatch that causes every validator to reject every proposed block indefinitely.
   */
  @Test
  public void blockWithPrevRandaoLogShouldBeAcceptedByAllValidators() throws Exception {
    final PrevRandaoContract contract =
        validator1.execute(contractTransactions.createSmartContract(PrevRandaoContract.class));

    final TransactionReceipt receipt = contract.logPrevRandao().send();

    assertThat(receipt).isNotNull();
    assertThat(receipt.isStatusOK())
        .as(
            "logPrevRandao() transaction must be accepted; if it fails the chain has stalled due "
                + "to receipts-root mismatch between proposer (prevrandao=Bytes32.ZERO) and "
                + "validators (prevrandao=EXPECTED_MIX_HASH)")
        .isTrue();

    // Verify the chain continues to produce blocks beyond the one containing the transaction.
    cluster.verify(
        blockchain.reachesHeight(validator1, receipt.getBlockNumber().longValue() + 1, 30));
  }
}
