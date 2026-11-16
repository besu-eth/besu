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
package org.hyperledger.besu.ethereum.eth.manager.task;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.ethtaskutils.PeerMessageTaskTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Test;

public class GetBodiesFromPeerTaskTest extends PeerMessageTaskTest<List<Block>> {

  @Override
  protected List<Block> generateDataToBeRequested() {
    final List<Block> requestedBlocks = new ArrayList<>();
    for (long i = 0; i < 3; i++) {
      final BlockHeader header = blockchain.getBlockHeader(10 + i).get();
      final BlockBody body = blockchain.getBlockBody(header.getHash()).get();
      requestedBlocks.add(new Block(header, body));
    }
    return requestedBlocks;
  }

  @Override
  protected EthTask<AbstractPeerTask.PeerTaskResult<List<Block>>> createTask(
      final List<Block> requestedData) {
    final List<BlockHeader> headersToComplete =
        requestedData.stream().map(Block::getHeader).collect(Collectors.toList());
    return GetBodiesFromPeerTask.forHeaders(
        protocolSchedule, ethContext, headersToComplete, metricsSystem);
  }

  @Override
  protected void assertPartialResultMatchesExpectation(
      final List<Block> requestedData, final List<Block> partialResponse) {
    assertThat(partialResponse.size()).isLessThanOrEqualTo(requestedData.size());
    assertThat(partialResponse.size()).isGreaterThan(0);
    for (final Block block : partialResponse) {
      assertThat(requestedData).contains(block);
    }
  }

  @Test
  public void assertBodyIdentifierUsesWithdrawalsToGenerateBodyIdentifiers() {
    final Withdrawal withdrawal =
        new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x1"), GWei.ONE);

    // Empty body block
    final BlockBody emptyBodyBlock = BlockBody.empty();
    // Block with no tx, no ommers, 1 withdrawal
    final BlockBody bodyBlockWithWithdrawal =
        new BlockBody(emptyList(), emptyList(), Optional.of(List.of(withdrawal)));

    assertThat(
            new BodyIdentifier(emptyBodyBlock).equals(new BodyIdentifier(bodyBlockWithWithdrawal)))
        .isFalse();
  }

  @Test
  public void bodyIdentifierTakesTheTransactionsRootTheBodyKeeps() {
    final BlockHeader header = blockchain.getBlockHeader(10).get();
    final BlockBody body = spy(blockchain.getBlockBody(header.getHash()).get());

    assertThat(new BodyIdentifier(body)).isEqualTo(new BodyIdentifier(header));
    verify(body).getTransactionsRoot();
  }
}
