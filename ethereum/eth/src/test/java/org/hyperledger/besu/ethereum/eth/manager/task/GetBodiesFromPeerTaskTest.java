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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.ethtaskutils.PeerMessageTaskTest;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Test;

public class GetBodiesFromPeerTaskTest extends PeerMessageTaskTest<List<Block>> {

  private static final Bytes MALFORMED_BODIES_RLP =
      Bytes.fromHexString(
          "0xf871b86f04f86c83301824800285012a05f2008307a1209471562b71999873db5b286df957af199ec94617f78080c0c080a0df13441160d9e36a96c4f27f7be42f0a67de1b27345d32e562d7a7e80cc61332a04160c3339755fd0f41d852dff56da6b71a975eda6fefdf1d00ba6d8b3ce3e0d2");

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
  public void disconnectsPeerWhenBodyContainsEmptyAuthorizationListCodeDelegationTx() {
    final RespondingEthPeer respondingPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 32);

    final List<Block> requestedData = generateDataToBeRequested();
    final EthTask<AbstractPeerTask.PeerTaskResult<List<Block>>> task = createTask(requestedData);
    final CompletableFuture<?> future = task.run();

    final RespondingEthPeer.Responder malformed =
        (cap, peer, msg) -> Optional.of(BlockBodiesMessage.createUnsafe(MALFORMED_BODIES_RLP));
    respondingPeer.respondWhile(malformed, () -> !future.isDone());

    assertThat(respondingPeer.getPeerConnection().isDisconnected()).isTrue();
    assertThat(future).isCompletedExceptionally();
  }
}
