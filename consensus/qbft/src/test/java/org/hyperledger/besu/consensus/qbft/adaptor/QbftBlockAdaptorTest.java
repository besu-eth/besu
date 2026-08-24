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
package org.hyperledger.besu.consensus.qbft.adaptor;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.Vote;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.Collections;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Tests that the empty-block decision in QbftBlockHeightManager is based on a)
 * emptyblockperiodseconds elapsing, b) some transactions arriving, or c) a validator vote being
 * received
 */
class QbftBlockAdaptorTest {

  private static final QbftExtraDataCodec EXTRA_DATA_CODEC = new QbftExtraDataCodec();

  @Test
  void blockWithNoTransactionsAndNoVoteIsEmpty() {
    assertThat(new QbftBlockAdaptor(block(Optional.empty(), false)).isEmpty()).isTrue();
  }

  @Test
  void blockContainingAnAddVoteIsNotEmpty() {
    final Optional<Vote> vote = Optional.of(Vote.authVote(Address.fromHexString("1")));
    assertThat(new QbftBlockAdaptor(block(vote, false)).isEmpty()).isFalse();
  }

  @Test
  void blockContainingADropVoteIsNotEmpty() {
    final Optional<Vote> vote = Optional.of(Vote.dropVote(Address.fromHexString("2")));
    assertThat(new QbftBlockAdaptor(block(vote, false)).isEmpty()).isFalse();
  }

  @Test
  void blockWithTransactionsIsNotEmpty() {
    assertThat(new QbftBlockAdaptor(block(Optional.empty(), true)).isEmpty()).isFalse();
  }

  @Test
  void blockWithBothTransactionsAndAVoteIsNotEmpty() {
    final Optional<Vote> vote = Optional.of(Vote.authVote(Address.fromHexString("3")));
    assertThat(new QbftBlockAdaptor(block(vote, true)).isEmpty()).isFalse();
  }

  /**
   * Undecodable extra data must not wedge the decision; fall back to the transactions-only check.
   */
  @Test
  void blockWithUndecodableExtraDataFallsBackToTransactionsOnly() {
    final BlockHeader header =
        new BlockHeaderTestFixture().extraData(Bytes.fromHexString("0xdeadbeef")).buildHeader();
    final Block besuBlock =
        new Block(header, new BlockBody(Collections.emptyList(), Collections.emptyList()));
    assertThat(new QbftBlockAdaptor(besuBlock).isEmpty()).isTrue();
  }

  private Block block(final Optional<Vote> vote, final boolean withTransactions) {
    final BftExtraData extraData =
        new BftExtraData(
            Bytes.wrap(new byte[32]), Collections.emptyList(), vote, 0, Collections.emptyList());
    final BlockHeaderTestFixture fixture =
        new BlockHeaderTestFixture().extraData(EXTRA_DATA_CODEC.encode(extraData));
    // isEmpty() reads the header's transactions root, so a non-empty root is all that is needed to
    // represent "this block has transactions"
    if (withTransactions) {
      fixture.transactionsRoot(Hash.fromHexStringLenient("0x1234"));
    }
    return new Block(
        fixture.buildHeader(), new BlockBody(Collections.emptyList(), Collections.emptyList()));
  }
}
