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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;

import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

/**
 * A block whose state root is right but whose receipts root or gas used is wrong is only rejected
 * after its post state has been computed. Processing it on the head world state must not leave the
 * head on that block, which is never stored in the blockchain.
 */
class InvalidBlockOutputTest extends AbstractIsolationTests {

  @Test
  void receiptsRootMismatchLeavesTheHeadWorldStateOnTheParent() {
    assertRejectedOutputLeavesTheHeadWorldStateOnTheParent(
        header -> header.receiptsRoot(Hash.EMPTY_TRIE_HASH));
  }

  @Test
  void gasUsedMismatchLeavesTheHeadWorldStateOnTheParent() {
    assertRejectedOutputLeavesTheHeadWorldStateOnTheParent(header -> header.gasUsed(42_000L));
  }

  private void assertRejectedOutputLeavesTheHeadWorldStateOnTheParent(
      final UnaryOperator<BlockHeaderBuilder> corruption) {
    final BlockHeader genesis = blockchain.getChainHeadHeader();
    final Block valid =
        forTransactions(List.of(burnTransaction(sender1, 0L, Address.fromHexString("0xdeadbeef"))));
    final Block invalid =
        new Block(
            corruption
                .apply(BlockHeaderBuilder.fromHeader(valid.getHeader()))
                .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
                .buildBlockHeader(),
            valid.getBody());
    assertThat(invalid.getHeader().getStateRoot()).isEqualTo(valid.getHeader().getStateRoot());
    assertThat(executeBlock(archive.getWorldState(), valid).isSuccessful()).isTrue();

    final BlockProcessingResult result =
        protocolSchedule
            .getByBlockHeader(invalid.getHeader())
            .getBlockValidator()
            .validateAndProcessBlock(
                protocolContext, invalid, HeaderValidationMode.NONE, HeaderValidationMode.NONE);

    assertThat(result.isFailed()).isTrue();
    assertThat(blockchain.getBlockHeader(invalid.getHash())).isEmpty();
    assertThat(((PathBasedWorldState) archive.getWorldState()).blockHash())
        .isEqualTo(genesis.getHash());
    assertThat(archive.getWorldState().rootHash()).isEqualTo(genesis.getStateRoot());
    // the head can still move to the valid sibling, as a forkchoiceUpdated to it would ask
    assertThat(
            archive.getWorldState(
                WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(valid.getHeader())))
        .isPresent();
  }
}
