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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;

public class BlockResultFactory {

  // region BlockResult

  public BlockResult transactionComplete(
      final BlockWithMetadata<TransactionWithMetadata, Hash> blockWithMetadata) {
    return transactionComplete(blockWithMetadata, false);
  }

  public BlockResult transactionComplete(
      final BlockWithMetadata<TransactionWithMetadata, Hash> blockWithMetadata,
      final boolean includeCoinbase) {
    final List<TransactionResult> txs =
        blockWithMetadata.getTransactions().stream()
            .map(TransactionWithMetadataResult::new)
            .collect(Collectors.toList());
    final List<JsonNode> ommers =
        blockWithMetadata.getOmmers().stream()
            .map(Hash::toString)
            .map(TextNode::new)
            .collect(Collectors.toList());
    return new BlockResult(
        blockWithMetadata.getHeader(),
        txs,
        ommers,
        blockWithMetadata.getTotalDifficulty(),
        blockWithMetadata.getSize(),
        includeCoinbase,
        blockWithMetadata.getWithdrawals());
  }

  public BlockResult transactionComplete(final Block block) {
    final int count = block.getBody().getTransactions().size();
    final List<TransactionWithMetadata> transactionWithMetadata = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      transactionWithMetadata.add(
          new TransactionWithMetadata(
              block.getBody().getTransactions().get(i),
              block.getHeader().getNumber(),
              block.getHeader().getBaseFee(),
              block.getHash(),
              i,
              block.getHeader().getTimestamp()));
    }
    final List<TransactionResult> txs =
        transactionWithMetadata.stream()
            .map(TransactionWithMetadataResult::new)
            .collect(Collectors.toList());

    final List<JsonNode> ommers =
        block.getBody().getOmmers().stream()
            .map(BlockHeader::getHash)
            .map(Hash::toString)
            .map(TextNode::new)
            .collect(Collectors.toList());
    return new BlockResult(
        block.getHeader(),
        txs,
        ommers,
        block.getHeader().getDifficulty(),
        block.getSize(),
        false,
        block.getBody().getWithdrawals());
  }

  public BlockResult transactionHash(final BlockWithMetadata<Hash, Hash> blockWithMetadata) {
    return transactionHash(blockWithMetadata, false);
  }

  public BlockResult transactionHash(
      final BlockWithMetadata<Hash, Hash> blockWithMetadata, final boolean includeCoinbase) {
    final List<TransactionResult> txs =
        blockWithMetadata.getTransactions().stream()
            .map(Hash::toString)
            .map(TransactionHashResult::new)
            .collect(Collectors.toList());
    final List<JsonNode> ommers =
        blockWithMetadata.getOmmers().stream()
            .map(Hash::toString)
            .map(TextNode::new)
            .collect(Collectors.toList());
    return new BlockResult(
        blockWithMetadata.getHeader(),
        txs,
        ommers,
        blockWithMetadata.getTotalDifficulty(),
        blockWithMetadata.getSize(),
        includeCoinbase,
        blockWithMetadata.getWithdrawals());
  }
}
