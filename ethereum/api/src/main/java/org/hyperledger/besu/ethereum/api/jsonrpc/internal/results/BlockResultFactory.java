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

import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.util.HexUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

  // endregion BlockResult

  // region EngineGetPayloadResult

  public EngineGetPayloadResultV1 payloadTransactionCompleteV1(final Block block) {
    final List<String> txs = txsAsHex(block);

    return new EngineGetPayloadResultV1(block.getHeader(), txs);
  }

  public EngineGetPayloadResultV2 payloadTransactionCompleteV2(final PayloadWrapper payload) {
    final var blockWithReceipts = payload.blockWithReceipts();
    final Block block = blockWithReceipts.getBlock();
    final List<String> txs = txsAsHex(block);

    return new EngineGetPayloadResultV2(
        blockWithReceipts.getHeader(),
        txs,
        block.getBody().getWithdrawals(),
        Quantity.create(payload.blockValue()));
  }

  public EngineGetPayloadResultV3 payloadTransactionCompleteV3(final PayloadWrapper payload) {
    final var blockWithReceipts = payload.blockWithReceipts();
    final List<String> txs = txsAsHex(blockWithReceipts.getBlock());

    final BlobsBundleV1 blobsBundleV1 =
        new BlobsBundleV1(blockWithReceipts.getBlock().getBody().getTransactions());
    return new EngineGetPayloadResultV3(
        blockWithReceipts.getHeader(),
        txs,
        blockWithReceipts.getBlock().getBody().getWithdrawals(),
        Quantity.create(payload.blockValue()),
        blobsBundleV1);
  }

  public EngineGetPayloadResultV4 payloadTransactionCompleteV4(final PayloadWrapper payload) {
    final var blockWithReceipts = payload.blockWithReceipts();
    final List<String> txs = txsAsHex(blockWithReceipts.getBlock());
    final Optional<List<String>> requestsWithoutRequestId = requestsAsHex(payload);

    final BlobsBundleV1 blobsBundleV1 =
        new BlobsBundleV1(blockWithReceipts.getBlock().getBody().getTransactions());
    return new EngineGetPayloadResultV4(
        blockWithReceipts.getHeader(),
        txs,
        blockWithReceipts.getBlock().getBody().getWithdrawals(),
        requestsWithoutRequestId,
        Quantity.create(payload.blockValue()),
        blobsBundleV1);
  }

  public EngineGetPayloadResultV5 payloadTransactionCompleteV5(final PayloadWrapper payload) {
    final var blockWithReceipts = payload.blockWithReceipts();
    final List<String> txs = txsAsHex(blockWithReceipts.getBlock());
    final Optional<List<String>> requestsWithoutRequestId = requestsAsHex(payload);

    final BlobsBundleV2 blobsBundleV2 =
        new BlobsBundleV2(blockWithReceipts.getBlock().getBody().getTransactions());
    return new EngineGetPayloadResultV5(
        blockWithReceipts.getHeader(),
        txs,
        blockWithReceipts.getBlock().getBody().getWithdrawals(),
        requestsWithoutRequestId,
        Quantity.create(payload.blockValue()),
        blobsBundleV2);
  }

  public EngineGetPayloadResultV6 payloadTransactionCompleteV6(final PayloadWrapper payload) {
    final var blockWithReceipts = payload.blockWithReceipts();
    final List<String> txs = txsAsHex(blockWithReceipts.getBlock());
    final Optional<List<String>> requestsWithoutRequestId = requestsAsHex(payload);

    final BlobsBundleV2 blobsBundleV2 =
        new BlobsBundleV2(blockWithReceipts.getBlock().getBody().getTransactions());

    final String blockAccessList =
        payload
            .blockAccessList()
            .map(
                bal -> {
                  final BytesValueRLPOutput output = new BytesValueRLPOutput();
                  bal.writeTo(output);
                  return output.encoded().toHexString();
                })
            .orElse(null);

    final String slotNumber =
        blockWithReceipts.getHeader().getOptionalSlotNumber().map(Quantity::create).orElse(null);

    return new EngineGetPayloadResultV6(
        blockWithReceipts.getHeader(),
        txs,
        blockWithReceipts.getBlock().getBody().getWithdrawals(),
        requestsWithoutRequestId,
        Quantity.create(payload.blockValue()),
        blobsBundleV2,
        blockAccessList,
        slotNumber);
  }

  private static List<String> txsAsHex(final Block block) {
    return block.getBody().getTransactions().stream()
        .map(
            transaction ->
                TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY))
        .map(b -> HexUtils.toFastHex(b, true))
        .collect(Collectors.toList());
  }

  private static Optional<List<String>> requestsAsHex(final PayloadWrapper payload) {
    return payload
        .requests()
        .map(
            rqs ->
                rqs.stream()
                    .sorted(Comparator.comparing(Request::getType))
                    .filter(r -> !r.getData().isEmpty())
                    .map(Request::getEncodedRequest)
                    .map(b -> HexUtils.toFastHex(b, true))
                    .toList());
  }

  // endregion EngineGetPayloadResult

  // region EngineGetPayloadBodiesResult

  public EngineGetPayloadBodiesResultV1 payloadBodiesCompleteV1(
      final List<Optional<BlockBody>> blockBodies) {
    final List<EngineGetPayloadBodiesResultV1.PayloadBody> payloadBodies =
        blockBodies.stream()
            .map(
                maybeBody ->
                    maybeBody.map(EngineGetPayloadBodiesResultV1.PayloadBody::new).orElse(null))
            .collect(Collectors.toList());
    return new EngineGetPayloadBodiesResultV1(payloadBodies);
  }

  public EngineGetPayloadBodiesResultV2 payloadBodiesCompleteV2(
      final List<Optional<BlockBody>> blockBodies, final List<Optional<String>> blockAccessLists) {
    final List<EngineGetPayloadBodiesResultV2.PayloadBody> payloadBodies =
        IntStream.range(0, blockBodies.size())
            .mapToObj(
                index ->
                    blockBodies
                        .get(index)
                        .map(
                            body ->
                                new EngineGetPayloadBodiesResultV2.PayloadBody(
                                    body, blockAccessLists.get(index).orElse(null)))
                        .orElse(null))
            .collect(Collectors.toList());
    return new EngineGetPayloadBodiesResultV2(payloadBodies);
  }

  // endregion EngineGetPayloadBodiesResult
}
