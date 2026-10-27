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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.query.TransactionReceiptWithMetadata;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameReceipt;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class FrameTransactionResultTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Address SENDER =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final Address TARGET =
      Address.fromHexString("0x00000000000000000000000000000000000000bb");

  private static Transaction frameTransaction(final Frame... trailing) {
    final List<Frame> frames = new ArrayList<>();
    frames.add(
        new Frame(Frame.MODE_VERIFY, 3, Optional.empty(), 50_000L, 1_000L, Wei.ZERO, Bytes.EMPTY));
    frames.add(
        new Frame(
            Frame.MODE_SENDER,
            0,
            Optional.of(TARGET),
            75_000L,
            0L,
            Wei.of(42),
            Bytes.fromHexString("0xdeadbeef")));
    frames.addAll(List.of(trailing));
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(7)
        .nonceKeys(List.of(UInt256.valueOf(0xbeef), UInt256.valueOf(0xcafe)))
        .sender(SENDER)
        .frames(frames)
        .frameSignatures(
            List.of(
                new FrameSignature(
                    FrameSignature.SCHEME_SECP256K1,
                    Bytes.EMPTY,
                    Bytes.EMPTY,
                    Bytes.repeat((byte) 7, 65))))
        .maxPriorityFeePerGas(Wei.of(2))
        .maxFeePerGas(Wei.of(100))
        .maxFeePerBlobGas(Wei.ZERO)
        .build();
  }

  @Test
  void transactionCarriesFramesSignaturesAndNonceKeys() {
    final JsonNode json =
        MAPPER.valueToTree(
            new TransactionWithMetadataResult(
                new TransactionWithMetadata(
                    frameTransaction(), 1L, Optional.of(Wei.of(7)), Hash.ZERO, 0, 0L)));

    assertThat(json.get("type").asText()).isEqualTo("0x6");
    assertThat(json.get("nonce").asText()).isEqualTo("0x7");
    assertThat(json.get("nonceKeys").toString()).isEqualTo("[\"0xbeef\",\"0xcafe\"]");
    // No outer signature: JsonRpcResult omits null fields, so v, yParity, r and s are all absent.
    assertThat(json.has("v")).isFalse();
    assertThat(json.has("yParity")).isFalse();
    assertThat(json.has("r")).isFalse();
    assertThat(json.has("s")).isFalse();

    final JsonNode verify = json.get("frames").get(0);
    assertThat(verify.get("mode").asText()).isEqualTo("0x1");
    assertThat(verify.get("flags").asText()).isEqualTo("0x3");
    assertThat(verify.has("target")).isFalse();
    assertThat(verify.get("executionGas").asText()).isEqualTo("0xc350");
    assertThat(verify.get("stateGas").asText()).isEqualTo("0x3e8");
    final JsonNode sender = json.get("frames").get(1);
    assertThat(sender.get("target").asText()).isEqualTo(TARGET.toString());
    assertThat(sender.get("value").asText()).isEqualTo("0x2a");
    assertThat(sender.get("data").asText()).isEqualTo("0xdeadbeef");

    final JsonNode signature = json.get("signatures").get(0);
    assertThat(signature.get("scheme").asText()).isEqualTo("0x1");
    assertThat(signature.get("signer").asText()).isEqualTo("0x");
    assertThat(signature.get("msg").asText()).isEqualTo("0x");
    assertThat(signature.get("signature").asText()).hasSize(2 + 130);
  }

  @Test
  void otherTransactionTypesCarryNoFrameFields() {
    final Transaction transaction = new BlockDataGenerator().transaction(TransactionType.EIP1559);
    final JsonNode json =
        MAPPER.valueToTree(
            new TransactionWithMetadataResult(
                new TransactionWithMetadata(
                    transaction, 1L, Optional.of(Wei.of(7)), Hash.ZERO, 0, 0L)));
    assertThat(json.has("nonceKeys")).isFalse();
    assertThat(json.has("frames")).isFalse();
    assertThat(json.has("signatures")).isFalse();
    // A signed transaction still renders its signature.
    assertThat(json.get("r").asText()).isEqualTo(Quantity.create(transaction.getR()));
    assertThat(json.get("s").asText()).isEqualTo(Quantity.create(transaction.getS()));
    assertThat(json.get("yParity").asText()).isEqualTo(Quantity.create(transaction.getYParity()));
  }

  @Test
  void receiptCarriesPayerAndFrameReceiptsWithContinuousLogIndexes() {
    final Log log = new Log(TARGET, Bytes.of(1), List.<LogTopic>of());
    final TransactionReceipt receipt =
        new TransactionReceipt(
            TARGET,
            90_000L,
            List.of(
                new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 100L, 50L, List.of(log, log)),
                new FrameReceipt(FrameReceipt.STATUS_FAILURE, 30_000L, 0L, List.of(log))));
    final Transaction transaction = frameTransaction();
    final JsonNode json =
        MAPPER.valueToTree(
            new TransactionReceiptStatusResult(
                TransactionReceiptWithMetadata.create(
                    receipt,
                    transaction,
                    transaction.getHash(),
                    0,
                    30_150L,
                    Optional.of(Wei.of(7)),
                    Hash.ZERO,
                    0L,
                    1L,
                    Optional.empty(),
                    Optional.empty(),
                    5)));

    assertThat(json.get("payer").asText()).isEqualTo(TARGET.toString());
    final JsonNode first = json.get("frameReceipts").get(0);
    assertThat(first.get("status").asText()).isEqualTo("0x1");
    assertThat(first.get("gasUsed").asText()).isEqualTo("0x96");
    assertThat(first.get("executionGasUsed").asText()).isEqualTo("0x64");
    assertThat(first.get("stateGasUsed").asText()).isEqualTo("0x32");
    assertThat(first.get("logs").get(1).get("logIndex").asText()).isEqualTo("0x6");
    final JsonNode second = json.get("frameReceipts").get(1);
    assertThat(second.get("status").asText()).isEqualTo("0x0");
    // The second frame's log continues after the first frame's two, from the block offset of 5.
    assertThat(second.get("logs").get(0).get("logIndex").asText()).isEqualTo("0x7");
    // The transaction-level logs are the same entries, numbered the same way.
    assertThat(json.get("logs").get(2).get("logIndex").asText()).isEqualTo("0x7");
  }

  private static Frame postTxFrame() {
    return new Frame(
        Frame.MODE_POST_TX, 0, Optional.of(TARGET), 30_000L, 0L, Wei.ZERO, Bytes.EMPTY);
  }

  @Test
  void aPostTxFrameRendersMode3() {
    final Transaction transaction = frameTransaction(postTxFrame());
    final JsonNode json =
        MAPPER.valueToTree(
            new TransactionWithMetadataResult(
                new TransactionWithMetadata(
                    transaction, 1L, Optional.of(Wei.of(7)), Hash.ZERO, 0, 0L)));

    final JsonNode postTx = json.get("frames").get(2);
    assertThat(postTx.get("mode").asText()).isEqualTo("0x3");
    assertThat(postTx.get("target").asText()).isEqualTo(TARGET.toString());
    assertThat(postTx.get("executionGas").asText()).isEqualTo("0x7530");
  }

  @Test
  void aFailedPostTxFrameRendersTheRevertedBodyAndAFailedStatus() {
    // EIP-7906: the assertion at frame 2 failed. The body (frame 1) keeps its status and gas and
    // loses its logs; frame 3 is skipped. The prefix's log is the transaction's only log.
    final Log prefixLog = new Log(SENDER, Bytes.of(1), List.<LogTopic>of());
    final TransactionReceipt receipt =
        new TransactionReceipt(
            SENDER,
            60_000L,
            List.of(
                new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 3_000L, 0L, List.of(prefixLog)),
                new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 25_000L, 0L, List.of()),
                new FrameReceipt(FrameReceipt.STATUS_FAILURE, 3_006L, 0L, List.of()),
                new FrameReceipt(FrameReceipt.STATUS_SKIPPED, 0L, 0L, List.of())));
    final Transaction transaction = frameTransaction(postTxFrame(), postTxFrame());
    final JsonNode json =
        MAPPER.valueToTree(
            new TransactionReceiptStatusResult(
                TransactionReceiptWithMetadata.create(
                    receipt,
                    transaction,
                    transaction.getHash(),
                    0,
                    31_006L,
                    Optional.of(Wei.of(7)),
                    Hash.ZERO,
                    0L,
                    1L,
                    Optional.empty(),
                    Optional.empty(),
                    5)));

    // A single status is derived from the frames, as EIP-8141 requires and geth's FrameTxStatus
    // does: any frame that did not succeed makes it 0.
    assertThat(json.get("status").asText()).isEqualTo("0x0");
    final JsonNode frames = json.get("frameReceipts");
    assertThat(frames).hasSize(4);
    assertThat(frames.get(0).get("status").asText()).isEqualTo("0x1");
    assertThat(frames.get(1).get("status").asText()).isEqualTo("0x1");
    assertThat(frames.get(1).get("executionGasUsed").asText()).isEqualTo("0x61a8");
    assertThat(frames.get(1).get("logs")).isEmpty();
    assertThat(frames.get(2).get("status").asText()).isEqualTo("0x0");
    assertThat(frames.get(2).get("gasUsed").asText()).isEqualTo("0xbbe");
    assertThat(frames.get(3).get("status").asText()).isEqualTo("0x2");
    assertThat(frames.get(3).get("gasUsed").asText()).isEqualTo("0x0");
    assertThat(frames.get(3).get("logs")).isEmpty();
    assertThat(frames.get(0).get("logs").get(0).get("logIndex").asText()).isEqualTo("0x5");
    assertThat(json.get("logs")).hasSize(1);
    assertThat(json.get("logs").get(0).get("logIndex").asText()).isEqualTo("0x5");
  }
}
