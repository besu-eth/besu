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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameReceipt;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class FrameTransactionCodecTest {

  private static final Address SENDER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address TARGET =
      Address.fromHexString("0x2222222222222222222222222222222222222222");

  private Transaction sampleTransaction() {
    final Frame verifyFrame =
        new Frame(
            Frame.MODE_VERIFY,
            Frame.APPROVE_SCOPE_MASK,
            Optional.empty(),
            50_000L,
            100_000L,
            Wei.ZERO,
            Bytes.EMPTY);
    final Frame senderFrame =
        new Frame(
            Frame.MODE_SENDER,
            0,
            Optional.of(TARGET),
            75_000L,
            0L,
            Wei.of(42),
            Bytes.fromHexString("0xdeadbeef"));
    final FrameSignature canonicalSignature =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, Bytes.repeat((byte) 7, 65));
    final FrameSignature explicitDigestSignature =
        new FrameSignature(
            FrameSignature.SCHEME_ARBITRARY,
            Bytes.EMPTY,
            Bytes32.leftPad(Bytes.of(9)),
            Bytes.fromHexString("0x0102030405"));
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(3)
        .sender(SENDER)
        .frames(List.of(verifyFrame, senderFrame))
        .frameSignatures(List.of(canonicalSignature, explicitDigestSignature))
        .maxPriorityFeePerGas(Wei.of(2))
        .maxFeePerGas(Wei.of(100))
        .maxFeePerBlobGas(Wei.ZERO)
        .build();
  }

  @Test
  void roundTripsThroughOpaqueBytes() {
    final Transaction transaction = sampleTransaction();
    final Bytes encoded =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);
    assertThat(encoded.get(0)).isEqualTo((byte) 0x06);

    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.BLOCK_BODY);
    assertThat(decoded.getType()).isEqualTo(TransactionType.FRAME);
    assertThat(decoded.getSender().getBytes()).isEqualTo(SENDER.getBytes());
    assertThat(decoded.getNonce()).isEqualTo(3);
    assertThat(decoded.getChainId()).contains(BigInteger.ONE);
    assertThat(decoded.getFrames().orElseThrow()).isEqualTo(transaction.getFrames().orElseThrow());
    assertThat(decoded.getFrameSignatures().orElseThrow())
        .isEqualTo(transaction.getFrameSignatures().orElseThrow());
    assertThat(decoded.getMaxFeePerGas()).contains(Wei.of(100));
    assertThat(decoded.getMaxPriorityFeePerGas()).contains(Wei.of(2));

    // Re-encoding is byte-identical, and so is the hash.
    final Bytes reEncoded =
        TransactionEncoder.encodeOpaqueBytes(decoded, EncodingContext.BLOCK_BODY);
    assertThat(reEncoded).isEqualTo(encoded);
    assertThat(decoded.getHash()).isEqualTo(transaction.getHash());
  }

  @Test
  void signatureHashElidesOnlyCanonicalHashEntries() {
    final Transaction transaction = sampleTransaction();
    // Same transaction with different raw bytes in the canonical-hash entry: same signing hash.
    final Transaction differentCanonicalSig =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonce(3)
            .sender(SENDER)
            .frames(transaction.getFrames().orElseThrow())
            .frameSignatures(
                List.of(
                    new FrameSignature(
                        FrameSignature.SCHEME_SECP256K1,
                        Bytes.EMPTY,
                        Bytes.EMPTY,
                        Bytes.repeat((byte) 1, 65)),
                    transaction.getFrameSignatures().orElseThrow().get(1)))
            .maxPriorityFeePerGas(Wei.of(2))
            .maxFeePerGas(Wei.of(100))
            .maxFeePerBlobGas(Wei.ZERO)
            .build();
    assertThat(differentCanonicalSig.getFrameSignatureHash())
        .isEqualTo(transaction.getFrameSignatureHash());

    // But changing the explicit-digest entry's raw bytes changes the signing hash.
    final Transaction differentArbitrarySig =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonce(3)
            .sender(SENDER)
            .frames(transaction.getFrames().orElseThrow())
            .frameSignatures(
                List.of(
                    transaction.getFrameSignatures().orElseThrow().get(0),
                    new FrameSignature(
                        FrameSignature.SCHEME_ARBITRARY,
                        Bytes.EMPTY,
                        Bytes32.leftPad(Bytes.of(9)),
                        Bytes.fromHexString("0x99"))))
            .maxPriorityFeePerGas(Wei.of(2))
            .maxFeePerGas(Wei.of(100))
            .maxFeePerBlobGas(Wei.ZERO)
            .build();
    assertThat(differentArbitrarySig.getFrameSignatureHash())
        .isNotEqualTo(transaction.getFrameSignatureHash());
  }

  @Test
  void gasLimitIsDerivedAsMaxGas() {
    final Transaction transaction = sampleTransaction();
    final long expected =
        org.hyperledger.besu.ethereum.core.FrameTransactionGas.maxGas(
            transaction.getFrames().orElseThrow(),
            transaction.getFrameSignatures().orElseThrow(),
            SENDER);
    assertThat(transaction.getGasLimit()).isEqualTo(expected);
    assertThat(transaction.isContractCreation()).isFalse();
  }

  @Test
  void blobCarryingFrameTransactionRoundTrips() {
    final VersionedHash versionedHash =
        new VersionedHash(Bytes32.wrap(Bytes.concatenate(Bytes.of(1), Bytes.repeat((byte) 5, 31))));
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonce(0)
            .sender(SENDER)
            .frames(
                List.of(
                    new Frame(
                        Frame.MODE_VERIFY,
                        Frame.APPROVE_SCOPE_MASK,
                        Optional.empty(),
                        50_000L,
                        0L,
                        Wei.ZERO,
                        Bytes.EMPTY)))
            .frameSignatures(List.of())
            .maxPriorityFeePerGas(Wei.of(2))
            .maxFeePerGas(Wei.of(100))
            .maxFeePerBlobGas(Wei.of(1))
            .versionedHashes(List.of(versionedHash))
            .build();
    final Bytes encoded =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);
    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.BLOCK_BODY);
    assertThat(decoded.getVersionedHashes().orElseThrow()).containsExactly(versionedHash);
    assertThat(decoded.getMaxFeePerBlobGas()).contains(Wei.of(1));
  }

  @Test
  void frameReceiptRoundTrips() {
    final Log log =
        new Log(
            TARGET,
            Bytes.fromHexString("0x0badf00d"),
            List.of(LogTopic.wrap(Bytes32.leftPad(Bytes.of(1)))));
    final TransactionReceipt receipt =
        new TransactionReceipt(
            SENDER,
            123_456L,
            List.of(
                new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 100L, 0L, List.of()),
                new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 3_000L, 183_600L, List.of(log)),
                new FrameReceipt(FrameReceipt.STATUS_SKIPPED, 0L, 0L, List.of())));

    // The opaque (wire/storage) encodings must round-trip through the decoder.
    for (final TransactionReceiptEncodingConfiguration configuration :
        List.of(
            TransactionReceiptEncodingConfiguration.DEFAULT,
            TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION,
            TransactionReceiptEncodingConfiguration.STORAGE_WITH_COMPACTION)) {
      final BytesValueRLPOutput out = new BytesValueRLPOutput();
      TransactionReceiptEncoder.writeTo(receipt, out, configuration);
      final TransactionReceipt decoded =
          TransactionReceiptDecoder.readFrom(RLP.input(out.encoded()), true);
      assertThat(decoded.getTransactionType()).isEqualTo(TransactionType.FRAME);
      assertThat(decoded.getPayer().orElseThrow().getBytes()).isEqualTo(SENDER.getBytes());
      assertThat(decoded.getCumulativeGasUsed()).isEqualTo(123_456L);
      assertThat(decoded.getFrameReceipts().orElseThrow())
          .isEqualTo(receipt.getFrameReceipts().orElseThrow());
      assertThat(decoded.getLogsList()).containsExactly(log);
      assertThat(decoded).isEqualTo(receipt);
    }

    // The trie-root form (used only for hashing, like other typed receipts) is the typed
    // envelope: type byte followed by the frame receipt payload.
    final BytesValueRLPOutput trieRootOut = new BytesValueRLPOutput();
    TransactionReceiptEncoder.writeTo(
        receipt, trieRootOut, TransactionReceiptEncodingConfiguration.TRIE_ROOT);
    assertThat(trieRootOut.encoded().get(0)).isEqualTo((byte) 0x06);
  }
}
