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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.FrameTransactionFrame;
import org.hyperledger.besu.datatypes.FrameTransactionSignature;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class FrameTransactionEncodingTest {

  private static final Address TEST_SENDER =
      Address.fromHexString("0x1234567890abcdef1234567890abcdef12345678");

  private static final Address TEST_TARGET =
      Address.fromHexString("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

  @Test
  void shouldEncodeAndDecodeBasicFrameTransaction() {
    final FrameTransactionFrame frame =
        new FrameTransactionFrame(
            FrameTransactionFrame.MODE_VERIFY,
            FrameTransactionFrame.APPROVAL_BOTH,
            Optional.of(TEST_TARGET),
            50_000L,
            Wei.ZERO,
            Bytes.fromHexString("0xdeadbeef"));

    final FrameTransactionSignature sig =
        new FrameTransactionSignature(
            FrameTransactionSignature.SCHEME_SECP256K1,
            TEST_SENDER,
            Bytes.EMPTY,
            Bytes.fromHexString("0x" + "ab".repeat(65)));

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .sender(TEST_SENDER)
            .frames(List.of(frame))
            .frameSignatures(List.of(sig))
            .maxPriorityFeePerGas(Wei.of(1_000_000_000L))
            .maxFeePerGas(Wei.of(2_000_000_000L))
            .maxFeePerBlobGas(Wei.ZERO)
            .gasLimit(50_000L)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .build();

    // Encode
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeByte(TransactionType.FRAME.getSerializedType());
    FrameTransactionEncoder.encode(tx, out);
    final Bytes encoded = out.encoded();

    // Decode
    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.BLOCK_BODY);

    assertThat(decoded.getType()).isEqualTo(TransactionType.FRAME);
    assertThat(decoded.getChainId()).contains(BigInteger.ONE);
    assertThat(decoded.getNonce()).isEqualTo(1L);
    assertThat(decoded.getSender()).isEqualTo(TEST_SENDER);
    assertThat(decoded.getMaxPriorityFeePerGas()).contains(Wei.of(1_000_000_000L));
    assertThat(decoded.getMaxFeePerGas()).contains(Wei.of(2_000_000_000L));

    // Verify frames
    assertThat(decoded.getFrames()).isPresent();
    final List<FrameTransactionFrame> frames = decoded.getFrames().get();
    assertThat(frames).hasSize(1);
    assertThat(frames.get(0).mode()).isEqualTo(FrameTransactionFrame.MODE_VERIFY);
    assertThat(frames.get(0).flags()).isEqualTo(FrameTransactionFrame.APPROVAL_BOTH);
    assertThat(frames.get(0).target()).contains(TEST_TARGET);
    assertThat(frames.get(0).gasLimit()).isEqualTo(50_000L);
    assertThat(frames.get(0).value()).isEqualTo(Wei.ZERO);
    assertThat(frames.get(0).data()).isEqualTo(Bytes.fromHexString("0xdeadbeef"));

    // Verify signatures
    assertThat(decoded.getFrameSignatures()).isPresent();
    final List<FrameTransactionSignature> sigs = decoded.getFrameSignatures().get();
    assertThat(sigs).hasSize(1);
    assertThat(sigs.get(0).scheme()).isEqualTo(FrameTransactionSignature.SCHEME_SECP256K1);
    assertThat(sigs.get(0).signer()).isEqualTo(TEST_SENDER);
    assertThat(sigs.get(0).msg()).isEqualTo(Bytes.EMPTY);
    assertThat(sigs.get(0).signature()).isEqualTo(Bytes.fromHexString("0x" + "ab".repeat(65)));
  }

  @Test
  void shouldEncodeAndDecodeMultiFrameTransaction() {
    final FrameTransactionFrame verifyFrame =
        new FrameTransactionFrame(
            FrameTransactionFrame.MODE_VERIFY,
            FrameTransactionFrame.APPROVAL_EXECUTION,
            Optional.empty(), // null target = sender
            30_000L,
            Wei.ZERO,
            Bytes.EMPTY);

    final FrameTransactionFrame senderFrame =
        new FrameTransactionFrame(
            FrameTransactionFrame.MODE_SENDER,
            FrameTransactionFrame.APPROVAL_NONE,
            Optional.of(TEST_TARGET),
            100_000L,
            Wei.of(1_000_000L),
            Bytes.fromHexString("0xcafebabe"));

    final FrameTransactionSignature sig =
        new FrameTransactionSignature(
            FrameTransactionSignature.SCHEME_P256,
            TEST_SENDER,
            Bytes.fromHexString("0x" + "ff".repeat(32)),
            Bytes.fromHexString("0x" + "cc".repeat(128)));

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.valueOf(1337))
            .nonce(42L)
            .sender(TEST_SENDER)
            .frames(List.of(verifyFrame, senderFrame))
            .frameSignatures(List.of(sig))
            .maxPriorityFeePerGas(Wei.of(500_000_000L))
            .maxFeePerGas(Wei.of(3_000_000_000L))
            .maxFeePerBlobGas(Wei.ZERO)
            .gasLimit(130_000L)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .build();

    // Encode
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeByte(TransactionType.FRAME.getSerializedType());
    FrameTransactionEncoder.encode(tx, out);
    final Bytes encoded = out.encoded();

    // Decode
    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.BLOCK_BODY);

    assertThat(decoded.getType()).isEqualTo(TransactionType.FRAME);
    assertThat(decoded.getChainId()).contains(BigInteger.valueOf(1337));
    assertThat(decoded.getNonce()).isEqualTo(42L);

    final List<FrameTransactionFrame> frames = decoded.getFrames().orElseThrow();
    assertThat(frames).hasSize(2);

    // First frame - VERIFY
    assertThat(frames.get(0).mode()).isEqualTo(FrameTransactionFrame.MODE_VERIFY);
    assertThat(frames.get(0).flags()).isEqualTo(FrameTransactionFrame.APPROVAL_EXECUTION);
    assertThat(frames.get(0).target()).isEmpty();
    assertThat(frames.get(0).gasLimit()).isEqualTo(30_000L);

    // Second frame - SENDER
    assertThat(frames.get(1).mode()).isEqualTo(FrameTransactionFrame.MODE_SENDER);
    assertThat(frames.get(1).target()).contains(TEST_TARGET);
    assertThat(frames.get(1).gasLimit()).isEqualTo(100_000L);
    assertThat(frames.get(1).value()).isEqualTo(Wei.of(1_000_000L));
    assertThat(frames.get(1).data()).isEqualTo(Bytes.fromHexString("0xcafebabe"));

    // Signature
    final List<FrameTransactionSignature> sigs = decoded.getFrameSignatures().orElseThrow();
    assertThat(sigs).hasSize(1);
    assertThat(sigs.get(0).scheme()).isEqualTo(FrameTransactionSignature.SCHEME_P256);
  }

  @Test
  void shouldEncodeFrameWithAtomicBatchFlag() {
    final FrameTransactionFrame frame1 =
        new FrameTransactionFrame(
            FrameTransactionFrame.MODE_SENDER,
            FrameTransactionFrame.ATOMIC_BATCH_FLAG | FrameTransactionFrame.APPROVAL_NONE,
            Optional.of(TEST_TARGET),
            50_000L,
            Wei.ZERO,
            Bytes.EMPTY);

    final FrameTransactionFrame frame2 =
        new FrameTransactionFrame(
            FrameTransactionFrame.MODE_SENDER,
            FrameTransactionFrame.APPROVAL_NONE,
            Optional.of(TEST_TARGET),
            50_000L,
            Wei.ZERO,
            Bytes.EMPTY);

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonce(0L)
            .sender(TEST_SENDER)
            .frames(List.of(frame1, frame2))
            .frameSignatures(
                List.of(
                    new FrameTransactionSignature(
                        FrameTransactionSignature.SCHEME_SECP256K1,
                        TEST_SENDER,
                        Bytes.EMPTY,
                        Bytes.fromHexString("0x" + "00".repeat(65)))))
            .maxPriorityFeePerGas(Wei.ZERO)
            .maxFeePerGas(Wei.of(1_000_000_000L))
            .maxFeePerBlobGas(Wei.ZERO)
            .gasLimit(100_000L)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .build();

    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeByte(TransactionType.FRAME.getSerializedType());
    FrameTransactionEncoder.encode(tx, out);
    final Bytes encoded = out.encoded();

    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.BLOCK_BODY);

    final List<FrameTransactionFrame> frames = decoded.getFrames().orElseThrow();
    assertThat(frames.get(0).isAtomicBatch()).isTrue();
    assertThat(frames.get(0).approvalScope()).isEqualTo(FrameTransactionFrame.APPROVAL_NONE);
    assertThat(frames.get(1).isAtomicBatch()).isFalse();
  }

  @Test
  void frameTransactionFrameHelperMethods() {
    final FrameTransactionFrame frame =
        new FrameTransactionFrame(
            FrameTransactionFrame.MODE_VERIFY,
            FrameTransactionFrame.ATOMIC_BATCH_FLAG | FrameTransactionFrame.APPROVAL_BOTH,
            Optional.empty(),
            10_000L,
            Wei.ZERO,
            Bytes.EMPTY);

    assertThat(frame.approvalScope()).isEqualTo(FrameTransactionFrame.APPROVAL_BOTH);
    assertThat(frame.isAtomicBatch()).isTrue();
    assertThat(frame.resolveTarget(TEST_SENDER)).isEqualTo(TEST_SENDER);

    final FrameTransactionFrame frameWithTarget =
        new FrameTransactionFrame(
            FrameTransactionFrame.MODE_DEFAULT,
            0,
            Optional.of(TEST_TARGET),
            10_000L,
            Wei.ZERO,
            Bytes.EMPTY);

    assertThat(frameWithTarget.resolveTarget(TEST_SENDER)).isEqualTo(TEST_TARGET);
  }

  @Test
  void frameTransactionSignatureGasCost() {
    final FrameTransactionSignature secp =
        new FrameTransactionSignature(
            FrameTransactionSignature.SCHEME_SECP256K1, TEST_SENDER, Bytes.EMPTY, Bytes.EMPTY);
    assertThat(secp.verificationGasCost()).isEqualTo(2800L);
    assertThat(secp.isCanonical()).isTrue();

    final FrameTransactionSignature p256 =
        new FrameTransactionSignature(
            FrameTransactionSignature.SCHEME_P256,
            TEST_SENDER,
            Bytes.fromHexString("0x" + "ab".repeat(32)),
            Bytes.EMPTY);
    assertThat(p256.verificationGasCost()).isEqualTo(6700L);
    assertThat(p256.isCanonical()).isFalse();
  }

  @Test
  void transactionTypeSupportsFrame() {
    assertThat(TransactionType.FRAME.supportsFrame()).isTrue();
    assertThat(TransactionType.FRAME.requiresChainId()).isTrue();
    assertThat(TransactionType.FRAME.supports1559FeeMarket()).isTrue();

    assertThat(TransactionType.FRONTIER.supportsFrame()).isFalse();
    assertThat(TransactionType.EIP1559.supportsFrame()).isFalse();
    assertThat(TransactionType.BLOB.supportsFrame()).isFalse();
    assertThat(TransactionType.DELEGATE_CODE.supportsFrame()).isFalse();
  }
}
