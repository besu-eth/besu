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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.FrameTransactionGas;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Round trips of EIP-8141 frame transactions through {@link FrameTransactionEncoder} and {@link
 * FrameTransactionDecoder}. The decoder is not registered for type 0x06 in {@link
 * TransactionDecoder} yet, so the round trips call it directly.
 */
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

    final Transaction decoded = FrameTransactionDecoder.decode(encoded);
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
    assertThat(decoded).isEqualTo(transaction);
    assertThat(decoded.hashCode()).isEqualTo(transaction.hashCode());
  }

  @Test
  void blockBodyAndPooledEncodingsAreTheSame() {
    final Transaction transaction = sampleTransaction();
    final Bytes blockBody =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);
    assertThat(
            TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION))
        .isEqualTo(blockBody);
    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(blockBody.size());
    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(blockBody.size());
  }

  @Test
  void hashIsStableAndCoversTheWholePayload() {
    final Transaction transaction = sampleTransaction();
    assertThat(transaction.getHash()).isEqualTo(sampleTransaction().getHash());

    final Transaction otherFee =
        Transaction.builder().copiedFrom(sampleTransaction()).maxFeePerGas(Wei.of(101)).build();
    assertThat(otherFee.getHash()).isNotEqualTo(transaction.getHash());

    // Unlike the signature hash, the transaction hash covers canonical-hash signature bytes.
    final Transaction otherSignatureBytes =
        Transaction.builder()
            .copiedFrom(transaction)
            .frameSignatures(
                List.of(
                    new FrameSignature(
                        FrameSignature.SCHEME_SECP256K1,
                        Bytes.EMPTY,
                        Bytes.EMPTY,
                        Bytes.repeat((byte) 1, 65)),
                    transaction.getFrameSignatures().orElseThrow().get(1)))
            .build();
    assertThat(otherSignatureBytes.getHash()).isNotEqualTo(transaction.getHash());
    assertThat(otherSignatureBytes.getFrameSignatureHash())
        .isEqualTo(transaction.getFrameSignatureHash());
  }

  @Test
  void keyedNonceFieldsRoundTripAndAreSigned() {
    final List<UInt256> keys = List.of(UInt256.valueOf(0xbeef), UInt256.valueOf(0xcafe));
    final Transaction keyed =
        Transaction.builder().copiedFrom(sampleTransaction()).nonceKeys(keys).build();
    final Bytes encoded = TransactionEncoder.encodeOpaqueBytes(keyed, EncodingContext.BLOCK_BODY);

    final Transaction decoded = FrameTransactionDecoder.decode(encoded);
    assertThat(decoded.getNonceKeys()).contains(keys);
    assertThat(decoded.getNonce()).isEqualTo(3);
    assertThat(TransactionEncoder.encodeOpaqueBytes(decoded, EncodingContext.BLOCK_BODY))
        .isEqualTo(encoded);
    // The key set is part of the signed payload.
    assertThat(keyed.getFrameSignatureHash())
        .isNotEqualTo(sampleTransaction().getFrameSignatureHash());
  }

  @Test
  void frameTransactionsDefaultToTheLegacyKeySet() {
    assertThat(sampleTransaction().getNonceKeys()).contains(List.of(UInt256.ZERO));
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
  void signatureHashIsOnlyDefinedForFrameTransactions() {
    final Transaction eip1559 =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(0)
            .gasLimit(21_000)
            .maxPriorityFeePerGas(Wei.of(1))
            .maxFeePerGas(Wei.of(2))
            .to(TARGET)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .sender(SENDER)
            .build();
    assertThatThrownBy(eip1559::getFrameSignatureHash)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Signature hash only defined for frame transactions");
  }

  @Test
  void gasLimitIsDerivedAsMaxGas() {
    final Transaction transaction = sampleTransaction();
    final long expected =
        FrameTransactionGas.maxGas(
            transaction.getFrames().orElseThrow(),
            transaction.getFrameSignatures().orElseThrow(),
            SENDER,
            transaction.getNonceKeys().orElseThrow(),
            transaction.getNonce());
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
    final Transaction decoded = FrameTransactionDecoder.decode(encoded);
    assertThat(decoded.getVersionedHashes().orElseThrow()).containsExactly(versionedHash);
    assertThat(decoded.getMaxFeePerBlobGas()).contains(Wei.of(1));
    assertThat(decoded.getBlobCount()).isEqualTo(1);
  }

  @Test
  void decodedBlobLessFrameTransactionCarriesAnEmptyVersionedHashList() {
    final Transaction decoded =
        FrameTransactionDecoder.decode(
            TransactionEncoder.encodeOpaqueBytes(sampleTransaction(), EncodingContext.BLOCK_BODY));
    assertThat(decoded.getVersionedHashes()).contains(List.of());
    assertThat(decoded.getBlobCount()).isZero();
  }

  @Test
  void rejectsAnOverWideNonceSequence() {
    final Bytes encoded =
        encodePayload(
            FrameTransactionCodecTest::writeLegacyNonceKeys,
            out -> out.writeBytes(Bytes.repeat((byte) 1, 9)),
            FrameTransactionCodecTest::writeSampleFees);
    assertThatThrownBy(() -> FrameTransactionDecoder.decode(encoded))
        .isInstanceOf(FrameFieldException.class)
        .hasMessage("nonce sequence exceeds 2^64-1");
  }

  @Test
  void rejectsAnOverWideNonceKey() {
    final Bytes encoded =
        encodePayload(
            out -> out.writeList(List.of(Bytes.repeat((byte) 1, 33)), (k, o) -> o.writeBytes(k)),
            FrameTransactionCodecTest::writeSampleNonceSeq,
            FrameTransactionCodecTest::writeSampleFees);
    assertThatThrownBy(() -> FrameTransactionDecoder.decode(encoded))
        .isInstanceOf(FrameFieldException.class)
        .hasMessage("nonce key exceeds 2^256-1");
  }

  @Test
  void rejectsAnOverWideFee() {
    final Bytes encoded =
        encodePayload(
            FrameTransactionCodecTest::writeLegacyNonceKeys,
            FrameTransactionCodecTest::writeSampleNonceSeq,
            out -> {
              out.startList();
              out.writeUInt256Scalar(Wei.of(2));
              out.writeBytes(Bytes.repeat((byte) 1, 33));
              out.writeUInt256Scalar(Wei.ZERO);
              out.endList();
            });
    assertThatThrownBy(() -> FrameTransactionDecoder.decode(encoded))
        .isInstanceOf(FrameFieldException.class)
        .hasMessage("max fee per gas exceeds 2^256-1");
  }

  @Test
  void rejectsAMalformedSender() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeByte(TransactionType.FRAME.getSerializedType());
    out.startList();
    out.writeBigIntegerScalar(BigInteger.ONE);
    writeLegacyNonceKeys(out);
    writeSampleNonceSeq(out);
    out.writeBytes(Bytes.repeat((byte) 1, 19));
    out.endList();
    assertThatThrownBy(() -> FrameTransactionDecoder.decode(out.encoded()))
        .isInstanceOf(RLPException.class)
        .isNotInstanceOf(FrameFieldException.class)
        .hasMessage("Frame transaction sender must be a 20-byte address");
  }

  @Test
  void rejectsAMalformedFrameTarget() {
    final Frame frame = sampleTransaction().getFrames().orElseThrow().get(1);
    final Bytes encoded =
        encodeWithFrame(
            out -> {
              out.startList();
              out.writeIntScalar(frame.mode());
              out.writeIntScalar(frame.flags());
              out.writeBytes(Bytes.repeat((byte) 2, 21));
              out.startList();
              out.writeLongScalar(frame.executionGasLimit());
              out.writeLongScalar(frame.stateGasLimit());
              out.endList();
              out.writeUInt256Scalar(frame.value());
              out.writeBytes(frame.data());
              out.endList();
            });
    assertThatThrownBy(() -> FrameTransactionDecoder.decode(encoded))
        .isInstanceOf(RLPException.class)
        .isNotInstanceOf(FrameFieldException.class)
        .hasMessage("Frame target must be empty or a 20-byte address");
  }

  @Test
  void rejectsAnOverWideFrameGasLimit() {
    final Bytes encoded =
        encodeWithFrame(
            out -> {
              out.startList();
              out.writeIntScalar(Frame.MODE_SENDER);
              out.writeIntScalar(0);
              out.writeBytes(TARGET.getBytes());
              out.startList();
              out.writeBytes(Bytes.repeat((byte) 1, 9));
              out.writeLongScalar(0);
              out.endList();
              out.writeUInt256Scalar(Wei.ZERO);
              out.writeBytes(Bytes.EMPTY);
              out.endList();
            });
    assertThatThrownBy(() -> FrameTransactionDecoder.decode(encoded))
        .isInstanceOf(FrameFieldException.class)
        .hasMessage("frame execution gas limit exceeds 2^64-1");
  }

  @Test
  void rejectsAFrameTransactionWithoutFrames() {
    final Bytes encoded = encodeWithFrame(out -> {});
    assertThatThrownBy(() -> FrameTransactionDecoder.decode(encoded))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Frame transaction must have at least one frame");
  }

  /** Encodes the sample transaction's frames with the given nonce and fee fields. */
  private Bytes encodePayload(
      final Consumer<RLPOutput> nonceKeysWriter,
      final Consumer<RLPOutput> nonceSeqWriter,
      final Consumer<RLPOutput> feesWriter) {
    final List<Frame> frames = sampleTransaction().getFrames().orElseThrow();
    return encode(
        nonceKeysWriter,
        nonceSeqWriter,
        out -> out.writeList(frames, FrameTransactionCodecTest::writeFrame),
        feesWriter);
  }

  /** Encodes a frame transaction whose frame list holds what {@code frameWriter} writes. */
  private Bytes encodeWithFrame(final Consumer<RLPOutput> frameWriter) {
    return encode(
        FrameTransactionCodecTest::writeLegacyNonceKeys,
        FrameTransactionCodecTest::writeSampleNonceSeq,
        out -> {
          out.startList();
          frameWriter.accept(out);
          out.endList();
        },
        FrameTransactionCodecTest::writeSampleFees);
  }

  private static Bytes encode(
      final Consumer<RLPOutput> nonceKeysWriter,
      final Consumer<RLPOutput> nonceSeqWriter,
      final Consumer<RLPOutput> framesWriter,
      final Consumer<RLPOutput> feesWriter) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeByte(TransactionType.FRAME.getSerializedType());
    out.startList();
    out.writeBigIntegerScalar(BigInteger.ONE);
    nonceKeysWriter.accept(out);
    nonceSeqWriter.accept(out);
    out.writeBytes(SENDER.getBytes());
    framesWriter.accept(out);
    out.writeEmptyList();
    feesWriter.accept(out);
    out.writeEmptyList();
    out.endList();
    return out.encoded();
  }

  private static void writeLegacyNonceKeys(final RLPOutput out) {
    out.writeList(List.of(UInt256.ZERO), (key, o) -> o.writeUInt256Scalar(key));
  }

  private static void writeSampleNonceSeq(final RLPOutput out) {
    out.writeLongScalar(3);
  }

  private static void writeSampleFees(final RLPOutput out) {
    out.startList();
    out.writeUInt256Scalar(Wei.of(2));
    out.writeUInt256Scalar(Wei.of(100));
    out.writeUInt256Scalar(Wei.ZERO);
    out.endList();
  }

  private static void writeFrame(final Frame frame, final RLPOutput out) {
    out.startList();
    out.writeIntScalar(frame.mode());
    out.writeIntScalar(frame.flags());
    out.writeBytes(frame.target().map(Address::getBytes).orElse(Bytes.EMPTY));
    out.startList();
    out.writeLongScalar(frame.executionGasLimit());
    out.writeLongScalar(frame.stateGasLimit());
    out.endList();
    out.writeUInt256Scalar(frame.value());
    out.writeBytes(frame.data());
    out.endList();
  }
}
