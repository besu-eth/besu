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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.KeyedNonces;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** The EIP-8141 and EIP-8250 fields of {@link Transaction}: construction rules and accessors. */
class TransactionFrameFieldsTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair KEYS = SIGNATURE_ALGORITHM.generateKeyPair();

  private static final Address SENDER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address OTHER_SENDER =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address TARGET =
      Address.fromHexString("0x2222222222222222222222222222222222222222");

  private static final Frame VERIFY_FRAME =
      new Frame(
          Frame.MODE_VERIFY,
          Frame.APPROVE_SCOPE_MASK,
          Optional.empty(),
          50_000L,
          10_000L,
          Wei.ZERO,
          Bytes.EMPTY);
  private static final Frame SENDER_FRAME =
      new Frame(
          Frame.MODE_SENDER,
          0,
          Optional.of(TARGET),
          75_000L,
          0L,
          Wei.of(42),
          Bytes.fromHexString("0xdeadbeef"));
  private static final FrameSignature SIGNATURE_ENTRY =
      new FrameSignature(
          FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, Bytes.repeat((byte) 7, 65));

  private static Transaction.Builder frameBuilder() {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(3)
        .sender(SENDER)
        .frames(List.of(VERIFY_FRAME, SENDER_FRAME))
        .frameSignatures(List.of(SIGNATURE_ENTRY))
        .maxPriorityFeePerGas(Wei.of(2))
        .maxFeePerGas(Wei.of(100));
  }

  private static Transaction.Builder nonFrameBuilder(final TransactionType type) {
    final Transaction.Builder builder =
        Transaction.builder()
            .type(type)
            .nonce(0)
            .gasLimit(21_000)
            .to(TARGET)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .chainId(BigInteger.ONE)
            .sender(SENDER);
    switch (type) {
      case FRONTIER -> builder.gasPrice(Wei.ONE);
      case ACCESS_LIST -> builder.gasPrice(Wei.ONE).accessList(List.of());
      case EIP1559 -> builder.maxPriorityFeePerGas(Wei.ONE).maxFeePerGas(Wei.ONE);
      case BLOB ->
          builder
              .maxPriorityFeePerGas(Wei.ONE)
              .maxFeePerGas(Wei.ONE)
              .maxFeePerBlobGas(Wei.ONE)
              .versionedHashes(List.of(VersionedHash.DEFAULT_VERSIONED_HASH));
      case DELEGATE_CODE ->
          builder.maxPriorityFeePerGas(Wei.ONE).maxFeePerGas(Wei.ONE).codeDelegations(List.of());
      case FRAME -> throw new IllegalArgumentException("not a non-frame type");
    }
    return builder;
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(
      value = TransactionType.class,
      names = {"FRAME"},
      mode = EnumSource.Mode.EXCLUDE)
  void nonFrameTypesRejectFrameFields(final TransactionType type) {
    assertThat(nonFrameBuilder(type).signAndBuild(KEYS).getFrames()).isEmpty();

    assertThatThrownBy(() -> nonFrameBuilder(type).frames(List.of(VERIFY_FRAME)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must not specify frames for transaction not supporting them");
    assertThatThrownBy(
            () -> nonFrameBuilder(type).frameSignatures(List.of(SIGNATURE_ENTRY)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must not specify frames for transaction not supporting them");
    assertThatThrownBy(() -> nonFrameBuilder(type).nonceKeys(KeyedNonces.LEGACY_NONCE_KEYS).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must not specify frames for transaction not supporting them");
  }

  @Test
  void frameTransactionRequiresASender() {
    assertThatThrownBy(() -> frameBuilder().sender(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must specify the sender of a frame transaction");
  }

  @Test
  void frameTransactionForbidsAnOuterSignature() {
    final Transaction signed = nonFrameBuilder(TransactionType.EIP1559).signAndBuild(KEYS);
    assertThatThrownBy(() -> frameBuilder().signature(signed.getSignature()).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must not specify a signature for a frame transaction");
  }

  @Test
  void frameTransactionNeedsAtLeastOneFrame() {
    assertThatThrownBy(() -> frameBuilder().frames(List.of()).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Frame transaction must have at least one frame");
    assertThatThrownBy(() -> frameBuilder().frames(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Frame transaction must have at least one frame");
  }

  @Test
  void blobTransactionStillRequiresVersionedHashesButFrameTransactionDoesNot() {
    assertThatThrownBy(() -> nonFrameBuilder(TransactionType.BLOB).versionedHashes(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must specify blob versioned hashes for blob transaction");

    final Transaction blobLess = frameBuilder().build();
    assertThat(blobLess.getVersionedHashes()).isEmpty();
    assertThat(blobLess.getBlobCount()).isZero();
    assertThat(blobLess.getType().supportsBlob()).isTrue();
  }

  @Test
  void builderFillsFrameDefaults() {
    final Transaction transaction = frameBuilder().build();
    assertThat(transaction.getNonceKeys()).contains(KeyedNonces.LEGACY_NONCE_KEYS);
    assertThat(transaction.getValue()).isEqualTo(Wei.ZERO);
    assertThat(transaction.getPayload()).isEqualTo(Bytes.EMPTY);
    assertThat(transaction.getMaxFeePerBlobGas()).contains(Wei.ZERO);
    assertThat(transaction.getTo()).isEmpty();
    assertThat(transaction.getGasLimit())
        .isEqualTo(
            FrameTransactionGas.maxGas(
                List.of(VERIFY_FRAME, SENDER_FRAME),
                List.of(SIGNATURE_ENTRY),
                SENDER,
                KeyedNonces.LEGACY_NONCE_KEYS,
                3));
  }

  @Test
  void builderKeepsAnExplicitGasLimitAndKeySet() {
    final List<UInt256> keys = List.of(UInt256.ONE, UInt256.valueOf(2));
    final Transaction transaction = frameBuilder().gasLimit(1_234_567L).nonceKeys(keys).build();
    assertThat(transaction.getGasLimit()).isEqualTo(1_234_567L);
    assertThat(transaction.getNonceKeys()).contains(keys);
  }

  @Test
  void frameTransactionIsUnsigned() {
    final Transaction transaction = frameBuilder().build();
    assertThat(transaction.getSignature()).isNull();
    assertThat(transaction.getR()).isNull();
    assertThat(transaction.getS()).isNull();
    assertThat(transaction.getV()).isNull();
    assertThat(transaction.getYParity()).isNull();
    assertThat(transaction.getSender()).isEqualTo(SENDER);
  }

  @Test
  void frameTransactionIsNotAContractCreation() {
    assertThat(frameBuilder().build().isContractCreation()).isFalse();
    assertThat(
            nonFrameBuilder(TransactionType.EIP1559)
                .to(null)
                .signAndBuild(KEYS)
                .isContractCreation())
        .isTrue();
  }

  @Test
  void copiedFromKeepsFramesSignatureEntriesAndKeys() {
    final List<UInt256> keys = List.of(UInt256.ONE);
    final Transaction original = frameBuilder().nonceKeys(keys).build();
    final Transaction copy = Transaction.builder().copiedFrom(original).build();
    assertThat(copy.getFrames()).isEqualTo(original.getFrames());
    assertThat(copy.getFrameSignatures()).isEqualTo(original.getFrameSignatures());
    assertThat(copy.getNonceKeys()).contains(keys);
    assertThat(copy.getSender()).isEqualTo(SENDER);
    assertThat(copy).isEqualTo(original);
    assertThat(copy.getHash()).isEqualTo(original.getHash());
  }

  @Test
  void detachedCopyKeepsFramesSignatureEntriesAndKeysWithoutSharingBytes() {
    final MutableBytes data = Bytes.fromHexString("0xdeadbeef").mutableCopy();
    final Frame frame =
        new Frame(Frame.MODE_SENDER, 0, Optional.of(TARGET), 1L, 0L, Wei.ZERO, data);
    final MutableBytes signatureBytes = Bytes.of(1, 2, 3).mutableCopy();
    final FrameSignature entry =
        new FrameSignature(
            FrameSignature.SCHEME_ARBITRARY, Bytes.EMPTY, Bytes.EMPTY, signatureBytes);
    final List<UInt256> keys = List.of(UInt256.valueOf(7));
    final Transaction original =
        frameBuilder()
            .frames(List.of(frame))
            .frameSignatures(List.of(entry))
            .nonceKeys(keys)
            .build();

    final Transaction detached = original.detachedCopy();
    assertThat(detached.getFrames()).isEqualTo(original.getFrames());
    assertThat(detached.getFrameSignatures()).isEqualTo(original.getFrameSignatures());
    assertThat(detached.getNonceKeys()).contains(keys);
    assertThat(detached.getSender()).isEqualTo(SENDER);
    assertThat(detached).isEqualTo(original);

    // Mutating the source buffers does not reach the detached copy.
    data.set(0, (byte) 0);
    signatureBytes.set(0, (byte) 0);
    assertThat(detached.getFrames().orElseThrow().get(0).data())
        .isEqualTo(Bytes.fromHexString("0xdeadbeef"));
    assertThat(detached.getFrameSignatures().orElseThrow().get(0).signature())
        .isEqualTo(Bytes.of(1, 2, 3));
  }

  @Test
  void frameTransactionsDifferingOnlyInSenderAreNotEqual() {
    final Transaction a = frameBuilder().build();
    final Transaction b = frameBuilder().sender(OTHER_SENDER).gasLimit(a.getGasLimit()).build();
    assertThat(a).isNotEqualTo(b);
    assertThat(b).isNotEqualTo(a);
  }

  @Test
  void frameTransactionsDifferingOnlyInFramesAreNotEqual() {
    final Transaction a = frameBuilder().build();
    final Frame otherData =
        new Frame(
            SENDER_FRAME.mode(),
            SENDER_FRAME.flags(),
            SENDER_FRAME.target(),
            SENDER_FRAME.executionGasLimit(),
            SENDER_FRAME.stateGasLimit(),
            SENDER_FRAME.value(),
            Bytes.fromHexString("0xdeadbeee"));
    final Transaction b =
        frameBuilder().frames(List.of(VERIFY_FRAME, otherData)).gasLimit(a.getGasLimit()).build();
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void frameTransactionsDifferingOnlyInSignatureEntriesAreNotEqual() {
    final Transaction a = frameBuilder().build();
    final Transaction b =
        frameBuilder()
            .frameSignatures(
                List.of(
                    new FrameSignature(
                        FrameSignature.SCHEME_SECP256K1,
                        Bytes.EMPTY,
                        Bytes.EMPTY,
                        Bytes.repeat((byte) 8, 65))))
            .gasLimit(a.getGasLimit())
            .build();
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void frameTransactionsDifferingOnlyInNonceKeysAreNotEqual() {
    final Transaction a =
        frameBuilder().nonceKeys(List.of(UInt256.ONE)).gasLimit(1_000_000L).build();
    final Transaction b =
        frameBuilder().nonceKeys(List.of(UInt256.valueOf(2))).gasLimit(1_000_000L).build();
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equalFrameTransactionsHaveEqualHashCodes() {
    final Transaction a = frameBuilder().build();
    final Transaction b = frameBuilder().build();
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void signedTransactionEqualityDoesNotDependOnSenderRecovery() {
    final Transaction signed = nonFrameBuilder(TransactionType.EIP1559).signAndBuild(KEYS);
    final Transaction recovered = Transaction.builder().copiedFrom(signed).sender(null).build();
    final Transaction unrecovered = Transaction.builder().copiedFrom(signed).sender(null).build();
    recovered.getSender();

    assertThat(recovered).isEqualTo(unrecovered);
    assertThat(unrecovered).isEqualTo(recovered);
    assertThat(recovered.hashCode()).isEqualTo(unrecovered.hashCode());
  }
}
