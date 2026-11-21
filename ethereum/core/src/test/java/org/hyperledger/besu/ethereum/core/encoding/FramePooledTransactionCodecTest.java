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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.datatypes.BlobType.KZG_CELL_PROOFS;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.Blob;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** EIP-8141 frame transactions in the network form that PooledTransactions carries. */
class FramePooledTransactionCodecTest extends TrustedSetupClassLoaderExtension {

  private static final Address SENDER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  @ParameterizedTest(name = "{0} blobs")
  @ValueSource(ints = {1, 3})
  void frameTransactionWithBlobsRoundTripsInItsWrapper(final int blobCount) {
    final Transaction transaction = frameTransactionWithBlobs(blobCount);
    final Bytes pooled =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION);
    final Bytes canonical =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(pooled, EncodingContext.POOLED_TRANSACTION);

    assertThat(isWrapped(pooled)).isTrue();
    assertThat(decoded.getBlobsWithCommitments()).isEqualTo(transaction.getBlobsWithCommitments());
    assertThat(decoded.getBlobsWithCommitments().orElseThrow().getBlobType())
        .isEqualTo(KZG_CELL_PROOFS);
    assertThat(decoded.getHash()).isEqualTo(transaction.getHash());
    assertThat(decoded.getSizeForAnnouncement()).isEqualTo(pooled.size());
    assertThat(decoded.getSizeForBlockInclusion()).isEqualTo(canonical.size());
    assertThat(TransactionEncoder.encodeOpaqueBytes(decoded, EncodingContext.POOLED_TRANSACTION))
        .isEqualTo(pooled);
    assertThat(TransactionEncoder.encodeOpaqueBytes(decoded, EncodingContext.BLOCK_BODY))
        .isEqualTo(canonical);
  }

  @Test
  void frameTransactionWithoutBlobsUsesThePlainPayload() {
    final Transaction transaction = frameTransaction(List.of()).build();
    final Bytes canonical =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    final Bytes pooled =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION);
    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(pooled, EncodingContext.POOLED_TRANSACTION);

    assertThat(pooled).isEqualTo(canonical);
    assertThat(decoded.getBlobsWithCommitments()).isEmpty();
    assertThat(decoded.getHash()).isEqualTo(transaction.getHash());
    assertThat(decoded.getSizeForAnnouncement()).isEqualTo(canonical.size());
  }

  @ParameterizedTest
  @EnumSource(EncodingContext.class)
  void frameTransactionWithoutBlobsIsEncodableInEveryContext(final EncodingContext context) {
    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(
            TransactionEncoder.encodeOpaqueBytes(
                frameTransaction(List.of()).build(), EncodingContext.POOLED_TRANSACTION),
            EncodingContext.POOLED_TRANSACTION);

    assertThat(context.canEncode(decoded)).isTrue();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidNetworkForms")
  void rejectsAnInvalidNetworkForm(
      final String name, final Bytes encoded, final String expectedMessage) {
    assertThatThrownBy(
            () -> TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.POOLED_TRANSACTION))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining(expectedMessage);
  }

  static Stream<Arguments> invalidNetworkForms() {
    final List<BlobProofBundle> bundles = blobProofBundles(2);
    final Transaction body = frameTransaction(versionedHashesOf(bundles)).build();
    final List<Blob> blobs =
        bundles.stream().map(bundle -> bundle.getBlob().orElseThrow()).toList();
    final List<KZGCommitment> commitments =
        bundles.stream().map(BlobProofBundle::getKzgCommitment).toList();
    final List<KZGProof> proofs =
        bundles.stream().flatMap(bundle -> bundle.getKzgProof().stream()).toList();
    final Transaction bodyWithoutHashes = frameTransaction(List.of()).build();

    return Stream.of(
        Arguments.of(
            "commitments do not match the versioned hashes",
            wrapped(body, 1, blobs, commitments.reversed(), proofs),
            "Commitment at index 0 does not match versioned hash"),
        Arguments.of(
            "one blob fewer than versioned hashes",
            wrapped(body, 1, blobs.subList(0, 1), commitments, proofs),
            "Invalid number of kzgCommitments, expected 1, got 2"),
        Arguments.of(
            "one commitment fewer than versioned hashes",
            wrapped(body, 1, blobs, commitments.subList(0, 1), proofs),
            "Invalid number of kzgCommitments, expected 2, got 1"),
        Arguments.of(
            "one cell proof missing",
            wrapped(body, 1, blobs, commitments, proofs.subList(1, proofs.size())),
            "Invalid number of proofs"),
        Arguments.of(
            "unknown wrapper version",
            wrapped(body, 2, blobs, commitments, proofs),
            "No BlobType found for version ID: 2"),
        Arguments.of(
            "wrapped without versioned hashes or blobs",
            wrapped(bodyWithoutHashes, 1, List.of(), List.of(), List.of()),
            "minimum of one blob"),
        Arguments.of(
            "wrapped without versioned hashes but with blobs",
            wrapped(
                bodyWithoutHashes,
                1,
                blobs.subList(0, 1),
                commitments.subList(0, 1),
                proofs.subList(0, proofs.size() / 2)),
            "Invalid number of versionedHashes, expected 1, got 0"),
        Arguments.of(
            "versioned hashes without the sidecar",
            TransactionEncoder.encodeOpaqueBytes(body, EncodingContext.BLOCK_BODY),
            "missing its blob sidecar"));
  }

  private static Transaction frameTransactionWithBlobs(final int blobCount) {
    final List<BlobProofBundle> bundles = blobProofBundles(blobCount);
    return frameTransaction(versionedHashesOf(bundles))
        .blobsWithCommitments(BlobsWithCommitments.createFromBundles(bundles))
        .build();
  }

  private static List<BlobProofBundle> blobProofBundles(final int blobCount) {
    final BlobTestFixture blobTestFixture = new BlobTestFixture();
    return IntStream.range(0, blobCount)
        .mapToObj(unused -> blobTestFixture.createBlobProofBundle(KZG_CELL_PROOFS))
        .toList();
  }

  private static List<VersionedHash> versionedHashesOf(final List<BlobProofBundle> bundles) {
    return bundles.stream().map(BlobProofBundle::getVersionedHash).toList();
  }

  private static Transaction.Builder frameTransaction(final List<VersionedHash> versionedHashes) {
    return Transaction.builder()
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
        .maxFee(Wei.of(1_000_000))
        .versionedHashes(versionedHashes);
  }

  /**
   * Encodes {@code type || rlp([tx_payload_body, wrapper_version, blobs, commitments,
   * cell_proofs])} from parts that need not agree with each other.
   */
  private static Bytes wrapped(
      final Transaction body,
      final int wrapperVersion,
      final List<Blob> blobs,
      final List<KZGCommitment> commitments,
      final List<KZGProof> proofs) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    FrameTransactionEncoder.encode(body, out);
    out.writeIntScalar(wrapperVersion);
    out.writeList(blobs, Blob::writeTo);
    out.writeList(commitments, KZGCommitment::writeTo);
    out.writeList(proofs, KZGProof::writeTo);
    out.endList();
    return Bytes.concatenate(Bytes.of(TransactionType.FRAME.getSerializedType()), out.encoded());
  }

  private static boolean isWrapped(final Bytes pooled) {
    final RLPInput input = RLP.input(pooled.slice(1));
    input.enterList();
    return input.nextIsList();
  }
}
