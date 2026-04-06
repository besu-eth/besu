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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.core.kzg.Blob;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;

import java.math.BigInteger;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.Bytes48;
import org.junit.jupiter.api.Test;

public class TransactionSizeAndHashTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  // Fake signature for transactions to not fail being processed.
  private static final SECPSignature FAKE_SIGNATURE =
      SIGNATURE_ALGORITHM.createSignature(
          SIGNATURE_ALGORITHM.getHalfCurveOrder(),
          SIGNATURE_ALGORITHM.getHalfCurveOrder(),
          (byte) 0);

  @Test
  public void returnsRightSizeForFrontierTx() {
    final Transaction transaction = buildFrontierTx();
    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytes.size());

    assertThat(transaction.getHash()).isEqualTo(Hash.hash(bytes));

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.BLOCK_BODY);

    assertThat(transactionRead.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytes.size());

    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytes));
  }

  @Test
  public void returnsRightSizeForEip1559Tx() {
    final Transaction transaction = build1559Transaction();
    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytes.size());

    assertThat(transaction.getHash()).isEqualTo(Hash.hash(bytes));

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.BLOCK_BODY);

    assertThat(transactionRead.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytes.size());

    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytes));
  }

  @Test
  public void returnsRightSizeForCodeDelegationTx() {
    final Transaction transaction = buildCodeDelegationTx();
    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytes.size());

    assertThat(transaction.getHash()).isEqualTo(Hash.hash(bytes));

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.BLOCK_BODY);

    assertThat(transactionRead.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytes.size());

    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytes));
  }

  @Test
  public void returnsRightSizeForAccessListTx() {
    final Transaction transaction = buildAccessListTx();
    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytes.size());

    assertThat(transaction.getHash()).isEqualTo(Hash.hash(bytes));

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.BLOCK_BODY);

    assertThat(transactionRead.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytes.size());

    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytes));
  }

  @Test
  public void returnsRightSizeForBlobTxWithoutBlobs() {
    final Transaction transaction = buildBlobTransactionNoBlobs();
    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    // Blob transactions without blobs cannot be announced
    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytes.size());

    assertThat(transaction.getHash()).isEqualTo(Hash.hash(bytes));

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.BLOCK_BODY);

    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytes.size());

    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytes));
  }

  @Test
  public void returnsRightSizeWithBlobsGetOrderAnnmBlockHash() {
    final Transaction transaction = buildBlobTransaction();

    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION);
    final Bytes bytesBB =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(bytes.size());

    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());
    assertThat(transaction.getHash()).isNotEqualTo(Hash.hash(bytes));

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.POOLED_TRANSACTION);

    assertThat(transactionRead.getSizeForAnnouncement()).isEqualTo(bytes.size());

    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());
    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytesBB));
  }

  @Test
  public void returnsRightSizeWithBlobsAnnmHashBlock() {
    final Transaction transaction = buildBlobTransaction();

    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION);
    final Bytes bytesBB =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(bytes.size());

    assertThat(transaction.getHash()).isNotEqualTo(Hash.hash(bytes));
    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.POOLED_TRANSACTION);

    assertThat(transactionRead.getSizeForAnnouncement()).isEqualTo(bytes.size());

    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytesBB));
    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());
  }

  @Test
  public void returnsRightSizeWithBlobsHashAnnmBlock() {
    final Transaction transaction = buildBlobTransaction();

    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION);
    final Bytes bytesBB =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    assertThat(transaction.getHash()).isEqualTo(Hash.hash(bytesBB));

    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.POOLED_TRANSACTION);

    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytesBB));

    assertThat(transactionRead.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());
  }

  @Test
  public void returnsRightSizeWithBlobsHashBlockAnnm() {
    final Transaction transaction = buildBlobTransaction();

    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION);
    final Bytes bytesBB =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    assertThat(transaction.getHash()).isEqualTo(Hash.hash(bytesBB));

    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());
    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(bytes.size());

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.POOLED_TRANSACTION);

    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytesBB));

    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());
    assertThat(transactionRead.getSizeForAnnouncement()).isEqualTo(bytes.size());
  }

  @Test
  public void returnsRightSizeWithBlobsBlockHashAnnm() {
    final Transaction transaction = buildBlobTransaction();

    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION);
    final Bytes bytesBB =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());

    assertThat(transaction.getHash()).isEqualTo(Hash.hash(bytesBB));
    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(bytes.size());

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.POOLED_TRANSACTION);

    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());

    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytesBB));
    assertThat(transactionRead.getSizeForAnnouncement()).isEqualTo(bytes.size());
  }

  @Test
  public void returnsRightSizeWithBlobsBlockAnnmHash() {
    final Transaction transaction = buildBlobTransaction();

    final Bytes bytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION);
    final Bytes bytesBB =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);

    assertThat(transaction.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());

    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transaction.getHash()).isEqualTo(Hash.hash(bytesBB));

    final Transaction transactionRead =
        TransactionDecoder.decodeOpaqueBytes(bytes, EncodingContext.POOLED_TRANSACTION);

    assertThat(transactionRead.getSizeForBlockInclusion()).isEqualTo(bytesBB.size());

    assertThat(transactionRead.getSizeForAnnouncement()).isEqualTo(bytes.size());
    assertThat(transactionRead.getHash()).isEqualTo(Hash.hash(bytesBB));
  }

  private static Transaction buildFrontierTx() {
    return Transaction.builder()
        .type(TransactionType.FRONTIER)
        .chainId(BigInteger.ONE)
        .nonce(1L)
        .gasLimit(2000)
        .gasPrice(Wei.of(500L))
        .to(Address.ECREC)
        .sender(Address.fromHexString("0x2000"))
        .value(Wei.of(2000L))
        .payload(Bytes.fromHexString("0x12345678"))
        .signature(FAKE_SIGNATURE)
        .build();
  }

  private static Transaction buildBlobTransaction() {
    return Transaction.builder()
        .type(TransactionType.BLOB)
        .chainId(BigInteger.ONE)
        .nonce(1L)
        .gasLimit(2000)
        .maxPriorityFeePerGas(Wei.of(100L))
        .maxFeePerGas(Wei.of(200L))
        .gasLimit(300L)
        .to(Address.ECREC)
        .sender(Address.fromHexString("0x2000"))
        .value(Wei.of(2000L))
        .payload(Bytes.fromHexString("0x12345678"))
        .maxFeePerBlobGas(Wei.of(250L))
        .versionedHashes(
            List.of(
                new VersionedHash(
                    Bytes32.fromHexString(
                        "0x0122334455667788991011121314151617181920212223242526272829303101"))))
        .blobsWithCommitments(
            new BlobsWithCommitments(
                BlobType.KZG_PROOF,
                List.of(new KZGCommitment(Bytes48.fromHexStringLenient("0x0987"))),
                List.of(new Blob(Bytes.fromHexString("0x0987"))),
                List.of(new KZGProof(Bytes48.fromHexStringLenient("0x1234"))),
                List.of(
                    new VersionedHash(
                        Bytes32.fromHexStringLenient(
                            "0x0122334455667788991011121314151617181920212223242526272829303101")))))
        .signature(FAKE_SIGNATURE)
        .build();
  }

  private static Transaction buildBlobTransactionNoBlobs() {
    return Transaction.builder()
        .type(TransactionType.BLOB)
        .chainId(BigInteger.ONE)
        .nonce(1L)
        .gasLimit(2000)
        .maxPriorityFeePerGas(Wei.of(100L))
        .maxFeePerGas(Wei.of(200L))
        .gasLimit(300L)
        .to(Address.ECREC)
        .sender(Address.fromHexString("0x2000"))
        .value(Wei.of(2000L))
        .payload(Bytes.fromHexString("0x12345678"))
        .maxFeePerBlobGas(Wei.of(250L))
        .versionedHashes(
            List.of(
                new VersionedHash(
                    Bytes32.fromHexString(
                        "0x0122334455667788991011121314151617181920212223242526272829303101"))))
        .signature(FAKE_SIGNATURE)
        .build();
  }

  private static Transaction build1559Transaction() {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .chainId(BigInteger.ONE)
        .nonce(1L)
        .gasLimit(2000)
        .maxPriorityFeePerGas(Wei.of(100L))
        .maxFeePerGas(Wei.of(200L))
        .to(Address.ECREC)
        .sender(Address.fromHexString("0x2000"))
        .value(Wei.of(2000L))
        .payload(Bytes.fromHexString("0x12345678"))
        .signature(FAKE_SIGNATURE)
        .build();
  }

  private static Transaction buildCodeDelegationTx() {
    return Transaction.builder()
        .type(TransactionType.DELEGATE_CODE)
        .chainId(BigInteger.ONE)
        .nonce(1L)
        .gasLimit(2000)
        .maxPriorityFeePerGas(Wei.of(100L))
        .maxFeePerGas(Wei.of(200L))
        .to(Address.ECREC)
        .sender(Address.fromHexString("0x2000"))
        .value(Wei.of(2000L))
        .payload(Bytes.fromHexString("0x12345678"))
        .signature(FAKE_SIGNATURE)
        .codeDelegations(
            List.of(new CodeDelegation(BigInteger.ONE, Address.ECREC, 1L, FAKE_SIGNATURE)))
        .build();
  }

  private static Transaction buildAccessListTx() {
    return Transaction.builder()
        .type(TransactionType.ACCESS_LIST)
        .chainId(BigInteger.ONE)
        .nonce(1L)
        .gasPrice(Wei.of(100L))
        .gasLimit(2000)
        .to(Address.ECREC)
        .sender(Address.fromHexString("0x2000"))
        .value(Wei.of(2000L))
        .payload(Bytes.fromHexString("0x12345678"))
        .signature(FAKE_SIGNATURE)
        .accessList(
            List.of(
                new AccessListEntry(
                    (Address.ALTBN128_ADD), List.of(Bytes32.fromHexString("0x01")))))
        .build();
  }
}
