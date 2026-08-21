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
package org.hyperledger.besu.ethereum.core.encoding;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import com.google.common.collect.ImmutableMap;
import org.apache.tuweni.bytes.Bytes;

public class TransactionEncoder {

  @FunctionalInterface
  interface Encoder {
    void encode(Transaction transaction, RLPOutput output);
  }

  private static final ImmutableMap<TransactionType, Encoder> TYPED_TRANSACTION_ENCODERS =
      ImmutableMap.<TransactionType, Encoder>builder()
          .put(TransactionType.ACCESS_LIST, AccessListTransactionEncoder::encode)
          .put(TransactionType.EIP1559, EIP1559TransactionEncoder::encode)
          .put(TransactionType.BLOB, BlobTransactionEncoder::encode)
          .put(TransactionType.DELEGATE_CODE, CodeDelegationTransactionEncoder::encode)
          .put(TransactionType.FRAME, FrameTransactionEncoder::encode)
          .build();

  private static final ImmutableMap<TransactionType, Encoder> POOLED_TRANSACTION_ENCODERS =
      ImmutableMap.of(TransactionType.BLOB, BlobPooledTransactionEncoder::encode);

  /**
   * Encodes a transaction into RLP format.
   *
   * @param transaction the transaction to encode
   * @param rlpOutput the RLP output stream
   * @param encodingContext the encoding context
   */
  public static void encodeRLP(
      final Transaction transaction,
      final RLPOutput rlpOutput,
      final EncodingContext encodingContext) {
    final TransactionType transactionType = getTransactionType(transaction);

    Bytes opaqueBytes = encodeOpaqueBytes(transaction, encodingContext);
    encodeRLP(transactionType, opaqueBytes, rlpOutput);
  }

  /**
   * Encodes a transaction into RLP format.
   *
   * @param transactionType the type of the transaction
   * @param opaqueBytes the bytes of the transaction
   * @param rlpOutput the RLP output stream
   */
  public static void encodeRLP(
      final TransactionType transactionType, final Bytes opaqueBytes, final RLPOutput rlpOutput) {
    checkNotNull(transactionType, "Transaction type was not specified.");
    if (TransactionType.FRONTIER.equals(transactionType)) {
      rlpOutput.writeRaw(opaqueBytes);
    } else {
      rlpOutput.writeBytes(opaqueBytes);
    }
  }

  /**
   * Encodes a transaction into opaque bytes.
   *
   * @param transaction the transaction to encode
   * @param encodingContext the encoding context
   * @return the encoded transaction as bytes
   */
  public static Bytes encodeOpaqueBytes(
      final Transaction transaction, final EncodingContext encodingContext) {
    final TransactionType transactionType = getTransactionType(transaction);
    if (TransactionType.FRONTIER.equals(transactionType)) {
      return RLP.encode(rlpOutput -> FrontierTransactionEncoder.encode(transaction, rlpOutput));
    } else {
      final Encoder encoder = getEncoder(transactionType, encodingContext);
      final BytesValueRLPOutput out = new BytesValueRLPOutput();
      transaction
          .getRawRlp()
          .ifPresentOrElse(
              (rawRlp) ->
                  out.writeRLPBytes(
                      Bytes.concatenate(Bytes.of(transactionType.getSerializedType()), rawRlp)),
              () -> {
                out.writeByte(transaction.getType().getSerializedType());
                encoder.encode(transaction, out);
              });
      return out.encoded();
    }
  }

  static void writeSignatureAndV(final Transaction transaction, final RLPOutput out) {
    out.writeBigIntegerScalar(transaction.getV());
    writeSignature(transaction, out);
  }

  static void writeSignatureAndRecoveryId(final Transaction transaction, final RLPOutput out) {
    out.writeIntScalar(transaction.getSignature().getRecId());
    writeSignature(transaction, out);
  }

  static void writeSignature(final Transaction transaction, final RLPOutput out) {
    out.writeBigIntegerScalar(transaction.getSignature().getR());
    out.writeBigIntegerScalar(transaction.getSignature().getS());
  }

  private static TransactionType getTransactionType(final Transaction transaction) {
    return checkNotNull(
        transaction.getType(), "Transaction type for %s was not specified.", transaction);
  }

  private static Encoder getEncoder(
      final TransactionType transactionType, final EncodingContext encodingContext) {

    if (encodingContext.equals(EncodingContext.POOLED_TRANSACTION)) {
      if (POOLED_TRANSACTION_ENCODERS.containsKey(transactionType)) {
        return POOLED_TRANSACTION_ENCODERS.get(transactionType);
      }
    }
    return checkNotNull(
        TYPED_TRANSACTION_ENCODERS.get(transactionType),
        "Developer Error. A supported transaction type %s has no associated encoding logic",
        transactionType);
  }
}
