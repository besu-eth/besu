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
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class TransactionDecoder {

  @FunctionalInterface
  interface Decoder {
    Transaction decode(Bytes input);
  }

  private static final EnumMap<TransactionType, Decoder> TRANSACTION_DECODERS =
      new EnumMap<>(
          Map.of(
              TransactionType.FRONTIER, FrontierTransactionDecoder::decode,
              TransactionType.ACCESS_LIST, AccessListTransactionDecoder::decode,
              TransactionType.EIP1559, EIP1559TransactionDecoder::decode,
              TransactionType.BLOB, BlobTransactionDecoder::decode,
              TransactionType.DELEGATE_CODE, CodeDelegationTransactionDecoder::decode,
              TransactionType.FRAME, FrameTransactionDecoder::decode));

  private static final EnumMap<TransactionType, Decoder> POOLED_TRANSACTION_DECODERS =
      new EnumMap<>(Map.of(TransactionType.BLOB, BlobPooledTransactionDecoder::decode));

  /**
   * Decodes an RLP input into a transaction. If the input represents a typed transaction, it uses
   * the appropriate decoder for that type. Otherwise, it uses the frontier decoder.
   *
   * @param rlpInput the RLP input
   * @return the decoded transaction
   */
  public static Transaction decodeRLP(
      final RLPInput rlpInput, final EncodingContext encodingContext) {
    if (isTypedTransaction(rlpInput)) {
      return decodeTransaction(rlpInput, encodingContext);
    } else {
      return FrontierTransactionDecoder.decode(rlpInput.readAsRlp());
    }
  }

  /**
   * Decodes a typed transaction from an RLP input. It first reads the transaction type from the
   * input, then uses the appropriate decoder for that type.
   *
   * @param rlpInput the RLP input
   * @return the decoded transaction
   */
  private static Transaction decodeTransaction(
      final RLPInput rlpInput, final EncodingContext context) {
    // Read the typed transaction bytes from the RLP input
    final Bytes typedTransactionBytes = rlpInput.readBytes();
    if (typedTransactionBytes.isEmpty()) {
      throw new IllegalArgumentException("Invalid RLP encoding: empty transaction bytes");
    }

    // Determine the transaction type from the typed transaction bytes
    TransactionType transactionType =
        getTransactionType(typedTransactionBytes)
            .orElseThrow((() -> new IllegalArgumentException("Unsupported transaction type")));
    return decodeTransaction(typedTransactionBytes, transactionType, context);
  }

  /**
   * Decodes a typed transaction. The method first slices the transaction bytes to exclude the
   * transaction type, then uses the appropriate decoder for the transaction type to decode the
   * remaining bytes.
   *
   * @param transactionBytes the transaction bytes
   * @param transactionType the type of the transaction
   * @param context the encoding context
   * @return the decoded transaction
   */
  private static Transaction decodeTransaction(
      final Bytes transactionBytes,
      final TransactionType transactionType,
      final EncodingContext context) {
    // Slice the transaction bytes to exclude the transaction type and prepare for decoding
    // Use the appropriate decoder for the transaction type to decode the remaining bytes
    return getDecoder(transactionType, context).decode(transactionBytes);
  }

  /**
   * Decodes a transaction from opaque bytes. The method first determines the transaction type from
   * the bytes. If the type is present, it delegates the decoding process to the appropriate decoder
   * for that type. If the type is not present, it decodes the bytes as an RLP input.
   *
   * @param opaqueBytes the opaque bytes
   * @param context the encoding context
   * @return the decoded transaction
   */
  public static Transaction decodeOpaqueBytes(
      final Bytes opaqueBytes, final EncodingContext context) {
    if (opaqueBytes.isEmpty()) {
      throw new IllegalArgumentException("Empty opaque bytes");
    }
    var transactionType = getTransactionType(opaqueBytes);
    if (transactionType.isPresent()) {
      return decodeTransaction(opaqueBytes, transactionType.get(), context);
    } else {
      // If the transaction type is not present, decode the opaque bytes as RLP
      return decodeRLP(RLP.input(opaqueBytes), context);
    }
  }

  /**
   * Retrieves the transaction type from the provided bytes. The method attempts to extract the
   * first byte from the input bytes and interpret it as a transaction type. If the byte does not
   * correspond to a valid transaction type, the method returns an empty Optional.
   *
   * @param opaqueBytes the bytes from which to extract the transaction type
   * @return an Optional containing the TransactionType if the first byte of the input corresponds
   *     to a valid transaction type, or an empty Optional if it does not
   */
  private static Optional<TransactionType> getTransactionType(final Bytes opaqueBytes) {
    byte transactionTypeByte = opaqueBytes.get(0);
    return TransactionType.fromOpaque(transactionTypeByte);
  }

  /**
   * Checks if the given RLP input is a typed transaction.
   *
   * <p>See EIP-2718
   *
   * <p>If it starts with a value in the range [0, 0x7f] then it is a new transaction type
   *
   * <p>if it starts with a value in the range [0xc0, 0xfe] then it is a legacy transaction type
   *
   * @param rlpInput the RLP input
   * @return true if the RLP input is a typed transaction, false otherwise
   */
  private static boolean isTypedTransaction(final RLPInput rlpInput) {
    return !rlpInput.nextIsList();
  }

  /**
   * Gets the decoder for a given transaction type and encoding context. If the context is
   * POOLED_TRANSACTION, it uses the network decoder for the type. Otherwise, it uses the typed
   * decoder.
   *
   * @param transactionType the transaction type
   * @param encodingContext the encoding context
   * @return the decoder
   */
  private static Decoder getDecoder(
      final TransactionType transactionType, final EncodingContext encodingContext) {
    if (encodingContext.equals(EncodingContext.POOLED_TRANSACTION)) {
      if (POOLED_TRANSACTION_DECODERS.containsKey(transactionType)) {
        return POOLED_TRANSACTION_DECODERS.get(transactionType);
      }
    }
    return checkNotNull(
        TRANSACTION_DECODERS.get(transactionType),
        "Developer Error. A supported transaction type %s has no associated decoding logic",
        transactionType);
  }
}
