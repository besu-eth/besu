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

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ChainId;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class CodeDelegationTransactionDecoder {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  private CodeDelegationTransactionDecoder() {
    // private constructor
  }

  public static Transaction decode(final Bytes input) {
    return decode(input, CodeDelegationListLimit.UNLIMITED);
  }

  /**
   * Decodes a code delegation transaction, refusing an authorization list longer than the limit
   * allows for the transaction's gas limit. The length is checked before any entry is decoded.
   *
   * @param input the transaction bytes, starting with the transaction type
   * @param codeDelegationListLimit the longest authorization list to decode
   * @return the decoded transaction
   * @throws org.hyperledger.besu.ethereum.rlp.RLPException if the input is malformed, or if the
   *     authorization list is longer than the limit
   */
  public static Transaction decode(
      final Bytes input, final CodeDelegationListLimit codeDelegationListLimit) {
    final RLPInput txRlp = RLP.input(input.slice(1)); // Skip the transaction type byte
    txRlp.enterList();
    final ChainId chainId = ChainId.readFrom(txRlp, Bytes32.SIZE);
    final Transaction.Builder builder =
        Transaction.builder()
            .type(TransactionType.DELEGATE_CODE)
            .chainId(chainId)
            .nonce(txRlp.readLongScalar())
            .maxPriorityFeePerGas(Wei.of(txRlp.readUInt256Scalar()))
            .maxFeePerGas(Wei.of(txRlp.readUInt256Scalar()));
    // The gas limit comes before the authorization list, so the list can be bounded by it.
    final long gasLimit = txRlp.readLongScalar();
    builder
        .gasLimit(gasLimit)
        .to(txRlp.readBytes(v -> v.isEmpty() ? null : Address.wrap(v)))
        .value(Wei.of(txRlp.readUInt256Scalar()))
        .payload(txRlp.readBytes())
        .rawRlp(txRlp.raw())
        .accessList(
            txRlp.readList(
                accessListEntryRLPInput -> {
                  accessListEntryRLPInput.enterList();
                  final AccessListEntry accessListEntry =
                      new AccessListEntry(
                          Address.wrap(accessListEntryRLPInput.readBytes()),
                          accessListEntryRLPInput.readList(RLPInput::readBytes32));
                  accessListEntryRLPInput.leaveList();
                  return accessListEntry;
                }))
        .codeDelegations(
            txRlp.readList(
                CodeDelegationTransactionDecoder::decodeInnerPayload,
                codeDelegationListLimit.maxCodeDelegations(gasLimit)))
        .sizeForBlockInclusion(input.size())
        .sizeForAnnouncement(input.size())
        .hash(Hash.hash(input));

    final byte recId = (byte) txRlp.readUnsignedByteScalar();
    final BigInteger r = txRlp.readUInt256Scalar().toUnsignedBigInteger();
    final BigInteger s = txRlp.readUInt256Scalar().toUnsignedBigInteger();

    txRlp.leaveList();

    return builder.signature(SIGNATURE_ALGORITHM.createSignature(r, s, recId)).build();
  }

  public static CodeDelegation decodeInnerPayload(final RLPInput input) {
    input.enterList();

    final ChainId chainId = ChainId.readFrom(input, Bytes32.SIZE);
    final Address address = Address.wrap(input.readBytes());
    final long nonce = input.readLongScalar();

    final byte yParity = (byte) input.readUnsignedByteScalar();
    final BigInteger r = input.readUInt256Scalar().toUnsignedBigInteger();
    final BigInteger s = input.readUInt256Scalar().toUnsignedBigInteger();

    input.leaveList();

    final SECPSignature signature =
        SIGNATURE_ALGORITHM.createCodeDelegationSignature(r, s, yParity);

    return new org.hyperledger.besu.ethereum.core.CodeDelegation(
        chainId, address, nonce, signature);
  }
}
