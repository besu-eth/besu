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

import static org.hyperledger.besu.ethereum.core.encoding.AccessListTransactionEncoder.writeAccessList;
import static org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder.writeSignatureAndRecoveryId;

import org.hyperledger.besu.datatypes.BytesHolder;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public class CodeDelegationTransactionEncoder {

  private CodeDelegationTransactionEncoder() {
    // private constructor
  }

  public static void encodeCodeDelegationInner(
      final List<CodeDelegation> payloads, final RLPOutput rlpOutput) {
    rlpOutput.startList();
    payloads.forEach(payload -> encodeSingleCodeDelegation(payload, rlpOutput));
    rlpOutput.endList();
  }

  public static void encodeSingleCodeDelegationWithoutSignature(
      final CodeDelegation payload, final RLPOutput rlpOutput) {
    rlpOutput.startList();
    encodeAuthorizationDetails(payload, rlpOutput);
    rlpOutput.endList();
  }

  public static void encodeSingleCodeDelegation(
      final CodeDelegation payload, final RLPOutput rlpOutput) {
    rlpOutput.startList();
    encodeAuthorizationDetails(payload, rlpOutput);
    rlpOutput.writeUnsignedByte(payload.signature().getRecId() & 0xFF);
    rlpOutput.writeBigIntegerScalar(payload.signature().getR());
    rlpOutput.writeBigIntegerScalar(payload.signature().getS());
    rlpOutput.endList();
  }

  private static void encodeAuthorizationDetails(
      final CodeDelegation payload, final RLPOutput rlpOutput) {
    payload.chainIdentifier().writeTo(rlpOutput);
    rlpOutput.writeBytes(payload.address().getBytes().copy());
    rlpOutput.writeLongScalar(payload.nonce());
  }

  public static void encode(final Transaction transaction, final RLPOutput out) {
    out.startList();
    transaction.getChainIdentifier().writeTo(out);
    out.writeLongScalar(transaction.getNonce());
    out.writeUInt256Scalar(transaction.getMaxPriorityFeePerGas().orElseThrow());
    out.writeUInt256Scalar(transaction.getMaxFeePerGas().orElseThrow());
    out.writeLongScalar(transaction.getGasLimit());
    out.writeBytes(
        transaction.getTo().map(BytesHolder::getBytes).map(Bytes::copy).orElse(Bytes.EMPTY));
    out.writeUInt256Scalar(transaction.getValue());
    out.writeBytes(transaction.getPayload());
    writeAccessList(out, transaction.getAccessList());
    encodeCodeDelegationInner(
        transaction
            .getCodeDelegationList()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Developer error: the transaction should be guaranteed to have a code delegation authorizations here")),
        out);
    writeSignatureAndRecoveryId(transaction, out);
    out.endList();
  }
}
