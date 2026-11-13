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

import org.hyperledger.besu.datatypes.BytesHolder;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public class BlobTransactionEncoder {

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
    AccessListTransactionEncoder.writeAccessList(out, transaction.getAccessList());
    out.writeUInt256Scalar(transaction.getMaxFeePerBlobGas().orElseThrow());
    out.startList();
    transaction
        .getVersionedHashes()
        .get()
        .forEach(
            vh -> {
              out.writeBytes(vh.getBytes());
            });
    out.endList();
    TransactionEncoder.writeSignatureAndRecoveryId(transaction, out);
    out.endList();
  }

  public static void writeBlobVersionedHashes(
      final RLPOutput rlpOutput, final List<VersionedHash> versionedHashes) {
    rlpOutput.writeList(versionedHashes, (h, out) -> out.writeBytes(h.getBytes()));
  }
}
