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

import static org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder.writeSignature;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BytesHolder;
import org.hyperledger.besu.datatypes.ChainId;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class AccessListTransactionEncoder {

  public static void encode(final Transaction transaction, final RLPOutput rlpOutput) {
    rlpOutput.startList();
    encodeAccessListInner(
        transaction.getChainIdentifier(),
        transaction.getNonce(),
        transaction.getGasPrice().orElseThrow(),
        transaction.getGasLimit(),
        transaction.getTo(),
        transaction.getValue(),
        transaction.getPayload(),
        transaction
            .getAccessList()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Developer error: access list should be guaranteed to be present")),
        rlpOutput);
    rlpOutput.writeIntScalar(transaction.getSignature().getRecId());
    writeSignature(transaction, rlpOutput);
    rlpOutput.endList();
  }

  public static void encodeAccessListInner(
      final ChainId chainId,
      final long nonce,
      final Wei gasPrice,
      final long gasLimit,
      final Optional<Address> to,
      final Wei value,
      final Bytes payload,
      final List<AccessListEntry> accessList,
      final RLPOutput rlpOutput) {
    chainId.writeTo(rlpOutput);
    rlpOutput.writeLongScalar(nonce);
    rlpOutput.writeUInt256Scalar(gasPrice);
    rlpOutput.writeLongScalar(gasLimit);
    rlpOutput.writeBytes(to.map(BytesHolder::getBytes).map(Bytes::copy).orElse(Bytes.EMPTY));
    rlpOutput.writeUInt256Scalar(value);
    rlpOutput.writeBytes(payload);
    /*
    Access List encoding should look like this
    where hex strings represent raw bytes
    [
      [
        "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae",
        [
          "0x0000000000000000000000000000000000000000000000000000000000000003",
          "0x0000000000000000000000000000000000000000000000000000000000000007"
        ]
      ],
      [
        "0xbb9bc244d798123fde783fcc1c72d3bb8c189413",
        []
      ]
    ] */
    writeAccessList(rlpOutput, Optional.of(accessList));
  }

  public static void writeAccessList(
      final RLPOutput out, final Optional<List<AccessListEntry>> accessListEntries) {
    if (accessListEntries.isEmpty()) {
      out.writeEmptyList();
    } else {
      out.writeList(
          accessListEntries.get(),
          (accessListEntry, accessListEntryRLPOutput) -> {
            accessListEntryRLPOutput.startList();
            out.writeBytes(accessListEntry.address().getBytes());
            out.writeList(
                accessListEntry.storageKeys(),
                (storageKeyBytes, storageKeyBytesRLPOutput) ->
                    storageKeyBytesRLPOutput.writeBytes(storageKeyBytes));
            accessListEntryRLPOutput.endList();
          });
    }
  }
}
