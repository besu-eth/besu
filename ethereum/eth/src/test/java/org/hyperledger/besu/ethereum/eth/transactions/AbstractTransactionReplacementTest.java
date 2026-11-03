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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class AbstractTransactionReplacementTest {
  protected static PendingTransaction frontierTx(final long price) {
    final PendingTransaction pendingTransaction = mock(PendingTransaction.class);
    final Transaction transaction =
        Transaction.builder()
            .chainId(BigInteger.ZERO)
            .type(TransactionType.FRONTIER)
            .gasPrice(Wei.of(price))
            .build();
    when(pendingTransaction.getTransaction()).thenReturn(transaction);
    when(pendingTransaction.getGasPrice()).thenReturn(Wei.of(price));
    return pendingTransaction;
  }

  protected static PendingTransaction eip1559Tx(
      final long maxPriorityFeePerGas, final long maxFeePerGas) {
    final PendingTransaction pendingTransaction = mock(PendingTransaction.class);
    final Transaction transaction =
        Transaction.builder()
            .chainId(BigInteger.ZERO)
            .type(TransactionType.EIP1559)
            .maxPriorityFeePerGas(Wei.of(maxPriorityFeePerGas))
            .maxFeePerGas(Wei.of(maxFeePerGas))
            .build();
    when(pendingTransaction.getTransaction()).thenReturn(transaction);
    return pendingTransaction;
  }

  protected static PendingTransaction blobTx(
      final long maxPriorityFeePerGas, final long maxFeePerGas, final long maxFeePerBlobGas) {
    final PendingTransaction pendingTransaction = mock(PendingTransaction.class);
    final Transaction transaction =
        Transaction.builder()
            .chainId(BigInteger.ZERO)
            .type(TransactionType.BLOB)
            .maxPriorityFeePerGas(Wei.of(maxPriorityFeePerGas))
            .maxFeePerGas(Wei.of(maxFeePerGas))
            .maxFeePerBlobGas(Wei.of(maxFeePerBlobGas))
            .versionedHashes(List.of(VersionedHash.DEFAULT_VERSIONED_HASH))
            .build();
    when(pendingTransaction.getTransaction()).thenReturn(transaction);
    return pendingTransaction;
  }

  /**
   * An EIP-8141 frame transaction. Its type supports blobs, but it carries them only when {@code
   * carriesBlobs} is set; it always declares a blob fee cap.
   */
  protected static PendingTransaction frameTx(
      final long maxPriorityFeePerGas,
      final long maxFeePerGas,
      final long maxFeePerBlobGas,
      final boolean carriesBlobs) {
    final PendingTransaction pendingTransaction = mock(PendingTransaction.class);
    final Transaction.Builder builder =
        Transaction.builder()
            .chainId(BigInteger.ZERO)
            .type(TransactionType.FRAME)
            .sender(Address.fromHexString("0x01"))
            .frames(
                List.of(
                    new Frame(
                        Frame.MODE_SENDER,
                        0,
                        Optional.empty(),
                        21_000L,
                        0L,
                        Wei.ZERO,
                        Bytes.EMPTY)))
            .maxPriorityFeePerGas(Wei.of(maxPriorityFeePerGas))
            .maxFeePerGas(Wei.of(maxFeePerGas))
            .maxFeePerBlobGas(Wei.of(maxFeePerBlobGas));
    if (carriesBlobs) {
      builder.versionedHashes(List.of(VersionedHash.DEFAULT_VERSIONED_HASH));
    }
    when(pendingTransaction.getTransaction()).thenReturn(builder.build());
    return pendingTransaction;
  }
}
