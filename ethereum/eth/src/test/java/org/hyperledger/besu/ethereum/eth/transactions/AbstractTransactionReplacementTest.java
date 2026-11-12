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

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

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

  protected static PendingTransaction frameTx(final long maxPriorityFeePerGas, final long maxFee) {
    final PendingTransaction pendingTransaction = mock(PendingTransaction.class);
    final Transaction transaction =
        new TransactionTestFixture()
            .type(TransactionType.FRAME)
            .maxPriorityFeePerGas(Optional.of(Wei.of(maxPriorityFeePerGas)))
            .maxFee(Optional.of(Wei.of(maxFee)))
            .createTransaction(null);
    when(pendingTransaction.getTransaction()).thenReturn(transaction);
    return pendingTransaction;
  }
}
