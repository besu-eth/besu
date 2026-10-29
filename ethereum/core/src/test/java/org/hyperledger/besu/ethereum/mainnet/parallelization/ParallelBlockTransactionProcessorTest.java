/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.Transaction;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class ParallelBlockTransactionProcessorTest {

  @Test
  void recoversTheSenderOnTheBlockTransactionBeforeCopyingIt() {
    final Transaction transaction = mock(Transaction.class);
    final Transaction copy = mock(Transaction.class);
    when(transaction.detachedCopy()).thenReturn(copy);

    assertThat(ParallelBlockTransactionProcessor.detachedCopyWithSender(transaction))
        .isSameAs(copy);

    final InOrder order = inOrder(transaction);
    order.verify(transaction).getSender();
    order.verify(transaction).detachedCopy();
  }

  @Test
  void copiesATransactionWhoseSignatureRecoversNoSender() {
    final Transaction transaction = mock(Transaction.class);
    final Transaction copy = mock(Transaction.class);
    when(transaction.getSender()).thenThrow(new IllegalStateException("no public key"));
    when(transaction.detachedCopy()).thenReturn(copy);

    assertThat(ParallelBlockTransactionProcessor.detachedCopyWithSender(transaction))
        .isSameAs(copy);
  }
}
