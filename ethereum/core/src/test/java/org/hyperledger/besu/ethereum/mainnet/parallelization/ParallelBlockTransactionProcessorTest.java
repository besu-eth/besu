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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class ParallelBlockTransactionProcessorTest {

  @Test
  void recoversTheSenderOnTheBlockTransactionBeforeCopyingIt() {
    final Transaction transaction = mock(Transaction.class);
    final Transaction copy = mock(Transaction.class);
    when(transaction.detachedCopy()).thenReturn(copy);

    assertThat(ParallelBlockTransactionProcessor.detachedCopyWithSigners(transaction))
        .isSameAs(copy);

    final InOrder order = inOrder(transaction);
    order.verify(transaction).getSender();
    order.verify(transaction).detachedCopy();
  }

  @Test
  void theCopyKeepsTheAuthoritiesRecoveredOnTheBlockTransaction() {
    final KeyPair authorityKeyPair = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    final CodeDelegation codeDelegation =
        org.hyperledger.besu.ethereum.core.CodeDelegation.builder()
            .chainId(BigInteger.ONE)
            .address(Address.fromHexString("0x1234"))
            .nonce(0)
            .signAndBuild(authorityKeyPair);
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.DELEGATE_CODE)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.ZERO)
            .maxFeePerGas(Wei.ONE)
            .gasLimit(100_000L)
            .to(Address.ZERO)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .codeDelegations(List.of(codeDelegation))
            .chainId(BigInteger.ONE)
            .signAndBuild(SignatureAlgorithmFactory.getInstance().generateKeyPair());

    final Transaction copy = ParallelBlockTransactionProcessor.detachedCopyWithSigners(transaction);

    // the same instance, so the copy did not recover the authority a second time
    final Optional<Address> authority =
        copy.getCodeDelegationList().orElseThrow().getFirst().authorizer();
    assertThat(authority).isSameAs(codeDelegation.authorizer());
    assertThat(authority).contains(Address.extract(authorityKeyPair.getPublicKey()));
  }

  @Test
  void copiesATransactionWhoseSignaturesRecoverNoSigner() {
    final Transaction transaction = mock(Transaction.class);
    final Transaction copy = mock(Transaction.class);
    final CodeDelegation unrecoverable = mock(CodeDelegation.class);
    final CodeDelegation recoverable = mock(CodeDelegation.class);
    when(transaction.getSender()).thenThrow(new IllegalStateException("no public key"));
    when(unrecoverable.authorizer()).thenThrow(new IllegalArgumentException("invalid point"));
    when(transaction.getCodeDelegationList())
        .thenReturn(Optional.of(List.of(unrecoverable, recoverable)));
    when(transaction.detachedCopy()).thenReturn(copy);

    assertThat(ParallelBlockTransactionProcessor.detachedCopyWithSigners(transaction))
        .isSameAs(copy);
    verify(recoverable).authorizer();
  }
}
