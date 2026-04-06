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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.VersionedHash.DEFAULT_VERSIONED_HASH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class TransactionBuilderTest {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair senderKeys = SIGNATURE_ALGORITHM.generateKeyPair();

  @Test
  void guessTypeCanGuessAllTypes() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final List<AccessListEntry> accessList =
        List.of(new AccessListEntry(gen.address(), List.of(gen.bytes32())));

    final Transaction.Builder frontierBuilder = Transaction.builder();
    final Transaction.Builder accessListBuilder = Transaction.builder().accessList(accessList);

    final Transaction.Builder eip1559Builder =
        Transaction.builder().accessList(accessList).maxFeePerGas(Wei.of(5));

    final Transaction.Builder blobBuilder =
        Transaction.builder()
            .accessList(accessList)
            .maxFeePerGas(Wei.of(5))
            .versionedHashes(List.of(DEFAULT_VERSIONED_HASH));

    final CodeDelegation codeDelegation =
        new CodeDelegation(
            BigInteger.ZERO,
            Address.ZERO,
            0,
            new SECPSignature(BigInteger.ZERO, BigInteger.ZERO, (byte) 0));

    final Transaction.Builder delegateCodeBuilder =
        Transaction.builder()
            .accessList(accessList)
            .maxFeePerGas(Wei.of(5))
            .codeDelegations(List.of(codeDelegation));

    final List<TransactionType> guessedTypes =
        Stream.of(
                frontierBuilder,
                accessListBuilder,
                eip1559Builder,
                blobBuilder,
                delegateCodeBuilder)
            .map(transactionBuilder -> transactionBuilder.guessType().getTransactionType())
            .toList();

    assertThat(guessedTypes)
        .containsExactly(
            TransactionType.FRONTIER,
            TransactionType.ACCESS_LIST,
            TransactionType.EIP1559,
            TransactionType.BLOB,
            TransactionType.DELEGATE_CODE);
  }

  @Test
  void zeroBlobTransactionIsInvalid() {
    TransactionTestFixture ttf =
        new TransactionTestFixture()
            .type(TransactionType.BLOB)
            .chainId(Optional.of(BigInteger.ONE))
            .versionedHashes(Optional.of(List.of()))
            .maxFeePerGas(Optional.of(Wei.of(5)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(5)))
            .maxFeePerBlobGas(Optional.of(Wei.of(5)));
    try {
      ttf.createTransaction(senderKeys);
      fail();
    } catch (IllegalArgumentException iea) {
      assertThat(iea).hasMessage("Blob transaction must have at least one versioned hash");
    }
  }

  @Test
  @SuppressWarnings("ReferenceEquality")
  void copyFromIsIdentical() {
    final TransactionTestFixture fixture = new TransactionTestFixture();
    final Transaction transaction = fixture.createTransaction(senderKeys);
    final Transaction.Builder builder = Transaction.builder();
    final Transaction copy = builder.copiedFrom(transaction).build();
    assertThat(copy).isEqualTo(transaction).isNotSameAs(transaction);
    assertThat(copy.getHash()).isEqualTo(transaction.getHash());
    BytesValueRLPOutput sourceRLP = new BytesValueRLPOutput();
    transaction.writeTo(sourceRLP);
    BytesValueRLPOutput copyRLP = new BytesValueRLPOutput();
    copy.writeTo(copyRLP);
    assertEquals(sourceRLP.encoded(), copyRLP.encoded());
  }
}
