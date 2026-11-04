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
package org.hyperledger.besu.ethereum.mainnet.inclusionlist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.crypto.Hash.keccak256;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class InclusionListTransactionsTest {

  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  private final Transaction tx1 = new TransactionTestFixture().nonce(0).createTransaction(KEY_PAIR);
  private final Transaction tx2 = new TransactionTestFixture().nonce(1).createTransaction(KEY_PAIR);

  @Test
  void transactionsAreDeduplicatedAndSortedByHash() {
    final InclusionListTransactions inclusionList =
        new InclusionListTransactions(List.of(tx2, tx1, tx2));

    assertThat(inclusionList.getTransactions())
        .extracting(Transaction::getHash)
        .containsExactlyElementsOf(
            List.of(tx1.getHash(), tx2.getHash()).stream()
                .sorted((h1, h2) -> h1.getBytes().compareTo(h2.getBytes()))
                .toList());
  }

  @Test
  void checksumIsTheHashOfTheSortedTransactionHashes() {
    final InclusionListTransactions inclusionList =
        new InclusionListTransactions(List.of(tx1, tx2));

    final List<Bytes> sortedHashes =
        inclusionList.getTransactions().stream().map(tx -> tx.getHash().getBytes()).toList();
    assertThat(inclusionList.getChecksum()).isEqualTo(keccak256(Bytes.concatenate(sortedHashes)));
  }

  @Test
  void checksumDoesNotDependOnTheOrderOfTheTransactions() {
    assertThat(new InclusionListTransactions(List.of(tx1, tx2)))
        .isEqualTo(new InclusionListTransactions(List.of(tx2, tx1)));
  }

  @Test
  void checksumDependsOnTheTransactions() {
    assertThat(new InclusionListTransactions(List.of(tx1)).getChecksum())
        .isNotEqualTo(new InclusionListTransactions(List.of(tx2)).getChecksum())
        .isNotEqualTo(InclusionListTransactions.EMPTY.getChecksum());
  }

  @Test
  void keepsTransactionsThatDifferOnlyInTheirAccessList() {
    final Transaction tx =
        new TransactionTestFixture().type(TransactionType.EIP1559).createTransaction(KEY_PAIR);
    final Transaction txWithAccessList =
        Transaction.builder()
            .copiedFrom(tx)
            .accessList(List.of(new AccessListEntry(Address.ZERO, List.of())))
            .sender(null)
            .build();
    // Transaction.equals ignores the access list, but they are different transactions
    assertThat(txWithAccessList).isEqualTo(tx);

    final InclusionListTransactions inclusionList =
        new InclusionListTransactions(List.of(tx, txWithAccessList));

    assertThat(inclusionList.getTransactions())
        .extracting(Transaction::getHash)
        .containsExactlyInAnyOrder(tx.getHash(), txWithAccessList.getHash());
    assertThat(inclusionList.getChecksum())
        .isNotEqualTo(new InclusionListTransactions(List.of(tx)).getChecksum());
  }

  @Test
  void emptyInclusionList() {
    assertThat(InclusionListTransactions.EMPTY.isEmpty()).isTrue();
    assertThat(InclusionListTransactions.EMPTY.getTransactions()).isEmpty();
    assertThat(new InclusionListTransactions(List.of())).isEqualTo(InclusionListTransactions.EMPTY);
  }

  @Test
  void notIncludedInDropsTheTransactionsOfTheBlock() {
    final Transaction ilTx = new TransactionTestFixture().nonce(0).createTransaction(KEY_PAIR);
    final Transaction otherIlTx = new TransactionTestFixture().nonce(1).createTransaction(KEY_PAIR);
    final Transaction otherBlockTx =
        new TransactionTestFixture().nonce(2).createTransaction(KEY_PAIR);

    assertThat(
            new InclusionListTransactions(List.of(ilTx, otherIlTx))
                .notIncludedIn(List.of(otherBlockTx, ilTx)))
        .containsExactly(otherIlTx);
  }

  @Test
  void notIncludedInKeepsATransactionThatDiffersFromABlockTransactionOnlyInItsAccessList() {
    final Transaction blockTx =
        new TransactionTestFixture().type(TransactionType.EIP1559).createTransaction(KEY_PAIR);
    // same signature, so it recovers a different sender
    final Transaction ilTxWithAccessList =
        Transaction.builder()
            .copiedFrom(blockTx)
            .accessList(List.of(new AccessListEntry(Address.ZERO, List.of())))
            .sender(null)
            .build();
    // Transaction.equals ignores the access list, but the bytes differ
    assertThat(ilTxWithAccessList).isEqualTo(blockTx);
    assertThat(ilTxWithAccessList.getHash()).isNotEqualTo(blockTx.getHash());

    assertThat(
            new InclusionListTransactions(List.of(ilTxWithAccessList))
                .notIncludedIn(List.of(blockTx)))
        .containsExactly(ilTxWithAccessList);
  }

  @Test
  void notIncludedInStopsLookingOnceAllTheTransactionsAreFound() {
    final Transaction ilTx = new TransactionTestFixture().nonce(0).createTransaction(KEY_PAIR);
    final Transaction notLookedAt = mock(Transaction.class);

    assertThat(
            new InclusionListTransactions(List.of(ilTx)).notIncludedIn(List.of(ilTx, notLookedAt)))
        .isEmpty();
    verify(notLookedAt, never()).getHash();
  }

  @Test
  void nothingIsNotIncludedForAnEmptyInclusionList() {
    final Transaction blockTx = mock(Transaction.class);

    assertThat(InclusionListTransactions.EMPTY.notIncludedIn(List.of(blockTx))).isEmpty();
    verifyNoInteractions(blockTx);
  }
}
