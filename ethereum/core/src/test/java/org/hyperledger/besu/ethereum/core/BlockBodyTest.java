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
package org.hyperledger.besu.ethereum.core;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;

import java.util.List;

import org.junit.jupiter.api.Test;

class BlockBodyTest {
  private final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();

  @Test
  void transactionsRootIsTheRootOfTheTransactions() {
    final List<Transaction> transactions =
        List.of(blockDataGenerator.transaction(), blockDataGenerator.transaction());

    assertThat(new BlockBody(transactions, emptyList()).getTransactionsRoot())
        .isEqualTo(BodyValidation.transactionsRoot(transactions));
  }

  @Test
  void transactionsRootIsComputedOnce() {
    final BlockBody body = new BlockBody(List.of(blockDataGenerator.transaction()), emptyList());

    assertThat(body.getTransactionsRoot()).isSameAs(body.getTransactionsRoot());
  }

  @Test
  void emptyBodyHasTheEmptyTrieRoot() {
    assertThat(BlockBody.empty().getTransactionsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
  }
}
