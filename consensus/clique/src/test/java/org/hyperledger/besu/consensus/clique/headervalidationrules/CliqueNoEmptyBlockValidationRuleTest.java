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
package org.hyperledger.besu.consensus.clique.headervalidationrules;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;

import java.util.List;

import org.junit.jupiter.api.Test;

class CliqueNoEmptyBlockValidationRuleTest {

  @Test
  void headerWithNoTransactionsIsInvalid() {
    final BlockHeader blockHeader = new BlockHeaderTestFixture().buildHeader();

    final CliqueNoEmptyBlockValidationRule noEmptyBlockRule =
        new CliqueNoEmptyBlockValidationRule();
    assertThat(noEmptyBlockRule.validate(blockHeader, blockHeader)).isFalse();
  }

  @Test
  void headerWithTransactionsIsValid() {
    final TransactionTestFixture transactionTestFixture = new TransactionTestFixture();
    final KeyPair keyPair = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    final Transaction transaction = transactionTestFixture.createTransaction(keyPair);
    final Hash transactionRoot = BodyValidation.transactionsRoot(List.of(transaction));
    final BlockHeader blockHeader =
        new BlockHeaderTestFixture().transactionsRoot(transactionRoot).buildHeader();

    final CliqueNoEmptyBlockValidationRule noEmptyBlockRule =
        new CliqueNoEmptyBlockValidationRule();
    assertThat(noEmptyBlockRule.validate(blockHeader, blockHeader)).isTrue();
  }
}
