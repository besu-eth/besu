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
package org.hyperledger.besu.tests.acceptance.dsl.transaction.eth;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.transaction.NodeRequests;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.Transaction;

import java.io.IOException;
import java.util.Optional;

import org.web3j.protocol.core.methods.response.EthTransaction;

public class EthGetTransactionByHashTransaction
    implements Transaction<Optional<org.web3j.protocol.core.methods.response.Transaction>> {

  private final String transactionHash;

  public EthGetTransactionByHashTransaction(final String transactionHash) {
    this.transactionHash = transactionHash;
  }

  @Override
  public Optional<org.web3j.protocol.core.methods.response.Transaction> execute(
      final NodeRequests node) {
    try {
      final EthTransaction result = node.eth().ethGetTransactionByHash(transactionHash).send();
      assertThat(result.hasError()).isFalse();
      return result.getTransaction();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
