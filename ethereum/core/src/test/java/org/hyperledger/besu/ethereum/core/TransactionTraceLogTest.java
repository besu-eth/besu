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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

class TransactionTraceLogTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair KEY_PAIR = SIGNATURE_ALGORITHM.generateKeyPair();

  @Test
  void traceLogContainsTheSender() {
    final Transaction transaction = new TransactionTestFixture().createTransaction(KEY_PAIR);

    assertThat(transaction.toTraceLog()).contains(transaction.getSender().toString());
  }

  @Test
  void traceLogDoesNotFailWhenTheSenderCannotBeRecovered() {
    // r = 5 is a valid scalar but not the x-coordinate of any curve point
    final Transaction transaction =
        Transaction.builder()
            .copiedFrom(new TransactionTestFixture().createTransaction(KEY_PAIR))
            .sender(null)
            .signature(
                SIGNATURE_ALGORITHM.createSignature(
                    BigInteger.valueOf(5), BigInteger.ONE, (byte) 0))
            .build();
    assertThatIllegalStateException().isThrownBy(transaction::getSender);

    assertThat(transaction.toTraceLog())
        .contains(transaction.getHash().toString())
        .contains("<unrecoverable sender>");
  }
}
