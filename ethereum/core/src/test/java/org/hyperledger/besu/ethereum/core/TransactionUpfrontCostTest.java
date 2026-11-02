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

import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class TransactionUpfrontCostTest {

  @Test
  void upfrontGasCostIsTheProductOfGasLimitAndPrice() {
    for (final long gasLimit : List.of(0L, 21_000L, 1L << 40, Long.MAX_VALUE)) {
      for (final Wei price :
          List.of(
              Wei.ZERO,
              Wei.ONE,
              Wei.of(30_000_000_000L),
              Wei.of(Long.MAX_VALUE),
              Wei.of(BigInteger.TWO.pow(64)),
              Wei.MAX_WEI)) {
        final Transaction transaction =
            new TransactionTestFixture()
                .type(TransactionType.EIP1559)
                .gasLimit(gasLimit)
                .maxFeePerGas(Optional.of(price))
                .createTransaction(SignatureAlgorithmFactory.getInstance().generateKeyPair());

        final BigInteger product = BigInteger.valueOf(gasLimit).multiply(price.getAsBigInteger());
        assertThat(transaction.getUpfrontGasCost(price, Wei.ZERO, 0))
            .isEqualTo(product.bitLength() > 256 ? Wei.MAX_WEI : Wei.of(product));
      }
    }
  }
}
