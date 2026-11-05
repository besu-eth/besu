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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.query.TransactionReceiptWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

public class TransactionReceiptResultTest {

  @Test
  public void frameTransactionEffectiveGasPriceIsBaseFeePlusTip() {
    final Transaction frameTx =
        new TransactionTestFixture()
            .type(TransactionType.FRAME)
            .createTransaction(new BlockDataGenerator().generateKeyPair());

    final TransactionReceiptStatusResult result =
        new TransactionReceiptStatusResult(
            TransactionReceiptWithMetadata.create(
                new TransactionReceipt(1, 21_000L, List.of(), Optional.empty()),
                frameTx,
                frameTx.getHash(),
                0,
                21_000L,
                Optional.of(Wei.of(7L)),
                Hash.ZERO,
                0L,
                1L,
                Optional.empty(),
                Optional.empty(),
                0));

    // the base fee of 7 plus the full tip of 500
    assertThat(result.getEffectiveGasPrice()).isEqualTo("0x1fb");
  }
}
