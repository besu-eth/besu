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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class BlockGasUsedValidatorTest {

  private static final List<TransactionReceipt> RECEIPTS =
      List.of(new TransactionReceipt(1, 21_000, List.of(), Optional.empty()));

  @ParameterizedTest
  @CsvSource({"30000, true", "29999, false", "30001, false"})
  void amsterdamComparesGasUsedWithBlockGasFromProcessing(
      final long blockGas, final boolean expectedValid) {
    final BlockHeader header = new BlockHeaderTestFixture().gasUsed(30_000).buildHeader();

    assertThat(
            BlockGasUsedValidator.AMSTERDAM.validate(header, RECEIPTS, OptionalLong.of(blockGas)))
        .isEqualTo(expectedValid);
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 1, 21_000, 30_000})
  void amsterdamDoesNotValidateGasUsedOfBlockWithTransactionsWithoutBlockGas(final long gasUsed) {
    final BlockHeader header = new BlockHeaderTestFixture().gasUsed(gasUsed).buildHeader();

    assertThat(BlockGasUsedValidator.AMSTERDAM.validate(header, RECEIPTS, OptionalLong.empty()))
        .isTrue();
  }
}
