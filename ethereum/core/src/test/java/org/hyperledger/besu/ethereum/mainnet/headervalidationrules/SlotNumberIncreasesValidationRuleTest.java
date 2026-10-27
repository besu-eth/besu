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
package org.hyperledger.besu.ethereum.mainnet.headervalidationrules;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SlotNumberIncreasesValidationRuleTest {

  private final SlotNumberIncreasesValidationRule rule = new SlotNumberIncreasesValidationRule();

  @ParameterizedTest
  @CsvSource({
    "41, 42, true",
    "41, 50, true",
    "41, 41, false",
    "41, 40, false",
    // slot numbers are uint64: negative longs are the top half of the range
    "1, -1, true",
    "-2, -1, true",
    "-1, 1, false",
    "9223372036854775807, -9223372036854775808, true"
  })
  void slotNumberMustBeGreaterThanTheParentOne(
      final long parentSlotNumber, final long slotNumber, final boolean valid) {
    final BlockHeader parent =
        new BlockHeaderTestFixture().slotNumber(parentSlotNumber).buildHeader();
    final BlockHeader header = new BlockHeaderTestFixture().slotNumber(slotNumber).buildHeader();

    assertThat(rule.validate(header, parent)).isEqualTo(valid);
  }
}
