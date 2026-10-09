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
import static org.hyperledger.besu.ethereum.mainnet.headervalidationrules.DaoExtraDataValidationRule.DAO_EXTRA_DATA;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DaoExtraDataValidationRuleTest {

  private static final long DAO_FORK_BLOCK = 100;

  private final DaoExtraDataValidationRule rule = new DaoExtraDataValidationRule(DAO_FORK_BLOCK);

  @ParameterizedTest
  @ValueSource(longs = {DAO_FORK_BLOCK, DAO_FORK_BLOCK + 9})
  void blocksInTheDaoRangeMustCarryTheDaoExtraData(final long number) {
    assertThat(rule.validate(header(number, DAO_EXTRA_DATA), null)).isTrue();
    assertThat(rule.validate(header(number, Bytes.EMPTY), null)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(longs = {DAO_FORK_BLOCK - 1, DAO_FORK_BLOCK + 10})
  void blocksOutsideTheDaoRangeAreNotChecked(final long number) {
    assertThat(rule.validate(header(number, Bytes.EMPTY), null)).isTrue();
  }

  private static BlockHeader header(final long number, final Bytes extraData) {
    return new BlockHeaderTestFixture().number(number).extraData(extraData).buildHeader();
  }
}
