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

import org.junit.jupiter.api.Test;

class SlotNumberPresentValidationRuleTest {

  private final SlotNumberPresentValidationRule rule = new SlotNumberPresentValidationRule();

  @Test
  void rejectsHeaderWithMissingSlotNumber() {
    final BlockHeader parent = new BlockHeaderTestFixture().buildHeader();
    final BlockHeader header = new BlockHeaderTestFixture().slotNumber(null).buildHeader();

    assertThat(header.getOptionalSlotNumber()).isEmpty();
    assertThat(rule.validate(header, parent)).isFalse();
  }

  @Test
  void acceptsHeaderWithZeroSlotNumber() {
    final BlockHeader parent = new BlockHeaderTestFixture().buildHeader();
    final BlockHeader header = new BlockHeaderTestFixture().slotNumber(0L).buildHeader();

    assertThat(rule.validate(header, parent)).isTrue();
  }

  @Test
  void acceptsHeaderWithMaxUint64SlotNumber() {
    // slot_number is a uint64: the top half of the range is represented as a negative long.
    final BlockHeader parent = new BlockHeaderTestFixture().buildHeader();
    final BlockHeader header = new BlockHeaderTestFixture().slotNumber(-1L).buildHeader();

    assertThat(rule.validate(header, parent)).isTrue();
  }

  @Test
  void includedInLightValidation() {
    // slotNumber presence is a detached header property and must be enforced even on the
    // light-validation sync path.
    assertThat(rule.includeInLightValidation()).isTrue();
  }
}
