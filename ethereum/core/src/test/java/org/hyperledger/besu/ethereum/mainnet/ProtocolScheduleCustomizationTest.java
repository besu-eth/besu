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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleActivation.blockNumber;
import static org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleActivation.timestamp;

import java.util.List;

import org.junit.jupiter.api.Test;

class ProtocolScheduleCustomizationTest {

  @Test
  void activationsRetainTheirDomainEvenWhenTheirValuesMatch() {
    final ProtocolScheduleCustomization customization =
        new ProtocolScheduleCustomization(
            "test",
            List.of(
                new ProtocolSpecModification(blockNumber(10), builder -> builder),
                new ProtocolSpecModification(timestamp(10), builder -> builder)));

    assertThat(customization.toForkIdActivations().blockNumbers()).containsExactly(10L);
    assertThat(customization.toForkIdActivations().timestamps()).containsExactly(10L);
  }

  @Test
  void forkIdActivationsAreDerivedFromRules() {
    final ProtocolScheduleCustomization customization =
        new ProtocolScheduleCustomization(
            "test", List.of(new ProtocolSpecModification(blockNumber(10), builder -> builder)));

    assertThat(customization.toForkIdActivations().blockNumbers()).containsExactly(10L);
  }

  @Test
  void duplicateRulesInTheSameDomainAreRejected() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ProtocolScheduleCustomization(
                    "test",
                    List.of(
                        new ProtocolSpecModification(blockNumber(10), builder -> builder),
                        new ProtocolSpecModification(blockNumber(10), builder -> builder))))
        .withMessageContaining("More than one");
  }
}
