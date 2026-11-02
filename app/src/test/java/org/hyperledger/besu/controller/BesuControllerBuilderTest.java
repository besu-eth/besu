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
package org.hyperledger.besu.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BOGOTA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.FUTURE_EIPS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BesuControllerBuilderTest {

  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);

  @BeforeEach
  void setUp() {
    when(protocolSchedule.milestoneFor(any())).thenReturn(Optional.empty());
  }

  @Test
  void amsterdamOrLaterMilestoneIsEmptyWhenOnlyEarlierForksAreScheduled() {
    when(protocolSchedule.milestoneFor(OSAKA)).thenReturn(Optional.of(10L));

    assertThat(BesuControllerBuilder.amsterdamOrLaterMilestone(protocolSchedule)).isEmpty();
  }

  @Test
  void amsterdamOrLaterMilestoneUsesAmsterdamWhenScheduled() {
    when(protocolSchedule.milestoneFor(AMSTERDAM)).thenReturn(Optional.of(20L));
    when(protocolSchedule.milestoneFor(BOGOTA)).thenReturn(Optional.of(30L));

    assertThat(BesuControllerBuilder.amsterdamOrLaterMilestone(protocolSchedule)).contains(20L);
  }

  @Test
  void amsterdamOrLaterMilestoneUsesBogotaWhenAmsterdamIsNotScheduled() {
    when(protocolSchedule.milestoneFor(BOGOTA)).thenReturn(Optional.of(30L));

    assertThat(BesuControllerBuilder.amsterdamOrLaterMilestone(protocolSchedule)).contains(30L);
  }

  @Test
  void amsterdamOrLaterMilestoneUsesEarliestLaterFork() {
    when(protocolSchedule.milestoneFor(BOGOTA)).thenReturn(Optional.of(30L));
    when(protocolSchedule.milestoneFor(FUTURE_EIPS)).thenReturn(Optional.of(0L));

    assertThat(BesuControllerBuilder.amsterdamOrLaterMilestone(protocolSchedule)).contains(0L);
  }
}
