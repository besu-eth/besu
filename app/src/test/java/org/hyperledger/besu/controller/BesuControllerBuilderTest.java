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

import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BesuControllerBuilderTest {

  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final SynchronizerConfiguration snap2 =
      SynchronizerConfiguration.builder()
          .syncMode(SyncMode.SNAP2)
          .syncMinimumPeerCount(3)
          .downloaderHeadersRequestSize(100)
          .build();

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

  @Test
  void snap2OrSnapKeepsAnotherSyncMode() {
    final SynchronizerConfiguration snap =
        SynchronizerConfiguration.builder().syncMode(SyncMode.SNAP).build();

    assertThat(BesuControllerBuilder.snap2OrSnap(snap, protocolSchedule, at(0))).isSameAs(snap);
  }

  @Test
  void snap2OrSnapSyncsWithSnapWhenNoForkAddsBlockAccessLists() {
    when(protocolSchedule.milestoneFor(OSAKA)).thenReturn(Optional.of(10L));

    final SynchronizerConfiguration resolved =
        BesuControllerBuilder.snap2OrSnap(snap2, protocolSchedule, at(100));

    assertThat(resolved.getSyncMode()).isEqualTo(SyncMode.SNAP);
    assertThat(resolved).usingRecursiveComparison().ignoringFields("syncMode").isEqualTo(snap2);
  }

  @Test
  void snap2OrSnapSyncsWithSnapBeforeTheFork() {
    when(protocolSchedule.milestoneFor(AMSTERDAM)).thenReturn(Optional.of(1_000L));

    assertThat(BesuControllerBuilder.snap2OrSnap(snap2, protocolSchedule, at(999)).getSyncMode())
        .isEqualTo(SyncMode.SNAP);
  }

  @Test
  void snap2OrSnapKeepsSnap2FromTheFork() {
    when(protocolSchedule.milestoneFor(AMSTERDAM)).thenReturn(Optional.of(1_000L));

    assertThat(BesuControllerBuilder.snap2OrSnap(snap2, protocolSchedule, at(1_000)))
        .isSameAs(snap2);
  }

  private static Clock at(final long epochSecond) {
    return Clock.fixed(Instant.ofEpochSecond(epochSecond), ZoneOffset.UTC);
  }
}
