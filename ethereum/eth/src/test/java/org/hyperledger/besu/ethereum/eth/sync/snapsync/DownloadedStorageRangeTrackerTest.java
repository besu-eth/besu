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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class DownloadedStorageRangeTrackerTest {

  private static final Bytes32 ACCOUNT_A = Bytes32.fromHexString("0xaa");
  private static final Bytes32 ACCOUNT_B = Bytes32.fromHexString("0xbb");

  private static final Bytes32 SLOT_START = Bytes32.fromHexString("0x10");
  private static final Bytes32 SLOT_END = Bytes32.fromHexString("0x20");

  private final DownloadedStorageRangeTracker tracker = new DownloadedStorageRangeTracker();

  @Test
  void onlySlotsWithinRegisteredRangesAreDownloaded() {
    tracker.registerSlotRange(ACCOUNT_A, SLOT_START, SLOT_END);

    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_A, SLOT_START)).isTrue();
    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_A, SLOT_END)).isTrue();
    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_A, Bytes32.fromHexString("0x0f"))).isFalse();
    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_A, Bytes32.fromHexString("0x21"))).isFalse();
    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_B, SLOT_START)).isFalse();
  }

  @Test
  void tracksRangesIndependentlyPerAccount() {
    tracker.registerSlotRange(ACCOUNT_A, SLOT_START, SLOT_END);
    tracker.registerSlotRange(
        ACCOUNT_B, Bytes32.fromHexString("0x30"), Bytes32.fromHexString("0x40"));

    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_A, Bytes32.fromHexString("0x30"))).isFalse();
    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_B, Bytes32.fromHexString("0x35"))).isTrue();
    assertThat(tracker.getCompletedSlotRanges(ACCOUNT_A)).hasSize(1);
    assertThat(tracker.getCompletedSlotRanges(Bytes32.fromHexString("0xcc"))).isEmpty();
  }

  @Test
  void rejectsInvalidOrOverlappingRanges() {
    assertThatThrownBy(() -> tracker.registerSlotRange(ACCOUNT_A, SLOT_END, SLOT_START))
        .isInstanceOf(IllegalArgumentException.class);

    tracker.registerSlotRange(ACCOUNT_A, SLOT_START, SLOT_END);
    assertThatThrownBy(
            () ->
                tracker.registerSlotRange(
                    ACCOUNT_A, Bytes32.fromHexString("0x18"), Bytes32.fromHexString("0x30")))
        .isInstanceOf(IllegalStateException.class);

    // the same interval on another account is independent
    tracker.registerSlotRange(ACCOUNT_B, SLOT_START, SLOT_END);
    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_B, SLOT_START)).isTrue();
  }

  @Test
  void removeAccountDropsOnlyThatAccount() {
    tracker.registerSlotRange(ACCOUNT_A, SLOT_START, SLOT_END);
    tracker.registerSlotRange(ACCOUNT_B, SLOT_START, SLOT_END);

    tracker.removeAccount(ACCOUNT_A);

    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_A, SLOT_START)).isFalse();
    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_B, SLOT_START)).isTrue();
  }

  @Test
  void removeAccountHashesInRangeDropsOnlyAccountsInInterval() {
    tracker.registerSlotRange(Bytes32.fromHexString("0x01"), SLOT_START, SLOT_END);
    tracker.registerSlotRange(Bytes32.fromHexString("0x02"), SLOT_START, SLOT_END);
    tracker.registerSlotRange(Bytes32.fromHexString("0x03"), SLOT_START, SLOT_END);

    tracker.removeAccountHashesInRange(
        Bytes32.fromHexString("0x02"), Bytes32.fromHexString("0x02"));

    assertThat(tracker.isSlotHashDownloaded(Bytes32.fromHexString("0x01"), SLOT_START)).isTrue();
    assertThat(tracker.isSlotHashDownloaded(Bytes32.fromHexString("0x02"), SLOT_START)).isFalse();
    assertThat(tracker.isSlotHashDownloaded(Bytes32.fromHexString("0x03"), SLOT_START)).isTrue();
  }

  @Test
  void clearResetsAllTrackedSlots() {
    tracker.registerSlotRange(ACCOUNT_A, SLOT_START, SLOT_END);

    tracker.clear();

    assertThat(tracker.isSlotHashDownloaded(ACCOUNT_A, SLOT_START)).isFalse();
    assertThat(tracker.getCompletedSlotRanges(ACCOUNT_A)).isEmpty();
  }
}
