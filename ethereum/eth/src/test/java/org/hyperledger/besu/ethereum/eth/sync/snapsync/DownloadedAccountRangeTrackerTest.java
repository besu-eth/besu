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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class DownloadedAccountRangeTrackerTest {

  private static final Bytes32 START = Bytes32.fromHexString("0x10");
  private static final Bytes32 MID = Bytes32.fromHexString("0x20");
  private static final Bytes32 END = Bytes32.fromHexString("0x30");

  private final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();

  @Test
  void rangeWithNoChildrenCompletesImmediately() {
    final AtomicInteger completions = new AtomicInteger();
    tracker.setOnRangeCompleted((start, end) -> completions.incrementAndGet());

    tracker.registerPending(START, END, 0);

    assertThat(tracker.isAccountHashDownloaded(MID)).isTrue();
    assertThat(tracker.pendingRangeCount()).isZero();
    assertThat(completions).hasValue(1);
  }

  @Test
  void pendingRangePromotesToCompletedOnlyWhenAllChildrenFinish() {
    tracker.registerPending(START, END, 2);

    assertThat(tracker.isAccountHashPending(MID)).isTrue();
    assertThat(tracker.isAccountHashPersisted(MID)).isTrue();
    assertThat(tracker.isAccountHashDownloaded(MID)).isFalse();

    tracker.onChildCompleted(START);
    assertThat(tracker.isAccountHashDownloaded(MID)).isFalse();

    tracker.onChildCompleted(START);
    assertThat(tracker.isAccountHashDownloaded(MID)).isTrue();
    assertThat(tracker.isAccountHashPending(MID)).isFalse();
  }

  @Test
  void addedChildDelaysCompletion() {
    tracker.registerPending(START, END, 1);
    tracker.addPendingChild(START);

    tracker.onChildCompleted(START);
    assertThat(tracker.isAccountHashDownloaded(MID)).isFalse();

    tracker.onChildCompleted(START);
    assertThat(tracker.isAccountHashDownloaded(MID)).isTrue();
  }

  @Test
  void rangeBoundariesAreInclusive() {
    tracker.registerPending(START, MID, 0);
    tracker.registerPending(END, Bytes32.fromHexString("0x40"), 0);

    assertThat(tracker.isAccountHashPersisted(START)).isTrue();
    assertThat(tracker.isAccountHashPersisted(MID)).isTrue();
    assertThat(tracker.isAccountHashPersisted(END)).isTrue();
    assertThat(tracker.isAccountHashPersisted(Bytes32.fromHexString("0x0f"))).isFalse();
    assertThat(tracker.isAccountHashPersisted(Bytes32.fromHexString("0x21"))).isFalse();
    assertThat(tracker.isAccountHashPersisted(Bytes32.fromHexString("0x41"))).isFalse();
  }

  @Test
  void rejectsOverlappingRanges() {
    tracker.registerPending(START, MID, 1);

    assertThatThrownBy(() -> tracker.registerPending(Bytes32.fromHexString("0x15"), END, 0))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> tracker.registerPending(START, END, 0))
        .isInstanceOf(IllegalStateException.class);

    // adjacent ranges sharing only a boundary key are fine
    tracker.registerPending(END, Bytes32.fromHexString("0x40"), 0);
    assertThat(tracker.completedRangeCount()).isEqualTo(1);
  }

  @Test
  void rejectsInvalidChildCounts() {
    assertThatThrownBy(() -> tracker.registerPending(START, END, -1))
        .isInstanceOf(IllegalArgumentException.class);

    tracker.registerPending(START, END, 1);
    assertThatThrownBy(() -> tracker.onChildCompleted(Bytes32.fromHexString("0x50")))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(
            () -> {
              tracker.onChildCompleted(START);
              tracker.onChildCompleted(START);
            })
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void clearResetsAllTrackedRanges() {
    tracker.registerPending(START, MID, 0);
    tracker.registerPending(END, Bytes32.fromHexString("0x40"), 1);

    tracker.clear();

    assertThat(tracker.isAccountHashPersisted(MID)).isFalse();
    assertThat(tracker.completedRangeCount()).isZero();
    assertThat(tracker.pendingRangeCount()).isZero();
  }
}
