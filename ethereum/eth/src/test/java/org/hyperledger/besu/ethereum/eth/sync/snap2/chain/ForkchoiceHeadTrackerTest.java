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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

class ForkchoiceHeadTrackerTest {

  private final ForkchoiceHeadTracker tracker = new ForkchoiceHeadTracker(() -> {});

  @Test
  void followsTheHeadOfTheForkchoiceUpdates() {
    final BlockHeader header = new BlockHeaderTestFixture().number(42).buildHeader();

    tracker.onNewPayload(header);
    tracker.onNewUnverifiedForkchoice(new ForkchoiceEvent(header.getHash(), Hash.ZERO, Hash.ZERO));

    assertThat(tracker.headHash()).contains(header.getHash());
    assertThat(tracker.knownHeader(header.getHash())).contains(header);
    assertThat(tracker.getBestChainHeight()).isEqualTo(42);
  }

  @Test
  void knowsOnlyTheHeadersOfReceivedPayloads() {
    final BlockHeader header = new BlockHeaderTestFixture().number(7).buildHeader();

    tracker.onNewUnverifiedForkchoice(new ForkchoiceEvent(header.getHash(), Hash.ZERO, Hash.ZERO));

    assertThat(tracker.headHash()).contains(header.getHash());
    assertThat(tracker.knownHeader(header.getHash())).isEmpty();
    assertThat(tracker.getBestChainHeight()).isZero();
  }

  @Test
  void keepsTheHeadOnAForkchoiceWithoutHead() {
    final Hash head = new BlockHeaderTestFixture().number(1).buildHeader().getHash();
    tracker.onNewUnverifiedForkchoice(new ForkchoiceEvent(head, Hash.ZERO, Hash.ZERO));

    tracker.onNewUnverifiedForkchoice(new ForkchoiceEvent(Hash.ZERO, Hash.ZERO, Hash.ZERO));
    tracker.onNewUnverifiedForkchoice(new ForkchoiceEvent(null, Hash.ZERO, Hash.ZERO));

    assertThat(tracker.headHash()).contains(head);
  }

  @Test
  void forgetsTheOldestHeadersOnceTheCacheIsFull() {
    // one more header than the cache holds
    final List<BlockHeader> headers = new ArrayList<>();
    for (int i = 0; i <= ForkchoiceHeadTracker.CACHED_HEADERS; i++) {
      headers.add(new BlockHeaderTestFixture().number(i).buildHeader());
    }

    headers.forEach(tracker::onNewPayload);

    assertThat(tracker.knownHeader(headers.getFirst().getHash())).isEmpty();
    assertThat(tracker.knownHeader(headers.get(1).getHash())).contains(headers.get(1));
    assertThat(tracker.knownHeader(headers.getLast().getHash())).contains(headers.getLast());
  }

  @Test
  void runsTheCleanupOnClose() {
    final AtomicBoolean cleaned = new AtomicBoolean();

    new ForkchoiceHeadTracker(() -> cleaned.set(true)).close();

    assertThat(cleaned).isTrue();
  }
}
