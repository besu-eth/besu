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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class StateServersTest {

  private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

  private final StateServers servers = new StateServers();
  private final EthPeer serving = peer();
  private final EthPeer lazy = peer();

  @Test
  void leavesOutAPeerThatAnswersNothingForAPivotAnotherPeerServes() {
    servers.served(serving, 0);

    assertThat(servers.empty(lazy, 0, 0)).isTrue();
    assertThat(servers.excluded(0)).containsExactly(lazy);
    verify(lazy).setIsServingSnap(false);
    assertThat(servers.hasServed(serving)).isTrue();
    assertThat(servers.hasServed(lazy)).isFalse();
  }

  @Test
  void keepsAPeerWhoseEmptyAnswerFollowsServingThePivot() {
    servers.served(serving, 0);

    // it served this pivot before: the pivot is getting too old
    assertThat(servers.empty(serving, 0, 0)).isFalse();
    assertThat(servers.excluded(0)).isEmpty();
  }

  @Test
  void leavesOutAPeerThatAnswersNothingForTwoPivotsInARow() {
    assertThat(servers.empty(lazy, 0, 0)).isFalse();

    assertThat(servers.empty(lazy, 1, 0)).isTrue();
  }

  @Test
  void probesALeftOutPeerAgainLater() {
    servers.served(serving, 0);
    servers.empty(lazy, 0, 0);

    assertThat(servers.excluded(29 * SECOND)).contains(lazy);
    assertThat(servers.excluded(31 * SECOND)).isEmpty();
    // a second strike leaves it out longer
    servers.empty(lazy, 0, 31 * SECOND);
    assertThat(servers.excluded(80 * SECOND)).contains(lazy);
  }

  @Test
  void countsTheEmptyAnswersOfALeftOutPeerAsOneStrike() {
    servers.served(serving, 0);

    // the answers to requests sent together
    assertThat(servers.empty(lazy, 0, 0)).isTrue();
    assertThat(servers.empty(lazy, 0, SECOND)).isTrue();
    assertThat(servers.empty(lazy, 0, 2 * SECOND)).isTrue();

    assertThat(servers.excluded(31 * SECOND)).isEmpty();
  }

  @Test
  void takesBackAPeerThatServesAgain() {
    servers.served(serving, 0);
    servers.empty(lazy, 0, 0);

    servers.served(lazy, 1);

    assertThat(servers.excluded(0)).isEmpty();
  }

  @Test
  void keepsOutAPeerWhoseLateAnswerServesAnOlderPivot() {
    servers.served(serving, 1);
    servers.empty(lazy, 1, 0);

    servers.served(lazy, 0);

    assertThat(servers.excluded(0)).containsExactly(lazy);
  }

  @Test
  void leavesOutAPeerTenMinutesAtMost() {
    servers.served(serving, 0);
    // five strikes, each once the last one ended: 30 s, then 1, 2, 4 and 8 minutes
    long now = 0;
    for (int strike = 0; strike < 5; strike++) {
      servers.empty(lazy, 0, now);
      now += (30 * SECOND) << strike;
    }

    // the sixth would double the wait to 16 minutes
    servers.empty(lazy, 0, now);

    assertThat(servers.excluded(now + 599 * SECOND)).contains(lazy);
    assertThat(servers.excluded(now + 601 * SECOND)).isEmpty();
  }

  @Test
  void forgetsDisconnectedPeers() {
    servers.served(serving, 0);
    servers.empty(lazy, 0, 0);
    when(lazy.isDisconnected()).thenReturn(true);
    when(serving.isDisconnected()).thenReturn(true);

    assertThat(servers.excluded(0)).isEmpty();
    assertThat(servers.hasServed(serving)).isFalse();
  }

  private static EthPeer peer() {
    return mock(EthPeer.class);
  }
}
