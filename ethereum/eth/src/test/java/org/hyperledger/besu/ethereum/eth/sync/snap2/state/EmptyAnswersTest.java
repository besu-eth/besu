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

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class EmptyAnswersTest {

  private final List<EthPeer> peers = Stream.generate(() -> mock(EthPeer.class)).limit(3).toList();
  private final EmptyAnswers answers = new EmptyAnswers();

  @Test
  void tellsThePivotTooOldAtTheTwelfthEmptyAnswerFromThreePeers() {
    assertThat(addInTurn(11)).isFalse();

    assertThat(answers.add(peers.getFirst(), 3)).isTrue();
  }

  @Test
  void needsThreePeersOrEveryPeerLeft() {
    final EthPeer first = peers.get(0);
    final EthPeer second = peers.get(1);
    for (int i = 0; i < 6; i++) {
      answers.add(first, 3);
      answers.add(second, 3);
    }

    assertThat(answers.add(first, 3)).isFalse();
    assertThat(answers.add(first, 2)).isTrue();
  }

  @Test
  void startsOverAfterAnAnswerWithState() {
    addInTurn(11);
    answers.clear();

    assertThat(addInTurn(11)).isFalse();
    assertThat(answers.add(peers.getFirst(), 3)).isTrue();
  }

  @Test
  void forgetsAPeerLeftOut() {
    addInTurn(12);
    answers.forget(peers.getLast());

    assertThat(answers.add(peers.getFirst(), 3)).isFalse();
    assertThat(answers.add(peers.getLast(), 3)).isTrue();
  }

  // empty answers from the peers in turn, returns whether the last made the pivot too old
  private boolean addInTurn(final int count) {
    boolean tooOld = false;
    for (int i = 0; i < count; i++) {
      tooOld = answers.add(peers.get(i % peers.size()), peers.size());
    }
    return tooOld;
  }
}
