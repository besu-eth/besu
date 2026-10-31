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

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;

import java.util.HashSet;
import java.util.Set;

/**
 * The empty answers in a row of peers that served the state, which tell that the pivot got too old
 * once twelve came from three peers, or from every peer left when fewer serve. Not thread safe: the
 * loop of the state download owns it.
 */
final class EmptyAnswers {

  private static final int IN_A_ROW = 12;
  private static final int PEERS = 3;

  private final Set<EthPeer> peers = new HashSet<>();
  private int inARow;

  /**
   * Records an empty answer of a peer that served state, and returns whether the pivot is too old.
   *
   * @param candidates the peers not left out
   */
  boolean add(final EthPeer peer, final int candidates) {
    inARow++;
    peers.add(peer);
    return inARow >= IN_A_ROW && peers.size() >= Math.min(PEERS, candidates);
  }

  /** Forgets a peer left out, whose answers say nothing about the pivot. */
  void forget(final EthPeer peer) {
    peers.remove(peer);
  }

  /** Starts over, after an answer with state or a pivot move. */
  void clear() {
    inARow = 0;
    peers.clear();
  }
}
