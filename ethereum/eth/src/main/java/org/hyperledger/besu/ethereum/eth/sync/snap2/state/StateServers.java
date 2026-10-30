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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tells, from their answers, which peers serve the state of the pivot. A peer that answers nothing
 * for a pivot another peer serves, or for two pivots in a row, is left out of state requests for a
 * while and stops counting as a snap server. Not thread safe: the download loop owns it.
 */
final class StateServers {

  private static final long FIRST_EXCLUSION_NANOS = TimeUnit.SECONDS.toNanos(30);
  private static final long MAX_EXCLUSION_NANOS = TimeUnit.MINUTES.toNanos(10);

  private final Map<EthPeer, Answers> answers = new HashMap<>();
  private int lastServedEpoch = -1;

  /** Records that a peer served the state of a pivot. */
  void served(final EthPeer peer, final int epoch) {
    final Answers record = answers.computeIfAbsent(peer, p -> new Answers());
    record.servedEpoch = Math.max(record.servedEpoch, epoch);
    lastServedEpoch = Math.max(lastServedEpoch, epoch);
    // a late answer for an older pivot does not take back a peer left out at a newer one
    if (epoch < record.emptyEpoch) {
      return;
    }
    record.strikes = 0;
    record.excludedUntil = 0;
    peer.setIsServingSnap(true);
  }

  /**
   * Records an empty answer for the state of a pivot.
   *
   * @return whether the peer is now left out, so that its answer says nothing about the pivot
   */
  boolean empty(final EthPeer peer, final int epoch, final long now) {
    final Answers record = answers.computeIfAbsent(peer, p -> new Answers());
    if (record.emptyEpoch != epoch) {
      record.previousEmptyEpoch = record.emptyEpoch;
      record.emptyEpoch = epoch;
    }
    final boolean othersServe = lastServedEpoch >= epoch && record.servedEpoch < epoch;
    final boolean twoPivots =
        record.previousEmptyEpoch == epoch - 1 && record.servedEpoch < epoch - 1;
    if (!othersServe && !twoPivots) {
      return false;
    }
    // requests sent together come back empty together: a peer left out takes no more strikes
    if (record.excludedUntil > now) {
      return true;
    }
    record.strikes++;
    record.excludedUntil =
        now
            + Math.min(
                MAX_EXCLUSION_NANOS, FIRST_EXCLUSION_NANOS << Math.min(record.strikes - 1, 5));
    peer.setIsServingSnap(false);
    return true;
  }

  /** Returns whether a peer ever served state. */
  boolean hasServed(final EthPeer peer) {
    final Answers record = answers.get(peer);
    return record != null && record.servedEpoch >= 0;
  }

  /** Returns the number of peers that served state and are not left out at this time. */
  int serving(final long now) {
    return (int)
        answers.values().stream()
            .filter(record -> record.servedEpoch >= 0 && record.excludedUntil <= now)
            .count();
  }

  /** Returns the peers left out at this time, forgetting the disconnected ones. */
  Set<EthPeer> excluded(final long now) {
    answers.keySet().removeIf(EthPeer::isDisconnected);
    final Set<EthPeer> out = new HashSet<>();
    answers.forEach(
        (peer, record) -> {
          if (record.excludedUntil > now) {
            out.add(peer);
          }
        });
    return out;
  }

  private static final class Answers {
    private int servedEpoch = -1;
    private int emptyEpoch = -1;
    private int previousEmptyEpoch = -2;
    private int strikes;
    private long excludedUntil;
  }
}
