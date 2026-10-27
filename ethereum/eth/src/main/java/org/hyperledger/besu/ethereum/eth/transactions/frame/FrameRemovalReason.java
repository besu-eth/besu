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
package org.hyperledger.besu.ethereum.eth.transactions.frame;

import org.hyperledger.besu.ethereum.eth.transactions.RemovalReason;

import java.util.Locale;

/**
 * Why a frame transaction left the pool. EIP-8141 forbids re-propagating evicted or replaced
 * transactions, but lets an evicted one be admitted again when it is received again, since the
 * state its prefix reads may have changed.
 */
public enum FrameRemovalReason implements RemovalReason {
  /** Included in an imported block. */
  CONFIRMED(false, true),
  /** Replaced by a transaction with the same nonce identity and higher fees. */
  REPLACED(false, true),
  /** Evicted to make room, or expired by retention. */
  EVICTED(true, true),
  /** No longer admissible against the head, or rejected by block building. */
  INVALIDATED(true, true);

  private final String label;
  private final boolean stopTracking;
  private final boolean stopBroadcasting;

  FrameRemovalReason(final boolean stopTracking, final boolean stopBroadcasting) {
    this.label = "frame_" + name().toLowerCase(Locale.ROOT);
    this.stopTracking = stopTracking;
    this.stopBroadcasting = stopBroadcasting;
  }

  @Override
  public String label() {
    return label;
  }

  @Override
  public boolean stopTracking() {
    return stopTracking;
  }

  @Override
  public boolean stopBroadcasting() {
    return stopBroadcasting;
  }
}
