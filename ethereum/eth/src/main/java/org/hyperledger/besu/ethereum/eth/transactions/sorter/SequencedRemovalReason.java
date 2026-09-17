/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.transactions.sorter;

import org.hyperledger.besu.ethereum.eth.transactions.RemovalReason;

import java.util.Locale;

/** The reason why a pending tx has been removed */
enum SequencedRemovalReason implements RemovalReason {
  EVICTED(true, false),
  TIMED_EVICTION(true, false),
  REPLACED(false, true),
  INVALID(false, true);

  private final String label;
  private final boolean stopTracking;
  private final boolean stopBroadcasting;

  SequencedRemovalReason(final boolean stopTracking, final boolean stopBroadcasting) {
    this.label = name().toLowerCase(Locale.ROOT);
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
