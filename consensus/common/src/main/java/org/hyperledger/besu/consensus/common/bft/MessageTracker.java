/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.consensus.common.bft;

import static java.util.Collections.newSetFromMap;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.Set;

import org.apache.tuweni.bytes.Bytes32;

/** The Message tracker. */
public class MessageTracker {
  private final Set<Bytes32> seenMessages;

  /**
   * Instantiates a new Message tracker.
   *
   * @param messageTrackingLimit the message tracking limit
   */
  public MessageTracker(final int messageTrackingLimit) {
    this.seenMessages = newSetFromMap(new SizeLimitedMap<>(messageTrackingLimit));
  }

  /**
   * Add seen message.
   *
   * @param hash the pre-computed message hash
   */
  public void addSeenMessage(final Bytes32 hash) {
    seenMessages.add(hash);
  }

  /**
   * Add seen message.
   *
   * @param message the message
   */
  public void addSeenMessage(final MessageData message) {
    addSeenMessage(Hash.sha256(message.getData()));
  }

  /**
   * Has seen message.
   *
   * @param hash the pre-computed message hash
   * @return the boolean
   */
  public boolean hasSeenMessage(final Bytes32 hash) {
    return seenMessages.contains(hash);
  }

  /**
   * Has seen message.
   *
   * @param message the message
   * @return the boolean
   */
  public boolean hasSeenMessage(final MessageData message) {
    return hasSeenMessage(Hash.sha256(message.getData()));
  }
}
