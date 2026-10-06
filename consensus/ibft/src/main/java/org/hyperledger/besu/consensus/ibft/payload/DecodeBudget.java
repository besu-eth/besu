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
package org.hyperledger.besu.consensus.ibft.payload;

import org.hyperledger.besu.consensus.common.bft.messagewrappers.BftMessage;
import org.hyperledger.besu.ethereum.rlp.RLPException;

/** Limits the number of signed payloads recovered while decoding a single IBFT2 message. */
public final class DecodeBudget {

  private int remaining;

  /**
   * Instantiates a new decode budget.
   *
   * @param maxSignedPayloads the maximum number of signed payloads that may be recovered
   */
  public DecodeBudget(final int maxSignedPayloads) {
    this.remaining = maxSignedPayloads;
  }

  /**
   * Budget for an IBFT2 message, derived from the current validator count. A valid {@code Proposal}
   * holds at most {@code (V + 1)^2} signed payloads ({@code V} round changes, each with up to
   * {@code V} prepares) — an attacker cannot force more recoveries than that honest maximum for the
   * current validator set.
   *
   * @param validatorCount the current validator-set size
   * @return a new decode budget
   */
  public static DecodeBudget forIbftMessage(final int validatorCount) {
    final long v = Math.clamp(validatorCount, 0, BftMessage.MAX_LIST_ENTRIES);
    return new DecodeBudget((int) ((v + 1) * (v + 1)));
  }

  /**
   * A budget that adds no limit beyond the structural per-list caps, for decode paths that are
   * post-authentication (gossip re-decode) or whose nesting is already additively (not
   * multiplicatively) bounded by those caps, such as a standalone {@code RoundChange}.
   *
   * @return a new, effectively unlimited, decode budget
   */
  public static DecodeBudget forSingleMessage() {
    return new DecodeBudget(Integer.MAX_VALUE);
  }

  /**
   * Charges for one signed payload about to be recovered. Must be called <em>before</em> the
   * recovery so an exhausted budget short-circuits the {@code ecrecover}.
   *
   * @throws RLPException if the budget is exhausted
   */
  public void chargeSignedPayload() {
    if (remaining <= 0) {
      throw new RLPException(
          "Signed payloads in message exceed the maximum permitted total for the validator set");
    }
    remaining--;
  }
}
