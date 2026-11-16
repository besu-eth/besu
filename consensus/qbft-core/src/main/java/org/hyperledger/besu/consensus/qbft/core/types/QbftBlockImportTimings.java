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
package org.hyperledger.besu.consensus.qbft.core.types;

import java.util.concurrent.TimeUnit;

/**
 * Timings of the local import of a QBFT block, a negative value means not measured.
 *
 * @param proposalValidationNanos time to validate, and so execute, the proposed block
 * @param proposalToCommitNanos time from the proposal being accepted to the block being committed
 * @param importNanos time to import the committed block to the chain
 */
public record QbftBlockImportTimings(
    long proposalValidationNanos, long proposalToCommitNanos, long importNanos) {

  private static String millis(final long nanos) {
    return nanos < 0 ? "n/a" : TimeUnit.NANOSECONDS.toMillis(nanos) + "ms";
  }

  @Override
  public String toString() {
    return "Timing(proposalValidation="
        + millis(proposalValidationNanos)
        + ", proposalToCommit="
        + millis(proposalToCommitNanos)
        + ", imported="
        + millis(importNanos)
        + ")";
  }
}
