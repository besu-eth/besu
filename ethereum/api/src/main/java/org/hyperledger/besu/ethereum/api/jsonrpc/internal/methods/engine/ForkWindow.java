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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.Optional;

/**
 * Declares the fork timestamp range in which an engine API method version is valid.
 *
 * <p>Use {@link #unbounded()} for versions with no fork gate, {@link #from(HardforkId)} for
 * versions valid from a given fork onward, or {@link #range(HardforkId, HardforkId)} for versions
 * valid in a half-open range {@code [fromFork, untilFork)}.
 */
public final class ForkWindow {

  private final Optional<HardforkId> fromFork;
  private final Optional<HardforkId> untilFork;

  private ForkWindow(final Optional<HardforkId> fromFork, final Optional<HardforkId> untilFork) {
    this.fromFork = fromFork;
    this.untilFork = untilFork;
  }

  public static ForkWindow unbounded() {
    return new ForkWindow(Optional.empty(), Optional.empty());
  }

  public static ForkWindow from(final HardforkId fromFork) {
    return new ForkWindow(Optional.of(fromFork), Optional.empty());
  }

  public static ForkWindow range(final HardforkId fromFork, final HardforkId untilFork) {
    return new ForkWindow(Optional.of(fromFork), Optional.of(untilFork));
  }

  public ValidationResult<RpcErrorType> validate(
      final long blockTimestamp, final ProtocolSchedule protocolSchedule) {
    if (fromFork.isPresent()) {
      final Optional<Long> fromMilestone = protocolSchedule.milestoneFor(fromFork.get());
      if (fromMilestone.isEmpty()) {
        return ValidationResult.invalid(
            RpcErrorType.UNSUPPORTED_FORK,
            "Configuration error, no schedule for " + fromFork.get().name() + " fork set");
      }
      if (Long.compareUnsigned(blockTimestamp, fromMilestone.get()) < 0) {
        return ValidationResult.invalid(
            RpcErrorType.UNSUPPORTED_FORK,
            fromFork.get().name() + " configured to start at timestamp: " + fromMilestone.get());
      }
    }

    if (untilFork.isPresent()) {
      final Optional<Long> untilMilestone = protocolSchedule.milestoneFor(untilFork.get());
      if (untilMilestone.isPresent()
          && Long.compareUnsigned(blockTimestamp, untilMilestone.get()) >= 0) {
        return ValidationResult.invalid(
            RpcErrorType.UNSUPPORTED_FORK,
            "block timestamp "
                + blockTimestamp
                + " is after the first unsupported milestone: "
                + untilFork.get().name()
                + " at timestamp "
                + untilMilestone.get());
      }
    }

    return ValidationResult.valid();
  }
}
