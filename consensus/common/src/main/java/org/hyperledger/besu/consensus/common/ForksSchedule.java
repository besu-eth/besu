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
package org.hyperledger.besu.consensus.common;

import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * The Forks schedule.
 *
 * @param <C> the type parameter
 */
public class ForksSchedule<C> {

  // Earliest permitted timestamp for TIME-based forks (1st January 2023 00:00:00 UTC)
  private static final long MIN_TIMESTAMP_FORK_EPOCH_SECONDS = 1_672_531_200L;

  private final NavigableSet<ForkSpec<C>> forks =
      new TreeSet<>(
          Comparator.comparing((Function<ForkSpec<C>, Long>) ForkSpec::getBlock).reversed());

  /**
   * Instantiates a new Forks schedule.
   *
   * @param forks the forks
   */
  public ForksSchedule(final Collection<ForkSpec<C>> forks) {
    this.forks.addAll(forks);
  }

  /**
   * Apply the protocol schedule to the forks to assert their fork type - block or timestamp. Also
   * validates that no TIMESTAMP-type fork (BFT or EVM) has a value before 1st January 2023.
   *
   * @param protocolSchedule the protocol schedule
   * @param currentBlockNumber the chain's current head block number, used to determine whether the
   *     active EVM spec is timestamp-based; callers without access to the chain head should pass
   *     {@link Long#MAX_VALUE} to apply the most conservative validation
   */
  public void applyMilestoneTypes(
      final BftProtocolSchedule protocolSchedule, final long currentBlockNumber) {
    forks.forEach(
        f -> {
          f.setForkType(
              protocolSchedule.getSpecTypeByBlockNumberOrTimestamp(f.getBlock(), f.getBlock()));
          if (f.getForkType() == ForkSpec.ForkScheduleType.TIME
              && f.getBlock() < MIN_TIMESTAMP_FORK_EPOCH_SECONDS) {
            throw new IllegalArgumentException(
                String.format(
                    "Fork of type TIMESTAMP has block value %d which is before 1st January 2023"
                        + " (epoch seconds %d); timestamp-based forks earlier than this are not supported.",
                    f.getBlock(), MIN_TIMESTAMP_FORK_EPOCH_SECONDS));
          }
        });

    final ForkSpec.ForkScheduleType currentEvmSpecType =
        protocolSchedule.getSpecTypeByBlockNumberOrTimestamp(
            currentBlockNumber, currentBlockNumber);
    if (currentEvmSpecType == ForkSpec.ForkScheduleType.TIME) {
      final Optional<ScheduledProtocolSpec> invalidEvmSpec =
          protocolSchedule.getScheduledProtocolSpecs().stream()
              .filter(s -> s instanceof ScheduledProtocolSpec.TimestampProtocolSpec)
              .filter(s -> s.fork().milestone() < MIN_TIMESTAMP_FORK_EPOCH_SECONDS)
              .findFirst();
      if (invalidEvmSpec.isPresent()) {
        throw new IllegalArgumentException(
            String.format(
                "Fork of type TIMESTAMP has block value %d which is before 1st January 2023"
                    + " (epoch seconds %d); timestamp-based forks earlier than this are not supported.",
                invalidEvmSpec.get().fork().milestone(), MIN_TIMESTAMP_FORK_EPOCH_SECONDS));
      }
    }
  }

  /**
   * Gets fork.
   *
   * @param blockNumber the block number
   * @param blockTimestamp the block timestamp
   * @return the fork
   */
  public ForkSpec<C> getFork(final long blockNumber, final long blockTimestamp) {
    for (final ForkSpec<C> f : forks) {
      final long blockValue =
          f.getForkType() == ForkSpec.ForkScheduleType.TIME ? blockTimestamp : blockNumber;
      if (blockValue >= f.getBlock()) {
        return f;
      }
    }

    return forks.first();
  }

  /**
   * Gets forks.
   *
   * @return the forks
   */
  public Set<ForkSpec<C>> getForks() {
    return Collections.unmodifiableSet(forks);
  }
}
