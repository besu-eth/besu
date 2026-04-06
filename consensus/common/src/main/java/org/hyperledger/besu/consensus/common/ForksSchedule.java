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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * The Forks schedule.
 *
 * @param <C> the type parameter
 */
public class ForksSchedule<C> {

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
   * Apply the protocol schedule to the forks to assert their fork type - block or timestamp
   *
   * @param protocolSchedule the protocol schedule
   */
  public void applyMilestoneTypes(final BftProtocolSchedule protocolSchedule) {
    forks.forEach(
        f ->
            f.setForkType(
                protocolSchedule.getSpecTypeByBlockNumberOrTimestamp(f.getBlock(), f.getBlock())));
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
