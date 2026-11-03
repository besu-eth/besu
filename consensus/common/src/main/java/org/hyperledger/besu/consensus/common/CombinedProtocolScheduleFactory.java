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

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.datatypes.ChainId;
import org.hyperledger.besu.ethereum.mainnet.DefaultProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec;

import java.util.NavigableSet;
import java.util.Optional;
import java.util.function.Predicate;

/** The Combined protocol schedule factory. */
public class CombinedProtocolScheduleFactory {
  /** Default constructor. */
  public CombinedProtocolScheduleFactory() {}

  /**
   * Create protocol schedule.
   *
   * @param forkSpecs the fork specs
   * @param chainId the chain id
   * @return the protocol schedule
   */
  public BftProtocolSchedule create(
      final NavigableSet<ForkSpec<ProtocolSchedule>> forkSpecs, final ChainId chainId) {
    final BftProtocolSchedule combinedProtocolSchedule =
        new BftProtocolSchedule(new DefaultProtocolSchedule(chainId));
    for (ForkSpec<ProtocolSchedule> spec : forkSpecs) {
      checkState(
          spec.getValue() instanceof DefaultProtocolSchedule,
          "Consensus migration requires a DefaultProtocolSchedule");
      final BftProtocolSchedule protocolSchedule =
          new BftProtocolSchedule((DefaultProtocolSchedule) spec.getValue());

      final Optional<Long> endBlock =
          Optional.ofNullable(forkSpecs.higher(spec)).map(ForkSpec::getBlock);
      protocolSchedule.getScheduledProtocolSpecs().stream()
          .filter(protocolSpecMatchesConsensusBlockRange(spec.getBlock(), endBlock))
          .forEach(
              s -> {
                if (s instanceof ScheduledProtocolSpec.TimestampProtocolSpec) {
                  combinedProtocolSchedule.putTimestampMilestone(s.fork().milestone(), s.spec());
                } else if (s instanceof ScheduledProtocolSpec.BlockNumberProtocolSpec) {
                  combinedProtocolSchedule.putBlockNumberMilestone(s.fork().milestone(), s.spec());
                } else {
                  throw new IllegalStateException(
                      "Unexpected milestone: " + s + " for milestone: " + s.fork().milestone());
                }
              });

      // When moving to a new consensus mechanism we want to use the last milestone but created by
      // our consensus mechanism's BesuControllerBuilder so any additional rules are applied
      if (spec.getBlock() > 0) {
        combinedProtocolSchedule.putBlockNumberMilestone(
            spec.getBlock(), protocolSchedule.getByBlockNumberOrTimestamp(spec.getBlock(), 0L));
      }
    }
    return combinedProtocolSchedule;
  }

  private Predicate<ScheduledProtocolSpec> protocolSpecMatchesConsensusBlockRange(
      final long startBlock, final Optional<Long> endBlock) {
    return scheduledProtocolSpec ->
        scheduledProtocolSpec.fork().milestone() >= startBlock
            && endBlock.map(b -> scheduledProtocolSpec.fork().milestone() < b).orElse(true);
  }
}
