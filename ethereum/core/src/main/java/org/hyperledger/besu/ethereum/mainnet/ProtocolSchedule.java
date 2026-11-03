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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.ChainId;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.PermissionTransactionFilter;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;
import org.hyperledger.besu.plugin.services.txvalidator.TransactionValidationRule;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public interface ProtocolSchedule {

  ProtocolSpec getByBlockHeader(final ProcessableBlockHeader blockHeader);

  default ProtocolSpec getForNextBlockHeader(
      final org.hyperledger.besu.plugin.data.BlockHeader parentBlockHeader,
      final long timestampForNextBlock) {
    final BlockHeader nextBlockHeader =
        BlockHeaderBuilder.fromHeader(parentBlockHeader)
            .difficulty(Difficulty.ZERO)
            .number(parentBlockHeader.getNumber() + 1)
            .timestamp(timestampForNextBlock)
            .parentHash(parentBlockHeader.getBlockHash())
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .buildBlockHeader();
    return getByBlockHeader(nextBlockHeader);
  }

  Optional<ScheduledProtocolSpec> getNextProtocolSpec(final long currentTime);

  Optional<ScheduledProtocolSpec> getLatestProtocolSpec();

  ChainId getChainId();

  String listMilestones();

  void putBlockNumberMilestone(final long blockNumber, final ProtocolSpec protocolSpec);

  void putTimestampMilestone(final long timestamp, final ProtocolSpec protocolSpec);

  default void setMilestones(final Map<HardforkId, Long> milestoneList) {
    throw new UnsupportedOperationException("Not implemented");
  }

  default Optional<ScheduledProtocolSpec.Hardfork> hardforkFor(
      final Predicate<ScheduledProtocolSpec> predicate) {
    throw new UnsupportedOperationException("Not implemented");
  }

  default Optional<Long> milestoneFor(final HardforkId hardforkId) {
    throw new UnsupportedOperationException("Not implemented");
  }

  boolean isOnMilestoneBoundary(final BlockHeader blockHeader);

  boolean anyMatch(Predicate<ScheduledProtocolSpec> predicate);

  void setPermissionTransactionFilter(
      final PermissionTransactionFilter permissionTransactionFilter);

  void setAdditionalValidationRules(
      final List<TransactionValidationRule> additionalValidationRules);
}
