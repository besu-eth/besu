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
package org.hyperledger.besu.ethereum.mainnet.milestones;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecBuilder;

import java.util.OptionalLong;
import java.util.function.Supplier;

/**
 * Configuration for a milestone definition.
 *
 * <p>A fork a chain does not configure activates with the next configured fork, unless it is {@code
 * optional}: a chain that does not configure an optional fork, such as the DAO fork, never has it.
 */
public record MilestoneDefinition(
    HardforkId hardforkId,
    OptionalLong blockNumberOrTimestamp,
    Supplier<ProtocolSpecBuilder> specBuilder,
    MilestoneType milestoneType,
    boolean optional) {

  static MilestoneDefinition createBlockNumberMilestone(
      final HardforkId hardforkId,
      final OptionalLong blockNumber,
      final Supplier<ProtocolSpecBuilder> specBuilder) {
    return new MilestoneDefinition(
        hardforkId, blockNumber, specBuilder, MilestoneType.BLOCK_NUMBER, false);
  }

  static MilestoneDefinition createOptionalBlockNumberMilestone(
      final HardforkId hardforkId,
      final OptionalLong blockNumber,
      final Supplier<ProtocolSpecBuilder> specBuilder) {
    return new MilestoneDefinition(
        hardforkId, blockNumber, specBuilder, MilestoneType.BLOCK_NUMBER, true);
  }

  static MilestoneDefinition createTimestampMilestone(
      final HardforkId hardforkId,
      final OptionalLong timestamp,
      final Supplier<ProtocolSpecBuilder> specBuilder) {
    return new MilestoneDefinition(
        hardforkId, timestamp, specBuilder, MilestoneType.TIMESTAMP, false);
  }
}
