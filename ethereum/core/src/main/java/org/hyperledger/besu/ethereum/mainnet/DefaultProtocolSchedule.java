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

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.PermissionTransactionFilter;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec.BlockNumberProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec.TimestampProtocolSpec;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;
import org.hyperledger.besu.plugin.services.txvalidator.TransactionValidationRule;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

public class DefaultProtocolSchedule implements ProtocolSchedule {

  @VisibleForTesting
  protected NavigableSet<ScheduledProtocolSpec> protocolSpecs =
      new TreeSet<>(Comparator.comparing(ScheduledProtocolSpec::fork).reversed());

  private final Map<HardforkId, Long> milestones = new HashMap<>();

  private final Optional<BigInteger> chainId;

  public DefaultProtocolSchedule(final Optional<BigInteger> chainId) {
    this.chainId = chainId;
  }

  @VisibleForTesting
  protected DefaultProtocolSchedule(final DefaultProtocolSchedule protocolSchedule) {
    this.chainId = protocolSchedule.chainId;
    this.protocolSpecs = protocolSchedule.protocolSpecs;
    // Copy the milestones map populated by ProtocolScheduleBuilder.setMilestones(...). Without
    // this, subclasses that wrap an existing schedule via this copy constructor (e.g.
    // BftProtocolSchedule) keep an empty milestones map even though protocolSpecs are inherited,
    // and milestoneFor(HardforkId) returns Optional.empty() for every fork. Engine API handlers
    // (e.g. EngineForkchoiceUpdatedV3) cache those Optionals at construction time and then reject
    // every payload at the Cancun/Amsterdam boundary with UNSUPPORTED_FORK.
    this.milestones.putAll(protocolSchedule.milestones);
  }

  @Override
  public ProtocolSpec getByBlockHeader(final ProcessableBlockHeader blockHeader) {
    checkArgument(
        !protocolSpecs.isEmpty(), "At least 1 milestone must be provided to the protocol schedule");
    checkArgument(
        protocolSpecs.last().fork().milestone() == 0,
        "There must be a milestone starting from block 0");

    // protocolSpecs is sorted in descending block order, so the first one we find that's lower than
    // the requested level will be the most appropriate spec
    for (final ScheduledProtocolSpec spec : protocolSpecs) {
      if (spec.isOnOrAfterMilestoneBoundary(blockHeader)) {
        return spec.spec();
      }
    }
    return null;
  }

  @Override
  public Optional<ScheduledProtocolSpec> getNextProtocolSpec(final long currentTime) {
    checkArgument(
        !protocolSpecs.isEmpty(), "At least 1 milestone must be provided to the protocol schedule");
    checkArgument(
        protocolSpecs.last().fork().milestone() == 0,
        "There must be a milestone starting from block 0");

    ScheduledProtocolSpec chosenSpec = null;
    for (final ScheduledProtocolSpec spec : protocolSpecs) {
      if (currentTime < spec.fork().milestone()
          && (chosenSpec == null || chosenSpec.fork().milestone() > spec.fork().milestone())) {
        // if we haven't chosen a valid spec, or we find an earlier spec, choose it.
        chosenSpec = spec;
      }
    }
    return Optional.ofNullable(chosenSpec);
  }

  /**
   * spec from the genesis file with the max ie latest timestamp
   *
   * @return the last defined spec from the genesis file
   */
  @Override
  public Optional<ScheduledProtocolSpec> getLatestProtocolSpec() {
    checkArgument(
        !protocolSpecs.isEmpty(), "At least 1 milestone must be provided to the protocol schedule");
    checkArgument(
        protocolSpecs.last().fork().milestone() == 0,
        "There must be a milestone starting from block 0");
    return protocolSpecs.stream().max(Comparator.comparing(ScheduledProtocolSpec::fork));
  }

  @Override
  public Optional<BigInteger> getChainId() {
    return chainId;
  }

  @Override
  public String listMilestones() {
    return protocolSpecs.stream()
        .sorted(Comparator.comparing(ScheduledProtocolSpec::fork))
        .map(scheduledSpec -> scheduledSpec.fork().toString())
        .collect(Collectors.joining(", ", "[", "]"));
  }

  @Override
  public boolean isOnMilestoneBoundary(final BlockHeader blockHeader) {
    return this.protocolSpecs.stream().anyMatch(s -> s.isOnMilestoneBoundary(blockHeader));
  }

  @Override
  public void putBlockNumberMilestone(final long blockNumber, final ProtocolSpec protocolSpec) {
    putMilestone(BlockNumberProtocolSpec.create(blockNumber, protocolSpec));
  }

  @Override
  public void putTimestampMilestone(final long timestamp, final ProtocolSpec protocolSpec) {
    putMilestone(TimestampProtocolSpec.create(timestamp, protocolSpec));
  }

  @Override
  public void setMilestones(final Map<HardforkId, Long> milestones) {
    this.milestones.clear();
    this.milestones.putAll(milestones);
  }

  private void putMilestone(final ScheduledProtocolSpec scheduledProtocolSpec) {
    // Ensure this replaces any existing spec at the same block number.
    protocolSpecs.remove(scheduledProtocolSpec);
    protocolSpecs.add(scheduledProtocolSpec);
  }

  @Override
  public boolean anyMatch(final Predicate<ScheduledProtocolSpec> predicate) {
    return this.protocolSpecs.stream().anyMatch(predicate);
  }

  @Override
  public Optional<ScheduledProtocolSpec.Hardfork> hardforkFor(
      final Predicate<ScheduledProtocolSpec> predicate) {
    return this.protocolSpecs.stream()
        .filter(predicate)
        .findFirst()
        .map(ScheduledProtocolSpec::fork);
  }

  @Override
  public Optional<Long> milestoneFor(final HardforkId hardforkId) {
    return Optional.ofNullable(milestones.get(hardforkId));
  }

  @Override
  public void setPermissionTransactionFilter(
      final PermissionTransactionFilter permissionTransactionFilter) {
    protocolSpecs.forEach(
        spec ->
            spec.spec()
                .getTransactionValidatorFactory()
                .setPermissionTransactionFilter(permissionTransactionFilter));
  }

  @Override
  public void setAdditionalValidationRules(
      final List<TransactionValidationRule> additionalValidationRules) {
    protocolSpecs.forEach(
        spec ->
            spec.spec()
                .getTransactionValidatorFactory()
                .setAdditionalValidationRules(additionalValidationRules));
  }
}
