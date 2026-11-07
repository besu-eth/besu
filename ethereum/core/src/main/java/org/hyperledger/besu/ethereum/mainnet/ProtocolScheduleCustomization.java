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

import org.hyperledger.besu.config.ForkIdActivations;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.plugin.Unstable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.LongStream;

/**
 * The immutable, fully resolved contribution of one protocol-schedule customizer.
 *
 * <p>EIP-2124 activations are derived from the modifications rather than supplied separately, so
 * the two lists cannot disagree: every modification's activation is advertised, and nothing else
 * is. That is bookkeeping between the lists, no more. A fork ID commits peers to the same
 * boundaries, not to the same rules behind them, and whether a modification changes anything at its
 * boundary is the customizer's to get right.
 */
@Unstable
public record ProtocolScheduleCustomization(
    String name, List<ProtocolSpecModification> modifications) {

  /** Blocks the DAO recovery spans, from the fork to the restoration of the previous spec. */
  private static final long DAO_RECOVERY_LENGTH = 10L;

  private static final ProtocolScheduleCustomization EMPTY =
      new ProtocolScheduleCustomization("none", List.of());

  /** Creates and validates a resolved customization. */
  @SuppressWarnings(
      "MethodInputParametersMustBeFinal") // compact record constructors have implicit parameters
  public ProtocolScheduleCustomization {
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("A protocol-schedule customization name is required");
    }
    modifications =
        List.copyOf(Objects.requireNonNull(modifications, "modifications must not be null"));

    final Set<ProtocolScheduleActivation> modifierActivations = new LinkedHashSet<>();
    for (final ProtocolSpecModification modification : modifications) {
      Objects.requireNonNull(modification, "modifications must not contain null");
      if (!modifierActivations.add(modification.activation())) {
        throw new IllegalArgumentException(
            "More than one protocol-spec modification is registered at "
                + modification.activation());
      }
    }
  }

  /**
   * Rejects activations this schedule cannot honor for the given chain.
   *
   * <p>An activation the schedule will never reach would still be advertised in the fork ID, so it
   * is refused here rather than left to announce a boundary the node does not keep. A timestamp
   * activation must come after every block activation, and a block activation before the chain's
   * first fork timestamp: nothing orders the two domains against each other once the schedule is
   * built, so an inverted pair takes effect at a height its activation never reached. The DAO
   * recovery window counts as block activations through its final restoration, ten blocks past the
   * fork, even though the config does not list those. The recovery owns that whole span: it writes
   * its own spec at the fork and reinstates the previous one at the far end, so nothing contributed
   * can hold anywhere in it.
   *
   * <p>These comparisons are numeric across domains, as the builder's own fork-order validation is
   * for the forks the genesis config declares. They establish that the schedule can place every
   * activation in its own era; they cannot establish that the chain reaches its last block
   * activation before its first timestamp activation's time, which EIP-6122 requires of a chain and
   * which the fork ID relies on the same way -- {@code ForkIdManager} settles the block forks
   * before it consults a timestamp. That is the chain's obligation, held to the same standard as
   * its declared forks, and it is not checkable at startup: the timestamp of the last block
   * activation is unknown until the chain reaches it.
   *
   * @param config the genesis configuration this customization was resolved for
   * @throws IllegalStateException if an activation cannot be honored
   */
  void validateAgainst(final GenesisConfigOptions config) {
    if (modifications.isEmpty()) {
      return;
    }
    final OptionalLong firstForkTimestamp =
        config.getForkBlockTimestamps().stream().mapToLong(Long::longValue).min();
    final long lastBlockActivation =
        LongStream.of(
                modifications.stream()
                    .map(ProtocolSpecModification::activation)
                    .filter(ProtocolScheduleActivation.BlockNumber.class::isInstance)
                    .mapToLong(ProtocolScheduleActivation::value)
                    .max()
                    .orElse(0L),
                config.getForkBlockNumbers().stream().mapToLong(Long::longValue).max().orElse(0L),
                config.getDaoForkBlock().stream()
                    .map(dao -> dao + DAO_RECOVERY_LENGTH)
                    .max()
                    .orElse(0L))
            .max()
            .orElse(0L);

    for (final ProtocolSpecModification modification : modifications) {
      switch (modification.activation()) {
        case ProtocolScheduleActivation.Timestamp timestamp ->
            validateTimestamp(timestamp, lastBlockActivation);
        case ProtocolScheduleActivation.BlockNumber block ->
            validateBlockNumber(block, config, firstForkTimestamp);
      }
    }
  }

  private void validateTimestamp(
      final ProtocolScheduleActivation.Timestamp activation, final long lastBlockActivation) {
    // A chain with no block activation above genesis has no block era to invert, and rules at
    // genesis belong to the genesis hash rather than the fork ID.
    if (lastBlockActivation > 0 && activation.value() <= lastBlockActivation) {
      throw new IllegalStateException(
          refusal(activation.value(), "timestamp")
              + " is not after the last block activation "
              + lastBlockActivation);
    }
  }

  private void validateBlockNumber(
      final ProtocolScheduleActivation.BlockNumber activation,
      final GenesisConfigOptions config,
      final OptionalLong firstForkTimestamp) {
    if (firstForkTimestamp.isPresent() && activation.value() >= firstForkTimestamp.getAsLong()) {
      throw new IllegalStateException(
          refusal(activation.value(), "block")
              + " is not before the first fork timestamp "
              + firstForkTimestamp.getAsLong());
    }
    final OptionalLong daoForkBlock = config.getDaoForkBlock();
    if (daoForkBlock.isPresent()
        && activation.value() >= daoForkBlock.getAsLong()
        && activation.value() <= daoForkBlock.getAsLong() + DAO_RECOVERY_LENGTH) {
      throw new IllegalStateException(
          refusal(activation.value(), "block")
              + " is inside the DAO recovery window starting at "
              + daoForkBlock.getAsLong());
    }
  }

  private String refusal(final long activation, final String domain) {
    return "Protocol-schedule customization '"
        + name
        + "' activates at "
        + domain
        + " "
        + activation
        + ", which";
  }

  /** Returns the absence of a customization. */
  public static ProtocolScheduleCustomization none() {
    return EMPTY;
  }

  /**
   * Converts the typed fork activations to the configuration representation used by EIP-2124.
   *
   * <p>Every activation reaches one of the two lists: the switch is over a sealed type, so a domain
   * added later fails to compile here rather than dropping out of the advertised fork ID.
   */
  public ForkIdActivations toForkIdActivations() {
    final List<Long> blockNumbers = new ArrayList<>();
    final List<Long> timestamps = new ArrayList<>();
    for (final ProtocolSpecModification modification : modifications) {
      switch (modification.activation()) {
        case ProtocolScheduleActivation.BlockNumber block -> blockNumbers.add(block.value());
        case ProtocolScheduleActivation.Timestamp timestamp -> timestamps.add(timestamp.value());
      }
    }
    return new ForkIdActivations(blockNumbers, timestamps);
  }
}
