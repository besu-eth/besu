/*
 * Copyright 2020 ConsenSys AG.
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

import org.hyperledger.besu.plugin.Unstable;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

/**
 * Modifiers applied to the protocol specs a schedule builds.
 *
 * <p>Two kinds of modifier live here. Structural modifiers come from a consensus mechanism, keyed
 * by a value that may be either a block number or a timestamp: BFT and Clique read transitions from
 * the genesis config, which states one number without saying which it is, and the domain is settled
 * later by where the value falls among the milestones. Those keep the single, magnitude-ordered
 * lookup they have always had.
 *
 * <p>Modifiers contributed by a {@link ProtocolScheduleCustomization} declare their domain, so they
 * are kept in separate maps and never ordered against a value from the other domain.
 */
public class ProtocolSpecAdapters {

  private final NavigableMap<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>>
      structuralModifiers;
  private final NavigableMap<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>>
      customBlockModifiers;
  private final NavigableMap<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>>
      customTimestampModifiers;

  private final ProtocolScheduleCustomization customization;

  /**
   * Creates adapters from structural modifiers whose activation domain is not yet known.
   *
   * @param modifiers modifiers keyed by block number or timestamp
   */
  public ProtocolSpecAdapters(
      final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> modifiers) {
    this(modifiers, Map.of(), Map.of(), ProtocolScheduleCustomization.none());
  }

  private ProtocolSpecAdapters(
      final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> structuralModifiers,
      final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> customBlockModifiers,
      final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> customTimestampModifiers,
      final ProtocolScheduleCustomization customization) {
    this.structuralModifiers = new TreeMap<>(structuralModifiers);
    this.customBlockModifiers = new TreeMap<>(customBlockModifiers);
    this.customTimestampModifiers = new TreeMap<>(customTimestampModifiers);
    this.customization = customization;
  }

  /**
   * The customization these adapters were composed from, which the schedule builder validates
   * against the chain before it builds anything.
   *
   * @return the customization, or the empty one
   */
  ProtocolScheduleCustomization customization() {
    return customization;
  }

  public static ProtocolSpecAdapters create(
      final long blockNumberOrTimestamp,
      final Function<ProtocolSpecBuilder, ProtocolSpecBuilder> modifier) {
    final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> entries = new HashMap<>();
    entries.put(blockNumberOrTimestamp, modifier);
    return new ProtocolSpecAdapters(entries);
  }

  /**
   * Combines a consensus mechanism's structural modifiers with one resolved customization.
   *
   * <p>Structural modifiers run first where both apply. Within an activation domain the greatest
   * activation not after the requested block or timestamp supplies that era's overlay. The two
   * contributed domains do not reach into each other: a block number and a timestamp cannot be
   * ordered against one another, so carrying a block-era overlay into the timestamp era would apply
   * it at heights its own activation has not reached. A chain whose block-era rules continue past
   * its first timestamp fork restates them in that timestamp modification.
   *
   * <p>Alongside a non-empty customization the structural modifiers must activate at milestones the
   * genesis config declares; the schedule builder refuses anything else, because a structural entry
   * that falls between milestones shares an instance the contributed overlay has already mutated.
   *
   * @param structuralModifiers the consensus mechanism's own modifiers
   * @param customization the centrally resolved contribution
   * @return the combined adapters
   */
  @Unstable
  public static ProtocolSpecAdapters compose(
      final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> structuralModifiers,
      final ProtocolScheduleCustomization customization) {
    final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> blockModifiers =
        new HashMap<>();
    final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> timestampModifiers =
        new HashMap<>();

    customization
        .modifications()
        .forEach(
            modification -> {
              final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> target =
                  switch (modification.activation()) {
                    case ProtocolScheduleActivation.BlockNumber ignored -> blockModifiers;
                    case ProtocolScheduleActivation.Timestamp ignored -> timestampModifiers;
                  };
              target.put(modification.activation().value(), modification.modifier());
            });
    return new ProtocolSpecAdapters(
        structuralModifiers, blockModifiers, timestampModifiers, customization);
  }

  public Function<ProtocolSpecBuilder, ProtocolSpecBuilder> getModifierForBlock(
      final long blockNumberOrTimestamp) {
    return combine(
        modifierAt(structuralModifiers, blockNumberOrTimestamp),
        modifierAt(customBlockModifiers, blockNumberOrTimestamp));
  }

  Function<ProtocolSpecBuilder, ProtocolSpecBuilder> getModifierForTimestamp(final long timestamp) {
    return combine(
        modifierAt(structuralModifiers, timestamp),
        modifierAt(customTimestampModifiers, timestamp));
  }

  /**
   * Activations of the structural modifiers, whose domain the schedule builder settles from the
   * milestones surrounding each one.
   *
   * @return the structural activations, ascending
   */
  Stream<Long> structuralActivations() {
    return structuralModifiers.keySet().stream();
  }

  /**
   * Block activations contributed by a customization.
   *
   * @return the contributed block activations, ascending
   */
  Stream<Long> customBlockActivations() {
    return customBlockModifiers.keySet().stream();
  }

  /**
   * Timestamp activations contributed by a customization.
   *
   * @return the contributed timestamp activations, ascending
   */
  Stream<Long> customTimestampActivations() {
    return customTimestampModifiers.keySet().stream();
  }

  private static @Nullable Function<ProtocolSpecBuilder, ProtocolSpecBuilder> modifierAt(
      final NavigableMap<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> modifiers,
      final long activation) {
    final Map.Entry<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> entry =
        modifiers.floorEntry(activation);
    return entry == null ? null : entry.getValue();
  }

  /** Returns a single modifier unchanged, so a lone modifier is handed back as it was supplied. */
  private static Function<ProtocolSpecBuilder, ProtocolSpecBuilder> combine(
      final @Nullable Function<ProtocolSpecBuilder, ProtocolSpecBuilder> first,
      final @Nullable Function<ProtocolSpecBuilder, ProtocolSpecBuilder> second) {
    if (first == null) {
      return second == null ? Function.identity() : second;
    }
    return second == null ? first : first.andThen(second);
  }
}
