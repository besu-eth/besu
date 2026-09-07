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
import org.hyperledger.besu.plugin.Unstable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The immutable, fully resolved contribution of one protocol-schedule customizer.
 *
 * <p>EIP-2124 activations are derived from the modifications rather than supplied separately, so a
 * customizer cannot advertise a boundary it does not change the rules at, nor change them without
 * advertising it.
 */
@Unstable
public record ProtocolScheduleCustomization(
    String name, List<ProtocolSpecModification> modifications) {

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
