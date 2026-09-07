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
package org.hyperledger.besu.services;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleCustomization;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleCustomizer;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleService;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Besu-owned protocol-schedule customizer registry. */
final class ProtocolScheduleServiceImpl implements ProtocolScheduleService {

  private final List<ProtocolScheduleCustomizer> customizers = new ArrayList<>();
  private ProtocolScheduleCustomization resolvedCustomization;
  private boolean frozen;

  @Override
  public synchronized void registerProtocolScheduleCustomizer(
      final ProtocolScheduleCustomizer customizer) {
    if (frozen) {
      throw new IllegalStateException(
          "Protocol-schedule customizers can only be registered during plugin registration");
    }
    customizers.add(Objects.requireNonNull(customizer, "customizer must not be null"));
  }

  /**
   * Resolves the registered customizers against the active chain, memoizing the result: the first
   * config wins and later calls return the same customization without re-evaluating.
   *
   * @param config the uncustomized genesis configuration
   * @return the single matching customization, or the empty one
   */
  synchronized ProtocolScheduleCustomization resolve(final GenesisConfigOptions config) {
    Objects.requireNonNull(config, "config must not be null");
    if (resolvedCustomization != null) {
      return resolvedCustomization;
    }
    frozen = true;
    final List<ProtocolScheduleCustomization> matchingCustomizations =
        customizers.stream()
            .map(customizer -> customizer.customize(config))
            .flatMap(Optional::stream)
            .toList();
    if (matchingCustomizations.size() > 1) {
      final String names =
          matchingCustomizations.stream()
              .map(ProtocolScheduleCustomization::name)
              .sorted()
              .collect(Collectors.joining(", "));
      throw new IllegalStateException(
          "Multiple protocol-schedule customizers support the active chain: " + names);
    }
    resolvedCustomization =
        matchingCustomizations.isEmpty()
            ? ProtocolScheduleCustomization.none()
            : matchingCustomizations.getFirst();
    return resolvedCustomization;
  }

  synchronized void freeze() {
    frozen = true;
  }

  synchronized void reset() {
    customizers.clear();
    resolvedCustomization = null;
    frozen = false;
  }
}
