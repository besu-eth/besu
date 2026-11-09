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
import org.hyperledger.besu.plugin.RegistrationClosedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * Besu-owned protocol-schedule customizer registry.
 *
 * <p>Like the other registration-tier services, it accepts customizers only while the plugin
 * context's registration window is open. It also stops accepting them once resolution has begun, so
 * that what was resolved is everything that was registered.
 */
final class ProtocolScheduleServiceImpl implements ProtocolScheduleService {

  private final List<ProtocolScheduleCustomizer> customizers = new ArrayList<>();
  private final BooleanSupplier registrationOpen;
  private ProtocolScheduleCustomization resolvedCustomization;
  private boolean resolving;

  /**
   * Creates the registry.
   *
   * @param registrationOpen whether plugin registration is currently open; customizers registered
   *     while it is closed are rejected
   */
  ProtocolScheduleServiceImpl(final BooleanSupplier registrationOpen) {
    this.registrationOpen = Objects.requireNonNull(registrationOpen);
  }

  @Override
  public synchronized void registerProtocolScheduleCustomizer(
      final ProtocolScheduleCustomizer customizer) {
    if (resolving || !registrationOpen.getAsBoolean()) {
      throw new RegistrationClosedException(
          "ProtocolScheduleService", "registerProtocolScheduleCustomizer");
    }
    customizers.add(Objects.requireNonNull(customizer, "customizer must not be null"));
  }

  /**
   * Resolves the registered customizers against the active chain.
   *
   * <p>A node runs one genesis per registration cycle, so the result is memoized: the first call
   * evaluates the customizers and every later call returns the same customization, whatever config
   * it is passed. {@link #reset()} starts a new cycle, as an Ephemery restart does.
   *
   * <p>A customizer that throws fails resolution, naming the customizer. This is fatal even under
   * {@code --plugin-continue-on-error}: a node that silently ran without the rules a plugin
   * contributes for its chain would be on a different chain.
   *
   * @param config the uncustomized genesis configuration
   * @return the single matching customization, or the empty one
   * @throws IllegalStateException if registration is still open, if a customizer throws, or if more
   *     than one customizer supports the chain
   */
  synchronized ProtocolScheduleCustomization resolve(final GenesisConfigOptions config) {
    Objects.requireNonNull(config, "config must not be null");
    if (resolvedCustomization != null) {
      return resolvedCustomization;
    }
    if (registrationOpen.getAsBoolean()) {
      throw new IllegalStateException(
          "Protocol-schedule customizers cannot be resolved while plugin registration is open");
    }
    resolving = true;
    final List<ProtocolScheduleCustomization> matchingCustomizations =
        customizers.stream()
            .map(customizer -> customize(customizer, config))
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

  private static Optional<ProtocolScheduleCustomization> customize(
      final ProtocolScheduleCustomizer customizer, final GenesisConfigOptions config) {
    try {
      return Objects.requireNonNull(
          customizer.customize(config), "customize() returned null rather than Optional.empty()");
    } catch (final RuntimeException e) {
      throw new IllegalStateException(
          "Protocol-schedule customizer "
              + customizer.getClass().getName()
              + " failed while evaluating the genesis config: "
              + e.getMessage(),
          e);
    }
  }

  /**
   * Marks how many customizers are registered, so that those a plugin registers afterwards can be
   * discarded if its registration fails.
   *
   * @return the mark
   */
  synchronized int registrationMark() {
    return customizers.size();
  }

  /**
   * Discards the customizers registered since a mark: a plugin whose registration failed is never
   * started, so it must not change the chain's rules either.
   *
   * @param mark a value returned by {@link #registrationMark()}
   */
  synchronized void discardRegistrationsSince(final int mark) {
    customizers.subList(mark, customizers.size()).clear();
  }

  synchronized void reset() {
    customizers.clear();
    resolvedCustomization = null;
    resolving = false;
  }
}
