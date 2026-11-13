/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.cryptoservices.pluginadapter;

import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.services.SecurityModuleService;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModule;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.inject.Inject;

/** The Security module service implementation. */
public class SecurityModuleServiceImpl implements SecurityModuleService {

  /** The registrant of an entry Besu registered itself, outside any plugin's register(). */
  private static final String BESU = "Besu";

  /** A registered module supplier with the id of the plugin that registered it. */
  private record Registered(String registrant, Supplier<SecurityModule> supplier) {}

  private final Map<String, Registered> securityModuleSuppliers = new ConcurrentHashMap<>();
  private final BooleanSupplier registrationOpen;
  private final Supplier<Optional<String>> currentRegistrant;

  /** Creates a service that accepts registrations at any time. */
  @Inject
  public SecurityModuleServiceImpl() {
    this(() -> true);
  }

  /**
   * Creates a service whose registrations all count as Besu's own.
   *
   * @param registrationOpen whether plugin registration is currently open; registrations made while
   *     it is closed are rejected
   */
  public SecurityModuleServiceImpl(final BooleanSupplier registrationOpen) {
    this(registrationOpen, Optional::empty);
  }

  /**
   * Creates the service.
   *
   * @param registrationOpen whether plugin registration is currently open; registrations made while
   *     it is closed are rejected
   * @param currentRegistrant the id of the plugin whose register() is running, empty when Besu
   *     itself is registering; a module name already registered by another registrant is rejected
   */
  public SecurityModuleServiceImpl(
      final BooleanSupplier registrationOpen, final Supplier<Optional<String>> currentRegistrant) {
    this.registrationOpen = registrationOpen;
    this.currentRegistrant = currentRegistrant;
  }

  @Override
  public void register(final String name, final Supplier<SecurityModule> securityModuleSupplier) {
    if (!registrationOpen.getAsBoolean()) {
      throw new RegistrationClosedException("SecurityModuleService", "register");
    }
    final String registrant = currentRegistrant.get().orElse(BESU);
    final Registered previous = securityModuleSuppliers.get(name);
    if (previous != null && !previous.registrant().equals(registrant)) {
      throw new IllegalStateException(
          "cannot register security module "
              + name
              + ": "
              + owner(previous.registrant())
              + " already registered it");
    }
    securityModuleSuppliers.put(name, new Registered(registrant, securityModuleSupplier));
  }

  private static String owner(final String registrant) {
    return BESU.equals(registrant) ? BESU : "plugin " + registrant;
  }

  @Override
  public Optional<Supplier<SecurityModule>> getByName(final String name) {
    return Optional.ofNullable(securityModuleSuppliers.get(name)).map(Registered::supplier);
  }
}
