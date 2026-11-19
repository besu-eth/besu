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
import org.hyperledger.besu.util.KeyedRegistrations;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.inject.Inject;

/** The Security module service implementation. */
public class SecurityModuleServiceImpl implements SecurityModuleService {

  private final KeyedRegistrations<Supplier<SecurityModule>> securityModuleSuppliers;
  private final BooleanSupplier registrationOpen;

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
    this.securityModuleSuppliers = new KeyedRegistrations<>("security module", currentRegistrant);
    this.registrationOpen = registrationOpen;
  }

  @Override
  public void register(final String name, final Supplier<SecurityModule> securityModuleSupplier) {
    if (!registrationOpen.getAsBoolean()) {
      throw new RegistrationClosedException("SecurityModuleService", "register");
    }
    securityModuleSuppliers.register(name, securityModuleSupplier);
  }

  @Override
  public Optional<Supplier<SecurityModule>> getByName(final String name) {
    return securityModuleSuppliers.get(name);
  }
}
