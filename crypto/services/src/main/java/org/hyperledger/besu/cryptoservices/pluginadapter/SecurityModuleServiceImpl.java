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

import org.hyperledger.besu.plugin.services.SecurityModuleService;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModule;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.inject.Inject;

/** The Security module service implementation. */
public class SecurityModuleServiceImpl implements SecurityModuleService {

  /** Default Constructor. */
  @Inject
  public SecurityModuleServiceImpl() {}

  private final Map<String, Supplier<SecurityModule>> securityModuleSuppliers =
      new ConcurrentHashMap<>();

  @Override
  public void register(final String name, final Supplier<SecurityModule> securityModuleSupplier) {
    securityModuleSuppliers.put(name, securityModuleSupplier);
  }

  @Override
  public Optional<Supplier<SecurityModule>> getByName(final String name) {
    return Optional.ofNullable(securityModuleSuppliers.get(name));
  }
}
