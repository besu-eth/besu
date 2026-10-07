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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModule;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * This service allows plugins to register a Security Module, which is abstraction of cryptographic
 * operations that defer to specific provider (e.g. BouncyCastle).
 */
@Unstable
public interface SecurityModuleService extends BesuService {

  /**
   * Registers a provider of security modules.
   *
   * @param name The name to identify the Security Provider Supplier
   * @param securityModuleSupplier Register reference of Security Module Supplier.
   */
  void register(String name, Supplier<SecurityModule> securityModuleSupplier);

  /**
   * Retrieves a registered Security Module Provider corresponding to the specified name
   *
   * @param name The name associated with Security Module Provider
   * @return Optional reference of Security Module Supplier, or empty if it hasn't been registered.
   */
  Optional<Supplier<SecurityModule>> getByName(String name);
}
