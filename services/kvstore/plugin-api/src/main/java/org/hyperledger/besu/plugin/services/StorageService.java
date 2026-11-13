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

import org.hyperledger.besu.plugin.RegistrationService;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.List;
import java.util.Optional;

/** This service allows plugins to register as an available storage engine. */
@Unstable
public interface StorageService extends RegistrationService {

  /**
   * Registers a factory as available for creating key-value storage instances. The factory's name
   * must be free: a name another plugin or Besu itself already registered fails the registration,
   * and with it the plugin's startup; a plugin may register its own name again, replacing its
   * factory.
   *
   * @param factory creates instances providing key-value storage.
   * @throws org.hyperledger.besu.plugin.RegistrationClosedException if called after registration
   *     has closed
   * @throws IllegalStateException if another registrant already registered a factory of that name
   */
  void registerKeyValueStorage(KeyValueStorageFactory factory);

  /**
   * Retrieves the identifiers for the isolation segments that could be requested during operation.
   *
   * @return full set of possible segments required from the storage service.
   */
  List<SegmentIdentifier> getAllSegmentIdentifiers();

  /**
   * Retrieves a registered factory corresponding to the supplied factory name
   *
   * @param name The name of the factory to retrieve
   * @return an optional containing the instance of the registered factory, or empty if the factory
   *     hasn't been registered.
   */
  Optional<KeyValueStorageFactory> getByName(String name);
}
