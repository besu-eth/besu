/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.plugin.storage;

import org.hyperledger.besu.plugin.RegistrationService;
import org.hyperledger.besu.plugin.StartService;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.nio.file.Path;
import java.util.List;

/**
 * Storage configuration of the Besu node.
 *
 * <p>Provides the storage-side part of the node configuration to storage plugins. It exists so that
 * a storage plugin can read where the database lives and how it is formatted without depending on
 * the full configuration surface. The remaining configuration is available through {@code
 * org.hyperledger.besu.plugin.services.BesuConfiguration} and, for the data directory, {@link
 * org.hyperledger.besu.plugin.CoreConfiguration}.
 */
@Unstable
public interface StorageConfiguration extends RegistrationService, StartService {

  /**
   * Location of the working directory of the storage in the file system running the client.
   *
   * @return location of the storage in the file system of the client.
   */
  Path getStoragePath();

  /**
   * Database storage configuration.
   *
   * @return Database storage configuration.
   */
  DataStorageConfiguration getDataStorageConfiguration();

  /**
   * Segments the node does not use in its current configuration, for example the chain pruner state
   * while chain pruning is off. A storage factory need not create them, but keeps serving them
   * where they already exist.
   *
   * @return the segments that may be absent from the database; empty by default
   */
  default List<SegmentIdentifier> getIgnorableSegments() {
    return List.of();
  }
}
