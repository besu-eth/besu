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

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.nio.file.Path;

/** Generally useful configuration provided by Besu. */
public interface BesuConfiguration extends BesuService {

  /**
   * Get the configured RPC http host.
   *
   * @return the configured RPC http host.
   */
  String getConfiguredRpcHttpHost();

  /**
   * Get the configured RPC http timeout in second.
   *
   * @return the configured RPC http timeout in second.
   */
  long getConfiguredRpcHttpTimeoutSec();

  /**
   * Get the configured RPC http port.
   *
   * @return the configured RPC http port.
   */
  Integer getConfiguredRpcHttpPort();

  /**
   * Location of the working directory of the storage in the file system running the client.
   *
   * @return location of the storage in the file system of the client.
   */
  Path getStoragePath();

  /**
   * Location of the data directory in the file system running the client.
   *
   * @return location of the data directory in the file system of the client.
   */
  Path getDataPath();

  /**
   * Database format. This sets the list of segmentIdentifiers that should be initialized.
   *
   * @return Database format.
   */
  @Unstable
  @Deprecated
  DataStorageFormat getDatabaseFormat();

  /**
   * The runtime value of the min gas price
   *
   * @return min gas price in wei
   */
  @Unstable
  Wei getMinGasPrice();

  /**
   * Database storage configuration.
   *
   * @return Database storage configuration.
   */
  @Unstable
  DataStorageConfiguration getDataStorageConfiguration();
}
