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
import java.util.Optional;

/** Generally useful configuration provided by Besu. */
public interface BesuConfiguration extends BesuService {

  /**
   * Get the configured RPC http host.
   *
   * @return the configured RPC http host.
   */
  @Deprecated(since = "25.1.0")
  Optional<String> getRpcHttpHost();

  /**
   * Get the configured RPC http port.
   *
   * @return the configured RPC http port.
   */
  @Deprecated(since = "25.1.0")
  Optional<Integer> getRpcHttpPort();

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

  /**
   * Returns the version of the running Besu node.
   *
   * <p>This is a human-readable version string. For release builds, it is typically a semantic
   * version such as {@code "25.3.0"}. Development builds may include a qualifier, e.g. {@code
   * "25.3.1-dev-ac23d311"}.
   *
   * <p>Available during all plugin lifecycle phases ({@code register} through {@code stop}).
   *
   * @return the Besu node version string
   */
  String getBesuVersion();

  /**
   * Returns the git commit hash of the running Besu build.
   *
   * <p>Available during all plugin lifecycle phases ({@code register} through {@code stop}).
   *
   * @return the git commit hash
   */
  String getBesuCommitHash();
}
