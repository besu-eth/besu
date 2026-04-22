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
package org.hyperledger.besu.services;

import org.hyperledger.besu.cli.options.JsonRpcHttpOptions;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.nio.file.Path;
import java.util.Optional;

/** A concrete implementation of BesuConfiguration which is used in Besu plugin framework. */
public class BesuConfigurationImpl implements BesuConfiguration {
  private Path storagePath;
  private Path dataPath;
  private DataStorageConfiguration dataStorageConfiguration;

  // defaults
  private MiningConfiguration miningConfiguration;
  private String rpcHttpHost = JsonRpcConfiguration.DEFAULT_JSON_RPC_HOST;
  private Integer rpcHttpPort = JsonRpcConfiguration.DEFAULT_JSON_RPC_PORT;
  private long rpcHttpTimeoutSec = JsonRpcConfiguration.DEFAULT_HTTP_TIMEOUT_SEC;

  /** Default Constructor. */
  public BesuConfigurationImpl() {}

  /**
   * Post creation initialization
   *
   * @param dataPath The Path representing data folder
   * @param storagePath The path representing storage folder
   * @param dataStorageConfiguration The data storage configuration
   * @return BesuConfigurationImpl instance
   */
  public BesuConfigurationImpl init(
      final Path dataPath,
      final Path storagePath,
      final DataStorageConfiguration dataStorageConfiguration) {
    this.dataPath = dataPath;
    this.storagePath = storagePath;
    this.dataStorageConfiguration = dataStorageConfiguration;
    return this;
  }

  /**
   * Set the mining parameters
   *
   * @param miningConfiguration configured mining parameters
   * @return BesuConfigurationImpl instance
   */
  public BesuConfigurationImpl withMiningParameters(final MiningConfiguration miningConfiguration) {
    this.miningConfiguration = miningConfiguration;
    return this;
  }

  /**
   * Set the RPC http options
   *
   * @param rpcHttpOptions configured rpc http options
   * @return BesuConfigurationImpl instance
   */
  public BesuConfigurationImpl withJsonRpcHttpOptions(final JsonRpcHttpOptions rpcHttpOptions) {
    this.rpcHttpHost = rpcHttpOptions.getRpcHttpHost();
    this.rpcHttpPort = rpcHttpOptions.getRpcHttpPort();
    this.rpcHttpTimeoutSec = rpcHttpOptions.jsonRpcConfiguration().getHttpTimeoutSec();
    return this;
  }

  @Deprecated
  @Override
  public Optional<String> getRpcHttpHost() {
    return Optional.of(rpcHttpHost);
  }

  @Deprecated
  @Override
  public Optional<Integer> getRpcHttpPort() {
    return Optional.of(rpcHttpPort);
  }

  @Override
  public String getConfiguredRpcHttpHost() {
    return rpcHttpHost;
  }

  @Override
  public Integer getConfiguredRpcHttpPort() {
    return rpcHttpPort;
  }

  @Override
  public long getConfiguredRpcHttpTimeoutSec() {
    return rpcHttpTimeoutSec;
  }

  @Override
  public Path getStoragePath() {
    return storagePath;
  }

  @Override
  public Path getDataPath() {
    return dataPath;
  }

  @Override
  public DataStorageFormat getDatabaseFormat() {
    return dataStorageConfiguration.getDataStorageFormat();
  }

  @Override
  public Wei getMinGasPrice() {
    return miningConfiguration.getMinTransactionGasPrice();
  }

  @Override
  public org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration
      getDataStorageConfiguration() {
    return new DataStoreConfigurationImpl(dataStorageConfiguration);
  }

  /**
   * A concrete implementation of DataStorageConfiguration which is used in Besu plugin framework.
   */
  public static class DataStoreConfigurationImpl
      implements org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration {

    private final DataStorageConfiguration dataStorageConfiguration;

    /**
     * Instantiate the concrete implementation of the plugin DataStorageConfiguration.
     *
     * @param dataStorageConfiguration The Ethereum core module data storage configuration
     */
    public DataStoreConfigurationImpl(final DataStorageConfiguration dataStorageConfiguration) {
      this.dataStorageConfiguration = dataStorageConfiguration;
    }

    @Override
    public DataStorageFormat getDataStorageFormat() {
      return dataStorageConfiguration.getDataStorageFormat();
    }

    @Override
    public boolean getReceiptCompactionEnabled() {
      return dataStorageConfiguration.getReceiptCompactionEnabled();
    }

    @Override
    public boolean isHistoryExpiryPruneEnabled() {
      return dataStorageConfiguration.getHistoryExpiryPruneEnabled();
    }

    @Override
    public boolean getHashPreImageStorageEnabled() {
      return dataStorageConfiguration.getHashPreImageStorageEnabled();
    }
  }
}
