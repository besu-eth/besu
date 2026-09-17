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
package org.hyperledger.besu.cli;

import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.JwtAlgorithm;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.nat.NatMethod;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import picocli.CommandLine;

/** The interface Default command values. */
public interface DefaultCommandValues {
  /** The constant CONFIG_FILE_OPTION_NAME. */
  String CONFIG_FILE_OPTION_NAME = "--config-file";

  /** The constant MANDATORY_PATH_FORMAT_HELP. */
  String MANDATORY_PATH_FORMAT_HELP = "<PATH>";

  /** The constant MANDATORY_FILE_FORMAT_HELP. */
  String MANDATORY_FILE_FORMAT_HELP = "<FILE>";

  /** The constant MANDATORY_DIRECTORY_FORMAT_HELP. */
  String MANDATORY_DIRECTORY_FORMAT_HELP = "<DIRECTORY>";

  /** The constant BESU_HOME_PROPERTY_NAME. */
  String BESU_HOME_PROPERTY_NAME = "besu.home";

  /** The constant DEFAULT_DATA_DIR_PATH. */
  String DEFAULT_DATA_DIR_PATH = "./build/data";

  /** The constant MANDATORY_INTEGER_FORMAT_HELP. */
  String MANDATORY_INTEGER_FORMAT_HELP = "<INTEGER>";

  /** The constant MANDATORY_DOUBLE_FORMAT_HELP. */
  String MANDATORY_DOUBLE_FORMAT_HELP = "<DOUBLE>";

  /** The constant MANDATORY_LONG_FORMAT_HELP. */
  String MANDATORY_LONG_FORMAT_HELP = "<LONG>";

  /** The constant MANDATORY_MODE_FORMAT_HELP. */
  String MANDATORY_MODE_FORMAT_HELP = "<MODE>";

  /** The constant MANDATORY_NETWORK_FORMAT_HELP. */
  String MANDATORY_NETWORK_FORMAT_HELP = "<NETWORK>";

  /** The constant PROFILE_OPTION_NAME. */
  String PROFILE_OPTION_NAME = "--profile";

  /** The constant PROFILE_FORMAT_HELP. */
  String PROFILE_FORMAT_HELP = "<PROFILE>";

  /** The constant MANDATORY_NODE_ID_FORMAT_HELP. */
  String MANDATORY_NODE_ID_FORMAT_HELP = "<NODEID>";

  /** The constant PERMISSIONING_CONFIG_LOCATION. */
  String PERMISSIONING_CONFIG_LOCATION = "permissions_config.toml";

  /** The constant MANDATORY_HOST_FORMAT_HELP. */
  String MANDATORY_HOST_FORMAT_HELP = "<HOST>";

  /** The constant MANDATORY_PORT_FORMAT_HELP. */
  String MANDATORY_PORT_FORMAT_HELP = "<PORT>";

  /** The constant DEFAULT_NAT_METHOD. */
  NatMethod DEFAULT_NAT_METHOD = NatMethod.AUTO;

  /** The constant DEFAULT_JWT_ALGORITHM. */
  JwtAlgorithm DEFAULT_JWT_ALGORITHM = JwtAlgorithm.RS256;

  /** The constant SYNC_MIN_PEER_COUNT. */
  int SYNC_MIN_PEER_COUNT = 5;

  /** The constant DEFAULT_MAX_PEERS. */
  int DEFAULT_MAX_PEERS = 25;

  /** The constant DEFAULT_HTTP_MAX_CONNECTIONS. */
  int DEFAULT_HTTP_MAX_CONNECTIONS = 80;

  /** The constant DEFAULT_HTTP_MAX_BATCH_SIZE. */
  int DEFAULT_HTTP_MAX_BATCH_SIZE = 1024;

  /** The constant DEFAULT_MAX_REQUEST_CONTENT_LENGTH. */
  long DEFAULT_MAX_REQUEST_CONTENT_LENGTH = 128 * 1024 * 1024; // 128MB

  /** The constant DEFAULT_WS_MAX_CONNECTIONS. */
  int DEFAULT_WS_MAX_CONNECTIONS = 80;

  /** The constant DEFAULT_WS_MAX_ACTIVE_SUBSCRIPTIONS. */
  int DEFAULT_WS_MAX_ACTIVE_SUBSCRIPTIONS = WebSocketConfiguration.DEFAULT_MAX_ACTIVE_SUBSCRIPTIONS;

  /** The constant DEFAULT_WS_MAX_FRAME_SIZE. */
  int DEFAULT_WS_MAX_FRAME_SIZE = 1024 * 1024;

  /** The constant DEFAULT_FRACTION_REMOTE_WIRE_CONNECTIONS_ALLOWED. */
  float DEFAULT_FRACTION_REMOTE_WIRE_CONNECTIONS_ALLOWED =
      RlpxConfiguration.DEFAULT_FRACTION_REMOTE_CONNECTIONS_ALLOWED;

  /** The constant DEFAULT_KEY_VALUE_STORAGE_NAME. */
  String DEFAULT_KEY_VALUE_STORAGE_NAME = "rocksdb";

  /** The constant DEFAULT_SECURITY_MODULE. */
  String DEFAULT_SECURITY_MODULE = "localfile";

  /** The constant DEFAULT_KEYSTORE_TYPE. */
  String DEFAULT_KEYSTORE_TYPE = "JKS";

  /** The Default tls protocols. */
  List<String> DEFAULT_TLS_PROTOCOLS = List.of("TLSv1.3", "TLSv1.2");

  /**
   * Gets default besu data path.
   *
   * @param command the command
   * @return the default besu data path
   */
  static Path getDefaultBesuDataPath(final Object command) {
    // this property is retrieved from Gradle tasks or Besu running shell script.
    final String besuHomeProperty = System.getProperty(BESU_HOME_PROPERTY_NAME);
    final Path besuHome;

    // If prop is found, then use it
    if (besuHomeProperty != null) {
      try {
        besuHome = Paths.get(besuHomeProperty);
      } catch (final InvalidPathException e) {
        throw new CommandLine.ParameterException(
            new CommandLine(command),
            String.format(
                "Unable to define default data directory from %s property.",
                BESU_HOME_PROPERTY_NAME),
            e);
      }
    } else {
      // otherwise use a default path.
      // That may only be used when NOT run from distribution script and Gradle as they all define
      // the property.
      try {
        final String path = new File(DEFAULT_DATA_DIR_PATH).getCanonicalPath();
        besuHome = Paths.get(path);
      } catch (final IOException e) {
        throw new CommandLine.ParameterException(
            new CommandLine(command), "Unable to create default data directory.");
      }
    }

    // Try to create it, then verify if the provided path is not already existing and is not a
    // directory. Otherwise, if it doesn't exist or exists but is already a directory,
    // Runner will use it to store data.
    try {
      Files.createDirectories(besuHome);
    } catch (final FileAlreadyExistsException e) {
      // Only thrown if it exists but is not a directory
      throw new CommandLine.ParameterException(
          new CommandLine(command),
          String.format("%s: already exists and is not a directory.", besuHome.toAbsolutePath()),
          e);
    } catch (final Exception e) {
      throw new CommandLine.ParameterException(
          new CommandLine(command),
          String.format("Error creating directory %s.", besuHome.toAbsolutePath()),
          e);
    }
    return besuHome;
  }
}
