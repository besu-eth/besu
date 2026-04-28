/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.cli.options;

import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration.DEFAULT_ENGINE_JSON_RPC_PORT;

import org.hyperledger.besu.cli.DefaultCommandValues;
import org.hyperledger.besu.cli.custom.JsonRPCAllowlistHostsProperty;
import org.hyperledger.besu.cli.util.CommandLineUtils;

import java.nio.file.Path;
import java.util.List;

import picocli.CommandLine;

/** Command line options for configuring Engine RPC on the node. */
public class EngineRPCOptions implements CLIOptions<EngineRPCConfiguration> {

  /** Default constructor */
  public EngineRPCOptions() {}

  @CommandLine.Option(
      names = {"--engine-rpc-enabled"},
      description = "enable the engine api, even in the absence of merge-specific configurations.")
  private final Boolean overrideEngineRpcEnabled = false;

  @CommandLine.Option(
      names = {"--engine-rpc-port", "--engine-rpc-http-port"},
      paramLabel = DefaultCommandValues.MANDATORY_PORT_FORMAT_HELP,
      description = "Port to provide consensus client APIS on (default: ${DEFAULT-VALUE})")
  private final Integer engineRpcPort = DEFAULT_ENGINE_JSON_RPC_PORT;

  @CommandLine.Option(
      names = {"--engine-jwt-secret"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "Path to file containing shared secret key for JWT signature verification")
  private final Path engineJwtKeyFile = null;

  @CommandLine.Option(
      names = {"--engine-jwt-disabled"},
      description = "Disable authentication for Engine APIs (default: ${DEFAULT-VALUE})")
  private final Boolean isEngineAuthDisabled = false;

  @CommandLine.Option(
      names = {"--engine-host-allowlist"},
      paramLabel = "<hostname>[,<hostname>...]... or * or all",
      description =
          "Comma separated list of hostnames to allow for ENGINE API access (applies to both HTTP and websockets), or * to accept any host (default: ${DEFAULT-VALUE})",
      defaultValue = "localhost,127.0.0.1")
  private final JsonRPCAllowlistHostsProperty engineHostsAllowlist =
      new JsonRPCAllowlistHostsProperty();

  @Override
  public EngineRPCConfiguration toDomainObject() {
    return new EngineRPCConfiguration(
        overrideEngineRpcEnabled,
        engineRpcPort,
        engineJwtKeyFile,
        isEngineAuthDisabled,
        engineHostsAllowlist);
  }

  @Override
  public List<String> getCLIOptions() {
    return CommandLineUtils.getCLIOptions(this, new EngineRPCOptions());
  }
}
