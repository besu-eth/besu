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

import org.hyperledger.besu.cli.custom.JsonRPCAllowlistHostsProperty;
import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.InclusionListConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.InclusionListValidationMode;

import java.nio.file.Path;

/**
 * Command line options for configuring Engine RPC on the node.
 *
 * @param overrideEngineRpcEnabled enable the engine api, even in the absence of merge-specific
 *     configurations.
 * @param engineRpcPort Port to provide consensus client APIS on
 * @param engineJwtKeyFile Path to file containing shared secret key for JWT signature verification
 * @param isEngineAuthDisabled Disable authentication for Engine APIs
 * @param engineHostsAllowlist List of hosts to allowlist for Engine APIs
 * @param inclusionListValidationMode Inclusion list validation mode (strict or lenient)
 */
public record EngineRPCConfiguration(
    Boolean overrideEngineRpcEnabled,
    Integer engineRpcPort,
    Path engineJwtKeyFile,
    Boolean isEngineAuthDisabled,
    JsonRPCAllowlistHostsProperty engineHostsAllowlist,
    InclusionListValidationMode inclusionListValidationMode) {

  /**
   * Creates a consolidated InclusionListConfiguration from the Engine RPC settings, using the
   * default transaction selector.
   *
   * @return the inclusion list configuration
   */
  public InclusionListConfiguration toInclusionListConfiguration() {
    return new InclusionListConfiguration(
        inclusionListValidationMode, InclusionListConfiguration.DEFAULT.selector());
  }
}
