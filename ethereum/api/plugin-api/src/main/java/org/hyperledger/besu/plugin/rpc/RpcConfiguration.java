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
package org.hyperledger.besu.plugin.rpc;

import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.services.BesuService;

/**
 * RPC configuration of the Besu node.
 *
 * <p>Provides the RPC-side part of the node configuration to plugins. It exists so that a plugin
 * can read how the node's JSON-RPC HTTP listener is configured without depending on the full
 * configuration surface. The remaining configuration is available through {@code
 * org.hyperledger.besu.plugin.services.BesuConfiguration} and, for the data directory, {@link
 * org.hyperledger.besu.plugin.CoreConfiguration}.
 */
@Unstable
public interface RpcConfiguration extends BesuService {

  /**
   * Get the configured RPC http host.
   *
   * @return the configured RPC http host.
   */
  String getConfiguredRpcHttpHost();

  /**
   * Get the configured RPC http port.
   *
   * @return the configured RPC http port.
   */
  Integer getConfiguredRpcHttpPort();

  /**
   * Get the configured RPC http timeout in seconds.
   *
   * @return the configured RPC http timeout in seconds.
   */
  long getConfiguredRpcHttpTimeoutSec();
}
