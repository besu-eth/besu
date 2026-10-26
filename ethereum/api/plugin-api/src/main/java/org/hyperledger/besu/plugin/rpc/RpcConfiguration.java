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

import java.util.List;

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

  /**
   * Get the configured host allowlist (--host-allowlist) that HTTP services check the Host header
   * against. An entry of "*" allows any host. An empty list allows no host.
   *
   * <p>The value reflects the command line from the {@code beforeExternalServices} callback onward.
   * During the {@code register} callback it is not yet known.
   *
   * @return the configured host allowlist, as an immutable list
   */
  default List<String> getConfiguredHostAllowlist() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the address that services bind to when no service-specific host is configured. This is the
   * --p2p-host value, which Besu also uses as the default host of its own listeners.
   *
   * <p>The value reflects the command line from the {@code beforeExternalServices} callback onward.
   * During the {@code register} callback it is not yet known.
   *
   * @return the default host address
   */
  default String getDefaultHostAddress() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the configured maximum number of addresses in a log filter
   * (--rpc-max-log-filter-addresses).
   *
   * <p>The value reflects the command line from the {@code beforeExternalServices} callback onward.
   * During the {@code register} callback it is not yet known.
   *
   * @return the maximum number of addresses in a log filter
   */
  default int getConfiguredMaxLogFilterAddresses() {
    throw new UnsupportedOperationException();
  }
}
