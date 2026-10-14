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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.plugin.StartService;
import org.hyperledger.besu.plugin.services.exception.InProcessRpcDisabledException;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcResponse;

/**
 * Calls the node's JSON-RPC methods in-process, without going through a network interface,
 * including the endpoints plugins registered through {@link RpcEndpointRegistry}.
 *
 * <p>In-process RPC is disabled by default and enabled with {@code --Xin-process-rpc-enabled}. The
 * namespaces of Besu's own methods it serves are chosen with {@code --Xin-process-rpc-apis}, while
 * a plugin endpoint is served when its namespace is enabled with {@code --rpc-http-api}. A plugin
 * that depends on it should check {@link #isEnabled()} in {@code start()} and report the missing
 * flag itself, since whether the feature is on is a property of the node's configuration, not of
 * the plugin.
 */
public interface InProcessRpcService extends StartService {

  /**
   * Whether in-process RPC is enabled on this node.
   *
   * @return true if {@link #call(String, Object[])} can serve methods
   */
  boolean isEnabled();

  /**
   * Calls one of the enabled in-process RPC methods.
   *
   * @param methodName the method to invoke, for example {@code eth_blockNumber}
   * @param params the list of parameters accepted by the method
   * @return the result of the method
   * @throws InProcessRpcDisabledException if in-process RPC is disabled on this node
   * @throws java.util.NoSuchElementException if the method is unknown or not served in-process: a
   *     Besu method whose namespace is not in {@code --Xin-process-rpc-apis}, or a plugin endpoint
   *     whose namespace is not in {@code --rpc-http-api}
   */
  PluginRpcResponse call(String methodName, Object[] params);
}
