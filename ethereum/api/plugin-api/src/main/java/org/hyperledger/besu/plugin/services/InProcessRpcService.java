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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.plugin.StartService;
import org.hyperledger.besu.plugin.services.exception.InProcessRpcDisabledException;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcResponse;

/**
 * Calls Besu's own JSON-RPC methods in-process, without going through a network interface.
 *
 * <p>In-process RPC is disabled by default and enabled with {@code --Xin-process-rpc-enabled}; the
 * namespaces it serves are chosen with {@code --Xin-process-rpc-apis}. A plugin that depends on it
 * should check {@link #isEnabled()} in {@code start()} and report the missing flag itself, since
 * whether the feature is on is a property of the node's configuration, not of the plugin.
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
   * @throws java.util.NoSuchElementException if the method is unknown, or its namespace is not
   *     among the enabled in-process APIs
   */
  PluginRpcResponse call(String methodName, Object[] params);
}
