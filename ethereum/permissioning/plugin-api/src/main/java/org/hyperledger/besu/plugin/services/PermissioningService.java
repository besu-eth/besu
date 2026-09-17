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

import org.hyperledger.besu.plugin.services.permissioning.NodeConnectionPermissioningProvider;
import org.hyperledger.besu.plugin.services.permissioning.NodeMessagePermissioningProvider;
import org.hyperledger.besu.plugin.services.permissioning.TransactionPermissioningProvider;

/**
 * This service allows plugins to decide who you should connect to and what you should send them.
 *
 * <p>Currently, there are two hooks available: connection permissioning and message permissioning.
 *
 * <ul>
 *   <li><b>Connection permissioning</b> - checks if inbound and outbound connections to peers are
 *       permitted. {@link NodeConnectionPermissioningProvider}
 *   <li><b>Message permissioning</b> - checks if a devp2p message can be sent to a peer. {@link
 *       NodeMessagePermissioningProvider}
 * </ul>
 */
public interface PermissioningService extends BesuService {

  /**
   * Registers a callback to allow the interception of a peer connection request
   *
   * @param provider The provider to register
   */
  void registerNodePermissioningProvider(NodeConnectionPermissioningProvider provider);

  /**
   * Registers a callback for transaction permission.
   *
   * @param provider The provider to register
   */
  void registerTransactionPermissioningProvider(final TransactionPermissioningProvider provider);

  /**
   * Registers a callback to allow the interception of a devp2p message sending request
   *
   * @param provider The provider to register
   */
  void registerNodeMessagePermissioningProvider(NodeMessagePermissioningProvider provider);
}
