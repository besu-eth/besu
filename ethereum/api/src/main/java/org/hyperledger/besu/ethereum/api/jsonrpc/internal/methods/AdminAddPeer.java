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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.DISCOVERY_DISABLED;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeDnsConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminAddPeer extends AdminModifyPeer {

  private static final Logger LOG = LoggerFactory.getLogger(AdminAddPeer.class);

  public AdminAddPeer(
      final P2PNetwork peerNetwork, final Optional<EnodeDnsConfiguration> enodeDnsConfiguration) {
    super(peerNetwork, enodeDnsConfiguration);
  }

  @Override
  public String getName() {
    return RpcMethod.ADMIN_ADD_PEER.getMethodName();
  }

  @Override
  protected JsonRpcResponse performOperation(final Object id, final String enode) {
    if (peerNetwork.isStopped()) {
      LOG.debug("Discovery is disabled, not adding ({}) to peers", enode);
      return new JsonRpcErrorResponse(id, DISCOVERY_DISABLED);
    }
    LOG.debug("Adding ({}) to peers", enode);
    final EnodeURLImpl enodeURL =
        this.enodeDnsConfiguration.isEmpty()
            ? EnodeURLImpl.fromString(enode)
            : EnodeURLImpl.fromString(enode, enodeDnsConfiguration.get());
    final Peer peer = DefaultPeer.fromEnodeURL(enodeURL);
    final boolean addedToNetwork = peerNetwork.addMaintainedConnectionPeer(peer);
    return new JsonRpcSuccessResponse(id, addedToNetwork);
  }
}
