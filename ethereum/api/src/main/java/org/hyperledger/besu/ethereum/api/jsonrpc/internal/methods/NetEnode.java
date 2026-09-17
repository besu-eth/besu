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

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;

import java.util.Optional;

public class NetEnode implements JsonRpcMethod {

  private final P2PNetwork p2pNetwork;

  public NetEnode(final P2PNetwork p2pNetwork) {
    this.p2pNetwork = p2pNetwork;
  }

  @Override
  public String getName() {
    return RpcMethod.NET_ENODE.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    if (!p2pNetwork.isP2pEnabled()) {
      return p2pDisabledResponse(requestContext);
    }

    final Optional<EnodeURLImpl> enodeURL = p2pNetwork.getLocalEnode();
    if (!enodeURL.isPresent()) {
      return enodeUrlNotAvailable(requestContext);
    }

    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(), enodeURL.get().toString());
  }

  private JsonRpcErrorResponse p2pDisabledResponse(final JsonRpcRequestContext requestContext) {
    return new JsonRpcErrorResponse(requestContext.getRequest().getId(), RpcErrorType.P2P_DISABLED);
  }

  private JsonRpcErrorResponse enodeUrlNotAvailable(final JsonRpcRequestContext requestContext) {
    return new JsonRpcErrorResponse(
        requestContext.getRequest().getId(), RpcErrorType.P2P_NETWORK_NOT_RUNNING);
  }
}
