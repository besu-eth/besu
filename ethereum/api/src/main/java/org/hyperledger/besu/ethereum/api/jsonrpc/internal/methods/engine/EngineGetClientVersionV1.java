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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetClientVersionResultV1;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Collections;
import java.util.List;

import io.vertx.core.Vertx;

public class EngineGetClientVersionV1 extends ExecutionEngineJsonRpcMethod {
  private static final String ENGINE_CLIENT_CODE = "BU";
  private static final String ENGINE_CLIENT_NAME = "Besu";

  private final String clientVersion;
  private final String commit;

  public EngineGetClientVersionV1(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final Vertx vertx,
      final EngineCallListener engineCallListener,
      final String clientVersion,
      final String commit) {
    super(protocolSchedule, protocolContext, vertx, engineCallListener);
    this.clientVersion = clientVersion;
    this.commit = commit;
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_CLIENT_VERSION_V1.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext request) {
    String safeCommit =
        (commit != null && commit.length() >= 8) ? commit.substring(0, 8) : "unknown";
    List<EngineGetClientVersionResultV1> versions =
        Collections.singletonList(
            new EngineGetClientVersionResultV1(
                ENGINE_CLIENT_CODE, ENGINE_CLIENT_NAME, clientVersion, safeCommit));
    return new JsonRpcSuccessResponse(request.getRequest().getId(), versions);
  }
}
