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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DebugTraceBlockByHash extends AbstractDebugTraceBlock {

  public DebugTraceBlockByHash(
      final ProtocolSchedule protocolSchedule, final BlockchainQueries blockchainQueries) {
    super(protocolSchedule, blockchainQueries);
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_TRACE_BLOCK_BY_HASH.getMethodName();
  }

  @Override
  protected Optional<Block> findBlock(final JsonRpcRequestContext request) {
    try {
      final Hash blockHash = request.getRequiredParameter(0, Hash.class);
      return getBlockchainQueries().getBlockchain().getBlockByHash(blockHash);
    } catch (JsonRpcParameterException e) {
      return Optional.empty();
    }
  }

  @Override
  public void streamResponse(
      final JsonRpcRequestContext requestContext, final OutputStream out, final ObjectMapper mapper)
      throws IOException {
    final Hash blockHash;
    try {
      blockHash = requestContext.getRequiredParameter(0, Hash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block hash parameter (index 0)", RpcErrorType.INVALID_BLOCK_HASH_PARAMS, e);
    }

    final TraceOptions traceOptions = getTraceOptions(requestContext);
    final Optional<Block> maybeBlock =
        getBlockchainQueries().getBlockchain().getBlockByHash(blockHash);

    final DebugTraceBlockStreamer streamer = createStreamer(traceOptions, maybeBlock);
    writeStreamingResponse(requestContext.getRequest().getId(), streamer, out, mapper);
  }
}
