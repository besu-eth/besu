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

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugTraceBlock extends AbstractDebugTraceBlock {

  private static final Logger LOG = LoggerFactory.getLogger(DebugTraceBlock.class);
  private final BlockHeaderFunctions blockHeaderFunctions;

  public DebugTraceBlock(
      final ProtocolSchedule protocolSchedule, final BlockchainQueries blockchainQueries) {
    super(protocolSchedule, blockchainQueries);
    this.blockHeaderFunctions = ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_TRACE_BLOCK.getMethodName();
  }

  @Override
  protected Optional<Block> findBlock(final JsonRpcRequestContext request) {
    try {
      final String input = request.getRequiredParameter(0, String.class);
      final Block block =
          Block.readFrom(RLP.input(Bytes.fromHexString(input)), this.blockHeaderFunctions);
      if (getBlockchainQueries()
          .getBlockchain()
          .getBlockByHash(block.getHeader().getParentHash())
          .isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(block);
    } catch (final RLPException | IllegalArgumentException | JsonRpcParameterException e) {
      LOG.debug("Failed to resolve block for batch trace request", e);
      return Optional.empty();
    }
  }

  @Override
  public void streamResponse(
      final JsonRpcRequestContext requestContext, final OutputStream out, final ObjectMapper mapper)
      throws IOException {
    final Block block;
    try {
      final String input = requestContext.getRequiredParameter(0, String.class);
      block = Block.readFrom(RLP.input(Bytes.fromHexString(input)), this.blockHeaderFunctions);
    } catch (final RLPException | IllegalArgumentException e) {
      LOG.debug("Failed to parse block RLP (index 0)", e);
      mapper.writeValue(
          out,
          new JsonRpcErrorResponse(
              requestContext.getRequest().getId(), RpcErrorType.INVALID_BLOCK_PARAMS));
      return;
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block params (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
    final TraceOptions traceOptions = getTraceOptions(requestContext);

    if (getBlockchainQueries()
        .getBlockchain()
        .getBlockByHash(block.getHeader().getParentHash())
        .isPresent()) {
      final DebugTraceBlockStreamer streamer =
          createStreamer(traceOptions, Optional.ofNullable(block));
      writeStreamingResponse(requestContext.getRequest().getId(), streamer, out, mapper);
    } else {
      mapper.writeValue(
          out,
          new JsonRpcErrorResponse(
              requestContext.getRequest().getId(), RpcErrorType.PARENT_BLOCK_NOT_FOUND));
    }
  }
}
