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

import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
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

  // Bounds on caller-supplied blocks to prevent unauthenticated DoS via debug_traceBlock.
  // A caller controls every field of the replayed block; without these guards a 562-byte
  // request can buy 40+ seconds of EVM execution or gigabytes of streaming output.
  static final int MAX_TRACE_BLOCK_TX_COUNT = 300;
  static final long MAX_TRACE_BLOCK_GAS_LIMIT = 50_000_000L;

  private final BlockHeaderFunctions blockHeaderFunctions;

  public DebugTraceBlock(
      final ProtocolSchedule protocolSchedule, final BlockchainQueries blockchainQueries) {
    this(protocolSchedule, blockchainQueries, null);
  }

  public DebugTraceBlock(
      final ProtocolSchedule protocolSchedule,
      final BlockchainQueries blockchainQueries,
      final ApiConfiguration apiConfiguration) {
    super(protocolSchedule, blockchainQueries, apiConfiguration);
    this.blockHeaderFunctions = ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_TRACE_BLOCK.getMethodName();
  }

  /**
   * The outcome of resolving the caller-supplied block parameter: either an accepted block or the
   * error type the caller must be told about. Both the streaming and the batch path go through
   * this, so a request rejected by one is rejected identically by the other — returning {@code
   * result: null} for an oversized block would hide the reason the trace was refused.
   */
  private record ResolvedBlock(Optional<Block> block, Optional<RpcErrorType> error) {
    static ResolvedBlock accepted(final Block block) {
      return new ResolvedBlock(Optional.of(block), Optional.empty());
    }

    static ResolvedBlock rejected(final RpcErrorType error) {
      return new ResolvedBlock(Optional.empty(), Optional.of(error));
    }
  }

  private ResolvedBlock resolveBlock(final JsonRpcRequestContext request) {
    final Block block;
    try {
      final String input = request.getRequiredParameter(0, String.class);
      block = Block.readFrom(RLP.input(Bytes.fromHexString(input)), this.blockHeaderFunctions);
    } catch (final RLPException | IllegalArgumentException e) {
      LOG.debug("Failed to parse block RLP (index 0)", e);
      return ResolvedBlock.rejected(RpcErrorType.INVALID_BLOCK_PARAMS);
    } catch (final JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block params (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }

    if (block.getBody().getTransactions().size() > MAX_TRACE_BLOCK_TX_COUNT) {
      LOG.warn(
          "debug_traceBlock rejected: tx count {} exceeds limit {}",
          block.getBody().getTransactions().size(),
          MAX_TRACE_BLOCK_TX_COUNT);
      return ResolvedBlock.rejected(RpcErrorType.EXCEEDS_RPC_TRACE_BLOCK_TX_COUNT);
    }

    if (block.getHeader().getGasLimit() > MAX_TRACE_BLOCK_GAS_LIMIT) {
      LOG.warn(
          "debug_traceBlock rejected: gasLimit {} exceeds limit {}",
          block.getHeader().getGasLimit(),
          MAX_TRACE_BLOCK_GAS_LIMIT);
      return ResolvedBlock.rejected(RpcErrorType.EXCEEDS_RPC_TRACE_BLOCK_GAS_LIMIT);
    }

    if (getBlockchainQueries()
        .getBlockchain()
        .getBlockByHash(block.getHeader().getParentHash())
        .isEmpty()) {
      return ResolvedBlock.rejected(RpcErrorType.PARENT_BLOCK_NOT_FOUND);
    }

    return ResolvedBlock.accepted(block);
  }

  @Override
  protected Optional<Block> findBlock(final JsonRpcRequestContext request) {
    return resolveBlock(request).block();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext request) {
    final ResolvedBlock resolved = resolveBlock(request);
    if (resolved.error().isPresent()) {
      return new JsonRpcErrorResponse(request.getRequest().getId(), resolved.error().get());
    }
    final TraceOptions traceOptions = getTraceOptions(request);
    final DebugTraceBlockStreamer streamer = createStreamer(traceOptions, resolved.block());
    return new JsonRpcSuccessResponse(
        request.getRequest().getId(), streamer.accumulateAll(request::isAlive));
  }

  @Override
  public void streamResponse(
      final JsonRpcRequestContext requestContext, final OutputStream out, final ObjectMapper mapper)
      throws IOException {
    final ResolvedBlock resolved = resolveBlock(requestContext);
    if (resolved.error().isPresent()) {
      mapper.writeValue(
          out,
          new JsonRpcErrorResponse(requestContext.getRequest().getId(), resolved.error().get()));
      return;
    }

    final TraceOptions traceOptions = getTraceOptions(requestContext);
    final DebugTraceBlockStreamer streamer = createStreamer(traceOptions, resolved.block());
    writeStreamingResponse(
        requestContext.getRequest().getId(), streamer, out, mapper, requestContext::isAlive);
  }
}
