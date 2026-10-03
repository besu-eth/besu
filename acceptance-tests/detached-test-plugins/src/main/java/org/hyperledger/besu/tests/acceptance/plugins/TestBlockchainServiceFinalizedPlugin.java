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
package org.hyperledger.besu.tests.acceptance.plugins;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.data.BlockContext;
import org.hyperledger.besu.plugin.services.BlockchainService;
import org.hyperledger.besu.plugin.services.RpcEndpointRegistry;
import org.hyperledger.besu.plugin.services.exception.PluginRpcEndpointException;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcRequest;

import java.util.Optional;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(BesuPlugin.class)
public class TestBlockchainServiceFinalizedPlugin implements BesuPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestBlockchainServiceFinalizedPlugin.class);
  private static final String RPC_NAMESPACE = "updater";
  private static final String RPC_METHOD_FINALIZED_BLOCK = "updateFinalizedBlockV1";
    private static final String RPC_METHOD_SAFE_BLOCK = "updateSafeBlockV1";
  private final FinalizationUpdaterRpcMethod rpcMethod = new FinalizationUpdaterRpcMethod();

  @Override
  public void register(final RegistrationContext context) {
    LOG.trace("Registering plugin ...");
    // the endpoints are registered here; the BlockchainService they use is a start-phase service,
    // so the handler receives it in start(), before any request can reach it
    final RpcEndpointRegistry rpcEndpointRegistry =
        context.getBesuService(RpcEndpointRegistry.class);
    rpcEndpointRegistry.registerRPCEndpoint(
        RPC_NAMESPACE, RPC_METHOD_FINALIZED_BLOCK, rpcMethod::setFinalizedBlock);
    rpcEndpointRegistry.registerRPCEndpoint(
        RPC_NAMESPACE, RPC_METHOD_SAFE_BLOCK, rpcMethod::setSafeBlock);
  }

  @Override
  public void start(final StartContext context) {
    LOG.trace("Starting plugin ...");
    rpcMethod.setBlockchainService(context.getBesuService(BlockchainService.class));
  }

  @Override
  public void stop() {
    LOG.trace("Stopping plugin ...");
  }

  static class FinalizationUpdaterRpcMethod {
    private volatile BlockchainService blockchainService;
    private final JsonRpcParameter parameterParser = new JsonRpcParameter();

    void setBlockchainService(final BlockchainService blockchainService) {
      this.blockchainService = blockchainService;
    }

    Boolean setFinalizedBlock(final PluginRpcRequest request) {
      return setFinalizedOrSafeBlock(request, true);
    }

    Boolean setSafeBlock(final PluginRpcRequest request) {
      return setFinalizedOrSafeBlock(request, false);
    }

    private Boolean setFinalizedOrSafeBlock(
        final PluginRpcRequest request, final boolean isFinalized) {
            final Long blockNumberToSet = parseResult(request);
      if (blockchainService == null) {
        throw new PluginRpcEndpointException(
            RpcErrorType.INTERNAL_ERROR, "Plugin has not started yet");
      }
      // lookup finalized block by number in local chain
      final Optional<BlockContext> finalizedBlock =
          blockchainService.getBlockByNumber(blockNumberToSet);
      if (finalizedBlock.isEmpty()) {
        throw new PluginRpcEndpointException(
            RpcErrorType.BLOCK_NOT_FOUND,
            "Block not found in the local chain: " + blockNumberToSet);
      }

      try {
        final Hash blockHash = finalizedBlock.get().getBlockHeader().getBlockHash();
        if (isFinalized) {
          blockchainService.setFinalizedBlock(blockHash);
        } else {
          blockchainService.setSafeBlock(blockHash);
        }
      } catch (final IllegalArgumentException e) {
        throw new PluginRpcEndpointException(
            RpcErrorType.BLOCK_NOT_FOUND,
            "Block not found in the local chain: " + blockNumberToSet);
      } catch (final UnsupportedOperationException e) {
        throw new PluginRpcEndpointException(
            RpcErrorType.METHOD_NOT_ENABLED,
            "Method not enabled for PoS network: setFinalizedBlock");
      } catch (final Exception e) {
        throw new PluginRpcEndpointException(
            RpcErrorType.INTERNAL_ERROR, "Error setting finalized block: " + blockNumberToSet);
      }

      return Boolean.TRUE;
    }

    private Long parseResult(final PluginRpcRequest request) {
      Long blockNumber;
      try {
        final Object[] params = request.getParams();
        blockNumber = parameterParser.required(params, 0, Long.class);
      } catch (final Exception e) {
        throw new PluginRpcEndpointException(RpcErrorType.INVALID_PARAMS, e.getMessage());
      }

      if (blockNumber <= 0) {
        throw new PluginRpcEndpointException(
            RpcErrorType.INVALID_PARAMS, "Block number must be greater than 0");
      }

      return blockNumber;
    }
  }
}
