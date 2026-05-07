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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;

import java.util.Optional;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Suppliers;

/** Returns effective routing capabilities for the current node. */
public class EthCapabilities implements JsonRpcMethod {

  private static final Supplier<ObjectMapper> mapperSupplier = Suppliers.memoize(ObjectMapper::new);

  private final BlockchainQueries blockchainQueries;

  public EthCapabilities(final BlockchainQueries blockchainQueries) {
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_CAPABILITIES.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final ObjectNode result = mapperSupplier.get().createObjectNode();

    final var blockchain = blockchainQueries.getBlockchain();
    final var chainHeadHeader = blockchain.getChainHeadHeader();

    final ObjectNode head = result.putObject("head");
    head.put("number", Quantity.create(chainHeadHeader.getNumber()));
    head.put("hash", chainHeadHeader.getHash().toHexString());

    final Optional<Long> maybeOldestBlock = blockchain.getEarliestBlockNumber();

    // TODO: state should report disabled=true when the node is running with --sync-mode=SNAP
    // (the default) or does not have a full state DB. See
    // https://github.com/besu-eth/besu/issues/10371
    addResource(result, "state", false, Optional.empty());
    addResource(result, "tx", false, maybeOldestBlock);
    addResource(result, "logs", false, maybeOldestBlock);
    addResource(result, "receipts", false, maybeOldestBlock);
    addResource(result, "blocks", false, maybeOldestBlock);
    // TODO: same as state — disabled detection needed for stateproofs. See
    // https://github.com/besu-eth/besu/issues/10371
    addResource(result, "stateproofs", false, Optional.empty());

    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), result);
  }

  private static void addResource(
      final ObjectNode parent,
      final String resource,
      final boolean disabled,
      final Optional<Long> maybeOldestBlock) {
    final ObjectNode node = parent.putObject(resource);
    node.put("disabled", disabled);
    maybeOldestBlock.ifPresent(oldest -> node.put("oldestBlock", Quantity.create(oldest)));
  }
}
