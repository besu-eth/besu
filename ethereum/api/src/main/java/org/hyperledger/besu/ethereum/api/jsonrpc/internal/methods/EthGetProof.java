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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.proof.GetProofResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.units.bigints.UInt256;

public class EthGetProof extends AbstractBlockParameterOrBlockHashMethod {
  private static final Pattern HEX_DIGITS = Pattern.compile("[0-9a-fA-F]*");

  // Maximum number of storage keys in one request, the same limit as geth, Nethermind and Erigon.
  @VisibleForTesting static final int MAX_STORAGE_KEYS = 1024;

  public EthGetProof(final BlockchainQueries blockchain) {
    super(blockchain);
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_PROOF.getMethodName();
  }

  @Override
  protected BlockParameterOrBlockHash blockParameterOrBlockHash(
      final JsonRpcRequestContext request) {
    return blockParameterOrBlockHashWithLatestDefault(request, 2);
  }

  @Override
  protected Object resultByBlockHash(
      final JsonRpcRequestContext requestContext, final Hash blockHash) {

    final Address address;
    try {
      address = requestContext.getRequiredParameter(0, Address.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid address parameter (index 0)", RpcErrorType.INVALID_ADDRESS_PARAMS, e);
    }
    final List<String> storageKeys = getStorageKeys(requestContext);

    final Blockchain blockchain = getBlockchainQueries().getBlockchain();
    final Optional<BlockHeader> maybeBlockHeader = blockchain.getBlockHeader(blockHash);
    if (maybeBlockHeader.isEmpty()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.WORLD_STATE_UNAVAILABLE);
    }
    final WorldStateArchive worldStateArchive = getBlockchainQueries().getWorldStateArchive();
    return worldStateArchive
        .getAccountProof(
            maybeBlockHeader.get(),
            address,
            storageKeys.stream().map(UInt256::fromHexString).distinct().toList(),
            maybeWorldStateProof ->
                maybeWorldStateProof.map(
                    proof -> GetProofResult.build(address, proof, storageKeys)))
        .<JsonRpcResponse>map(
            result -> new JsonRpcSuccessResponse(requestContext.getRequest().getId(), result))
        .orElseGet(
            () ->
                new JsonRpcErrorResponse(
                    requestContext.getRequest().getId(),
                    isHeadWorldStateAvailable(blockchain, worldStateArchive)
                        ? RpcErrorType.PRUNED_HISTORY_UNAVAILABLE
                        : RpcErrorType.WORLD_STATE_UNAVAILABLE));
  }

  // Without the head state the node is still syncing, so the gap is temporary, not pruned.
  private static boolean isHeadWorldStateAvailable(
      final Blockchain blockchain, final WorldStateArchive worldStateArchive) {
    final BlockHeader head = blockchain.getChainHeadHeader();
    return worldStateArchive.isWorldStateAvailable(head.getStateRoot(), head.getHash());
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    return (JsonRpcResponse) handleParamTypes(requestContext);
  }

  private List<String> getStorageKeys(final JsonRpcRequestContext request) {
    final String[] keys;
    try {
      keys = request.getRequiredParameter(1, String[].class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid storage keys parameters (index 1)", RpcErrorType.INVALID_STORAGE_KEYS_PARAMS, e);
    }
    // checked before any key is parsed or any state is read
    if (keys.length > MAX_STORAGE_KEYS) {
      throw new InvalidJsonRpcParameters(
          String.format(
              "Too many storage keys requested (max %d, got %d)", MAX_STORAGE_KEYS, keys.length),
          RpcErrorType.INVALID_STORAGE_KEYS_PARAMS);
    }
    return Arrays.stream(keys).map(EthGetProof::normalizeStorageKey).toList();
  }

  // A key with 63 or 64 hex digits is echoed as a 32-byte value; any other key as a minimal
  // quantity, per execution-apis eth_getProof.
  private static String normalizeStorageKey(final String key) {
    if (key == null) {
      throw invalidStorageKey(key);
    }
    final String digits = key.startsWith("0x") ? key.substring(2) : key;
    if (digits.length() > 64 || !HEX_DIGITS.matcher(digits).matches()) {
      throw invalidStorageKey(key);
    }
    final UInt256 value = digits.isEmpty() ? UInt256.ZERO : UInt256.fromHexString(digits);
    return digits.length() >= 63 ? value.toHexString() : Quantity.create(value);
  }

  private static InvalidJsonRpcParameters invalidStorageKey(final String key) {
    return new InvalidJsonRpcParameters(
        "Invalid storage key: " + key, RpcErrorType.INVALID_STORAGE_KEYS_PARAMS);
  }
}
