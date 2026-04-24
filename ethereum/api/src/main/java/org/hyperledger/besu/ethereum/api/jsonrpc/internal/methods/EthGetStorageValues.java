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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.StorageSlotsRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.units.bigints.UInt256;

public class EthGetStorageValues extends AbstractBlockParameterOrBlockHashMethod {

  static final int MAX_STORAGE_SLOTS = 1024;

  public EthGetStorageValues(final BlockchainQueries blockchainQueries) {
    super(blockchainQueries);
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_STORAGE_VALUES.getMethodName();
  }

  @Override
  protected BlockParameterOrBlockHash blockParameterOrBlockHash(
      final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(1, BlockParameterOrBlockHash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block or block hash parameter (index 1)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  @Override
  protected Object resultByBlockHash(final JsonRpcRequestContext request, final Hash blockHash) {
    final StorageSlotsRequest storageSlotsRequest;
    try {
      storageSlotsRequest = request.getRequiredParameter(0, StorageSlotsRequest.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid storage slots request parameter (index 0)", RpcErrorType.INVALID_PARAMS, e);
    }

    int totalSlots = 0;
    for (final List<String> keys : storageSlotsRequest.values()) {
      if (keys == null) {
        return new JsonRpcErrorResponse(
            request.getRequest().getId(),
            new JsonRpcError(RpcErrorType.INVALID_PARAMS.getCode(), "null slot list", null));
      }
      totalSlots += keys.size();
      if (totalSlots > MAX_STORAGE_SLOTS) {
        return new JsonRpcErrorResponse(
            request.getRequest().getId(),
            new JsonRpcError(
                RpcErrorType.INVALID_PARAMS.getCode(),
                "too many slots (max " + MAX_STORAGE_SLOTS + ")",
                null));
      }
    }

    if (totalSlots == 0) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(),
          new JsonRpcError(RpcErrorType.INVALID_PARAMS.getCode(), "empty request", null));
    }

    final Map<String, List<String>> result = new HashMap<>();
    for (final Map.Entry<Address, List<String>> entry : storageSlotsRequest.entrySet()) {
      final Address address = entry.getKey();
      final List<String> keys = entry.getValue();
      final List<String> values = new ArrayList<>(keys.size());
      for (final String keyHex : keys) {
        if (keyHex == null) {
          throw new InvalidJsonRpcParameters(
              "Invalid storage key parameter", RpcErrorType.INVALID_PARAMS);
        }
        final UInt256 key;
        try {
          key = UInt256.fromHexString(keyHex);
        } catch (IllegalArgumentException e) {
          throw new InvalidJsonRpcParameters(
              "Invalid storage key parameter", RpcErrorType.INVALID_PARAMS, e);
        }
        final UInt256 value =
            blockchainQueries.get().storageAt(address, key, blockHash).orElse(UInt256.ZERO);
        values.add(value.toHexString());
      }
      result.put(address.toHexString(), values);
    }
    return result;
  }
}
