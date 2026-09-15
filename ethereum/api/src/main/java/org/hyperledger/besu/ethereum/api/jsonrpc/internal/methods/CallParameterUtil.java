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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.transaction.CallParameter;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CallParameterUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CallParameterUtil.class);

  private CallParameterUtil() {}

  public static CallParameter validateAndGetCallParams(final JsonRpcRequestContext request) {
    final CallParameter callParams;
    try {
      callParams = request.getRequiredParameter(0, CallParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid call parameters (index 0)", RpcErrorType.INVALID_CALL_PARAMS);
    }

    if (callParams.getGasPrice().isPresent()
        && (callParams.getMaxFeePerGas().isPresent()
            || callParams.getMaxPriorityFeePerGas().isPresent())) {
      try {
        LOG.debug(
            "gasPrice will be ignored since 1559 values are defined (maxFeePerGas or maxPriorityFeePerGas). {}",
            Arrays.toString(request.getRequest().getParams()));
      } catch (Exception e) {
        LOG.debug(
            "gasPrice will be ignored since 1559 values are defined (maxFeePerGas or maxPriorityFeePerGas)");
      }
    }
    return callParams;
  }

  public static boolean isAllowExceedingBalance(
      final BlockHeader header, final CallParameter callParams) {
    if (callParams.getStrict().isPresent()) {
      return !callParams.getStrict().get();
    }

    final boolean isZeroGasPrice = callParams.getGasPrice().map(Wei.ZERO::equals).orElse(true);

    if (header.getBaseFee().isPresent()) {
      if (callParams.getBlobVersionedHashes().isPresent()
          && (callParams.getMaxFeePerBlobGas().isEmpty()
              || callParams.getMaxFeePerBlobGas().get().equals(Wei.ZERO))) {
        return true;
      }
      final boolean isZeroMaxFeePerGas =
          callParams.getMaxFeePerGas().orElse(Wei.ZERO).equals(Wei.ZERO);
      final boolean isZeroMaxPriorityFeePerGas =
          callParams.getMaxPriorityFeePerGas().orElse(Wei.ZERO).equals(Wei.ZERO);
      return isZeroGasPrice && isZeroMaxFeePerGas && isZeroMaxPriorityFeePerGas;
    }

    return isZeroGasPrice;
  }
}
