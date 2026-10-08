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
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.transaction.CallParameter;

public class CallParameterUtil {
  private CallParameterUtil() {}

  public static CallParameter validateAndGetCallParams(final JsonRpcRequestContext request) {
    final CallParameter callParams;
    try {
      callParams = request.getRequiredParameter(0, CallParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid call parameters (index 0)", RpcErrorType.INVALID_CALL_PARAMS);
    }

    rejectMixedFeeFields(callParams);
    return callParams;
  }

  /**
   * Rejects a call that sets gasPrice together with maxFeePerGas or maxPriorityFeePerGas. No
   * transaction carries both, so neither can be chosen over the other without rewriting the call.
   *
   * @param callParams the call parameters
   */
  public static void rejectMixedFeeFields(final CallParameter callParams) {
    if (callParams.getGasPrice().isPresent()
        && (callParams.getMaxFeePerGas().isPresent()
            || callParams.getMaxPriorityFeePerGas().isPresent())) {
      throw new InvalidJsonRpcParameters(
          "both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified",
          RpcErrorType.INVALID_PARAMS);
    }
  }

  public static boolean isAllowExceedingBalance(
      final BlockHeader header, final CallParameter callParams) {
    if (callParams.getStrict().isPresent()) {
      return !callParams.getStrict().get();
    }

    final boolean isZeroGasPrice = callParams.getGasPrice().map(Wei.ZERO::equals).orElse(true);

    // the blob fee is priced independently, by the simulator
    if (header.getBaseFee().isPresent()) {
      final boolean isZeroMaxFeePerGas =
          callParams.getMaxFeePerGas().orElse(Wei.ZERO).equals(Wei.ZERO);
      final boolean isZeroMaxPriorityFeePerGas =
          callParams.getMaxPriorityFeePerGas().orElse(Wei.ZERO).equals(Wei.ZERO);
      return isZeroGasPrice && isZeroMaxFeePerGas && isZeroMaxPriorityFeePerGas;
    }

    return isZeroGasPrice;
  }

  /**
   * Returns the validation parameters eth_call uses for a call. When {@link
   * #isAllowExceedingBalance} holds, the call runs with a zero gas price and base fee and without
   * execution gas fees; otherwise it is validated against the block's base fee and the sender's
   * balance and pays for its gas.
   *
   * @param header the header of the block the call runs on
   * @param callParams the call parameters
   * @return the transaction validation parameters for the call
   */
  public static TransactionValidationParams getTransactionValidationParams(
      final BlockHeader header, final CallParameter callParams) {
    return isAllowExceedingBalance(header, callParams)
        ? TransactionValidationParams.transactionSimulatorAllowExceedingBalanceAndFutureNonce()
        : TransactionValidationParams.transactionSimulatorAllowFutureNonce();
  }
}
