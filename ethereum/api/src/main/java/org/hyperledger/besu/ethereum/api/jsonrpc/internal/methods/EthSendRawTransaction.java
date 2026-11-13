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
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcErrorConverter;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.util.DomainObjectDecodeUtils;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EthSendRawTransaction implements JsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(EthSendRawTransaction.class);

  private final boolean sendEmptyHashOnInvalidBlock;

  private final Supplier<TransactionPool> transactionPool;

  public EthSendRawTransaction(final TransactionPool transactionPool) {
    this(Suppliers.ofInstance(transactionPool), false);
  }

  public EthSendRawTransaction(
      final Supplier<TransactionPool> transactionPool, final boolean sendEmptyHashOnInvalidBlock) {
    this.transactionPool = transactionPool;
    this.sendEmptyHashOnInvalidBlock = sendEmptyHashOnInvalidBlock;
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_SEND_RAW_TRANSACTION.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    if (requestContext.getRequest().getParamLength() != 1) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_PARAM_COUNT);
    }
    final String rawTransaction;
    try {
      rawTransaction = requestContext.getRequiredParameter(0, String.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction parameters (index 0)", RpcErrorType.INVALID_TRANSACTION_PARAMS, e);
    }

    final Transaction transaction;
    try {
      transaction = DomainObjectDecodeUtils.decodeRawTransaction(rawTransaction);
      CompletableFuture.runAsync(
          () -> {
            Address sender = transaction.getSender();
            LOG.atTrace()
                .setMessage("The sender for transaction {} is calculated : {}")
                .addArgument(transaction::getHash)
                .addArgument(sender)
                .log();
          });
      LOG.trace("Received local transaction {}", transaction);
    } catch (final RLPException e) {
      LOG.debug("RLPException: {} caused by {}", e.getMessage(), e.getCause());
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_BLOCK_PARAMS);
    } catch (final InvalidJsonRpcRequestException i) {
      LOG.debug("InvalidJsonRpcRequestException: {} caused by {}", i.getMessage(), i.getCause());
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_PARAMS);
    } catch (final IllegalArgumentException ill) {
      LOG.debug("IllegalArgumentException: {} caused by {}", ill.getMessage(), ill.getCause());
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_PARAMS);
    }

    final ValidationResult<TransactionInvalidReason> validationResult =
        transactionPool.get().addTransactionViaApi(transaction);
    return validationResult.either(
        () ->
            new JsonRpcSuccessResponse(
                requestContext.getRequest().getId(), transaction.getHash().toString()),
        errorReason -> getJsonRpcResponse(requestContext, errorReason, validationResult));
  }

  @NotNull
  private JsonRpcResponse getJsonRpcResponse(
      final JsonRpcRequestContext requestContext,
      final TransactionInvalidReason errorReason,
      final ValidationResult<TransactionInvalidReason> validationResult) {
    if (sendEmptyHashOnInvalidBlock) {
      return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), Hash.EMPTY.toString());
    } else {
      if (errorReason == TransactionInvalidReason.PLUGIN_TX_POOL_VALIDATOR) {
        final RpcErrorType rpcErrorType =
            JsonRpcErrorConverter.convertTransactionInvalidReason(
                validationResult.getInvalidReason());
        return new JsonRpcErrorResponse(
            requestContext.getRequest().getId(),
            new JsonRpcError(rpcErrorType.getCode(), validationResult.getErrorMessage(), null));
      } else if (errorReason == TransactionInvalidReason.NONCE_TOO_LOW) {
        final RpcErrorType rpcErrorType =
            JsonRpcErrorConverter.convertTransactionInvalidReason(errorReason);
        // getErrorMessage() falls back to the enum name when no detail message was supplied
        final String details = validationResult.getErrorMessage();
        final String message =
            details.equals(errorReason.toString())
                ? "nonce too low"
                : "nonce too low (%s)".formatted(details);
        return new JsonRpcErrorResponse(
            requestContext.getRequest().getId(),
            new JsonRpcError(rpcErrorType.getCode(), message, null));
      } else {
        return new JsonRpcErrorResponse(
            requestContext.getRequest().getId(),
            JsonRpcErrorConverter.convertTransactionInvalidReason(errorReason));
      }
    }
  }
}
