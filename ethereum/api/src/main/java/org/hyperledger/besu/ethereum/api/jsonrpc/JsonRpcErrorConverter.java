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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;

public class JsonRpcErrorConverter {

  public static RpcErrorType convertTransactionInvalidReason(
      final TransactionInvalidReason reason) {
    return switch (reason) {
      case NONCE_TOO_LOW -> RpcErrorType.NONCE_TOO_LOW;
      case NONCE_TOO_HIGH -> RpcErrorType.NONCE_TOO_HIGH;
      case INVALID_SIGNATURE -> RpcErrorType.INVALID_TRANSACTION_SIGNATURE;
      case INTRINSIC_GAS_EXCEEDS_GAS_LIMIT -> RpcErrorType.INTRINSIC_GAS_EXCEEDS_LIMIT;
      case UPFRONT_COST_EXCEEDS_BALANCE -> RpcErrorType.TRANSACTION_UPFRONT_COST_EXCEEDS_BALANCE;
      case EXCEEDS_BLOCK_GAS_LIMIT -> RpcErrorType.EXCEEDS_BLOCK_GAS_LIMIT;
      case EXCEEDS_TRANSACTION_GAS_LIMIT -> RpcErrorType.EXCEEDS_TRANSACTION_GAS_LIMIT;
      case WRONG_CHAIN_ID -> RpcErrorType.WRONG_CHAIN_ID;
      case REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED ->
          RpcErrorType.REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED;
      case REPLAY_PROTECTED_SIGNATURE_REQUIRED -> RpcErrorType.REPLAY_PROTECTED_SIGNATURE_REQUIRED;
      case TX_SENDER_NOT_AUTHORIZED -> RpcErrorType.TX_SENDER_NOT_AUTHORIZED;
      case CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE ->
          RpcErrorType.CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE;
      case GAS_PRICE_TOO_LOW -> RpcErrorType.GAS_PRICE_TOO_LOW;
      case GAS_PRICE_BELOW_CURRENT_BASE_FEE -> RpcErrorType.GAS_PRICE_BELOW_CURRENT_BASE_FEE;
      case TX_FEECAP_EXCEEDED -> RpcErrorType.TX_FEECAP_EXCEEDED;
      case MAX_PRIORITY_FEE_PER_GAS_EXCEEDS_MAX_FEE_PER_GAS ->
          RpcErrorType.MAX_PRIORITY_FEE_PER_GAS_EXCEEDS_MAX_FEE_PER_GAS;
      case INVALID_TRANSACTION_FORMAT -> RpcErrorType.INVALID_TRANSACTION_TYPE;
      case TRANSACTION_ALREADY_KNOWN -> RpcErrorType.ETH_SEND_TX_ALREADY_KNOWN;
      case TRANSACTION_REPLACEMENT_UNDERPRICED -> RpcErrorType.ETH_SEND_TX_REPLACEMENT_UNDERPRICED;
      case NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER -> RpcErrorType.NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER;
      case TOTAL_BLOB_GAS_TOO_HIGH -> RpcErrorType.TOTAL_BLOB_GAS_TOO_HIGH;
      case TX_POOL_DISABLED -> RpcErrorType.TX_POOL_DISABLED;
      case PLUGIN_TX_VALIDATOR -> RpcErrorType.PLUGIN_TX_VALIDATOR;
      case PLUGIN_TX_POOL_VALIDATOR -> RpcErrorType.PLUGIN_TX_POOL_VALIDATOR;
      case INVALID_BLOBS -> RpcErrorType.INVALID_BLOBS;
      case BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE ->
          RpcErrorType.BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE;
      case EXECUTION_HALTED -> RpcErrorType.EXECUTION_HALTED;
      case BLOCK_NOT_FOUND -> RpcErrorType.BLOCK_NOT_FOUND;
      default -> RpcErrorType.INTERNAL_ERROR;
    };
  }
}
